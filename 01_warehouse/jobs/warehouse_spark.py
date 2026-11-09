"""Transformacoes Spark reais do job; testaveis sem AWS."""
import json
from pyspark.sql import Window, functions as F, types as T
from warehouse_core import clean_order, clean_payment, QualityViolation

class QualityError(ValueError):
    def __init__(self, message, counts):
        super().__init__(message)
        self.counts = counts

def checked(row, kind):
    """Executada nos workers Spark; inclui motivo de rejeicao sem publicar dados invalidos."""
    raw = row.asDict()
    try:
        val = clean_order(raw) if kind == 'pedidos' else clean_payment(raw)
        val['_source_key'] = raw.get('_source_key')
        val['_source_version'] = raw.get('_source_version')
        return json.dumps({'ok': True, 'data': val}, default=str)
    except (ValueError, TypeError) as exc:
        # Nao persiste valores de origem nem texto arbitrario da excecao.
        return json.dumps({'ok': False, 'reason': str(exc) if isinstance(exc, QualityViolation) else 'CAMPO_OU_FORMATO_INVALIDO',
                           'source_key': raw.get('_source_key'),
                           'source_version': raw.get('_source_version')})


def prepare(spark, paths, kind, attempt, rejected_root, *, source_metadata=None,
            lote=None, allow_partial=False, with_details=False):
    columns = 'id string,dia string,valor string,status string,canal string' if kind == 'pedidos' else 'pedido_id string,resultado string,atualizado_em string'
    reader = spark.read.schema(columns).option('mode', 'FAILFAST')
    raw = reader.option('header', 'true').option('sep', ';').option('enforceSchema', 'false').csv(paths) if kind == 'pedidos' else reader.json(paths)
    raw = raw.withColumn('_source_path', F.regexp_replace(F.input_file_name(), r'^s3a://', 's3://'))
    if source_metadata:
        items = []
        for path, info in source_metadata.items():
            items.extend([F.lit(path), F.struct(F.lit(info['key']).alias('key'), F.lit(info['version_id']).alias('version'))])
        raw = raw.withColumn('_source', F.create_map(*items)[F.col('_source_path')])
        if raw.filter(F.col('_source').isNull()).limit(1).count():
            raise ValueError('Origem temporaria sem correspondencia no manifesto')
        raw = raw.withColumn('_source_key', F.col('_source.key')).withColumn('_source_version', F.col('_source.version')).drop('_source')
    else:
        raw = raw.withColumn('_source_key', F.col('_source_path')).withColumn('_source_version', F.lit(None).cast('string'))
    validation_schema = T.StructType([
        T.StructField('ok', T.BooleanType()), T.StructField('reason', T.StringType()),
        T.StructField('source_key', T.StringType()), T.StructField('source_version', T.StringType()),
        T.StructField('data', T.MapType(T.StringType(), T.StringType())),
    ])
    lines = raw.rdd.map(lambda row: checked(row, kind))
    validated = spark.read.schema(validation_schema).json(lines).cache()
    errors = validated.filter('NOT ok')
    received = validated.count()
    rejected = errors.count()
    stats = {'recebidos': received, 'aceitos': received - rejected, 'rejeitados': rejected}
    if rejected:
        (errors.select(F.lit(lote).alias('lote_id'), F.lit(attempt).alias('attempt_id'),
                       F.lit(kind).alias('origem'), F.col('source_key').alias('arquivo'),
                       F.col('source_version').alias('versao_s3'), F.col('reason').alias('motivo'))
         .write.mode('overwrite').json(f'{rejected_root}/{kind}/'))
        if not allow_partial:
            raise QualityError('Registros invalidos; consulte rejeitados/' + attempt, stats)
    clean = validated.filter('ok').select('data')
    provenance = clean.select(F.col('data.pedido_id').alias('pedido_id'), F.lit(kind).alias('tipo'),
                              F.col('data._source_key').alias('arquivo'),
                              F.col('data._source_version').alias('versao_s3'),
                              F.lit(attempt).alias('attempt_id')).distinct()
    def reject_conflicts(conflicts, reason, message):
        keys = conflicts.select('pedido_id').distinct()
        rejected_count = clean.join(keys, clean['data.pedido_id'] == keys.pedido_id, 'inner').count()
        (provenance.join(keys, 'pedido_id').select(
            F.lit(lote).alias('lote_id'), 'attempt_id', F.col('tipo').alias('origem'),
            'arquivo', 'versao_s3', F.lit(reason).alias('motivo'))
         .write.mode('overwrite').json(f'{rejected_root}/{kind}_conflitos/'))
        stats['rejeitados'] += rejected_count
        stats['aceitos'] -= rejected_count
        raise QualityError(message, stats)
    if kind == 'pedidos':
        df = clean.select(
            F.col('data.pedido_id').alias('pedido_id'), F.to_date('data.data').alias('data'),
            F.col('data.valor').cast('decimal(12,2)').alias('valor'), F.col('data.status').alias('status'),
            F.col('data.canal').alias('canal'), F.col('data.row_hash').alias('row_hash'),
        )
        conflicts = df.groupBy('pedido_id').agg(F.countDistinct('row_hash').alias('n')).filter('n > 1')
        if conflicts.take(1):
            reject_conflicts(conflicts, 'PEDIDO_DUPLICADO_CONFLITANTE', 'Pedidos conflitantes no lote')
        df = df.dropDuplicates(['pedido_id'])
    else:
        df = clean.select(F.col('data.pedido_id').alias('pedido_id'), F.col('data.resultado').alias('resultado'),
                          F.to_timestamp('data.atualizado_em').alias('atualizado_em'))
        conflicts = df.groupBy('pedido_id', 'atualizado_em').agg(F.countDistinct('resultado').alias('n')).filter('n > 1')
        if conflicts.take(1):
            reject_conflicts(conflicts, 'PAGAMENTO_VERSAO_CONFLITANTE', 'Pagamentos conflitantes no mesmo instante')
        window = Window.partitionBy('pedido_id').orderBy(F.col('atualizado_em').desc())
        df = df.withColumn('_n', F.row_number().over(window)).filter('_n = 1').drop('_n')
    result = df.withColumn('attempt_id', F.lit(attempt)).cache()
    return (result, provenance, stats) if with_details else result
