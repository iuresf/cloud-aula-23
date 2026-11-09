
import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MATRICULA', 'LOTE'])
gc = GlueContext(SparkContext.getOrCreate())
spark = gc.spark_session
spark.conf.set('spark.sql.ansi.enabled', 'false')
spark.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
job = Job(gc)
job.init(args['JOB_NAME'], args)

# Buckets da matrícula
M = args['MATRICULA'].strip().lower()
L = args['LOTE']

prefixo = (
    getResolvedOptions(sys.argv, ['PREFIXO'])['PREFIXO']
    if '--PREFIXO' in sys.argv
    else 'ufpi-cead-infovis'
)

if not re.fullmatch(r'[a-z0-9]+', M):
    raise ValueError('MATRICULA: use somente letras minusculas e numeros, sem < >')

buckets = {
    tipo: f'{prefixo}-{tipo}-{M}'
    for tipo in ['raw', 'stage', 'final', 'quarantine', 'audit', 'athena', 'apoio']
}

if not all(
    re.fullmatch(r'[a-z0-9][a-z0-9-]{1,61}[a-z0-9]', n)
    for n in buckets.values()
):
    raise ValueError('Nome de bucket S3 invalido')

RAW, STAGE, FINAL, QUARANTINE, AUDIT = (
    's3://' + buckets[t]
    for t in ['raw', 'stage', 'final', 'quarantine', 'audit']
)

# Leitura dos CSVs diretamente do S3
def csv_read(source, schema):
    return (
        spark.read.schema(schema)
        .option('header', True)
        .option('sep', ';')
        .option('mode', 'FAILFAST')
        .csv(f'{RAW}/{source}/{L}/')
    )

# Conversão de valores monetários brasileiros
def money(name):
    t = F.regexp_replace(F.trim(F.col(name)), r'^R\$\s*', '')
    ok = t.rlike(r'^(\d+|\d{1,3}(\.\d{3})+),\d{2}$')
    return F.when(
        ok,
        F.regexp_replace(
            F.regexp_replace(t, r'\.', ''),
            ',', '.'
        ).cast('decimal(12,2)')
    )

# Escrita em Parquet
def write(df, destino, path):
    (
        df.withColumn('lote', F.lit(L))
        .write.mode('errorifexists')
        .parquet(f'{destino}/{path}/')
    )

# Validação de fontes auxiliares
def checked(df, key, bad):
    assert df.filter(bad).count() == 0, 'Fonte auxiliar invalida: revisar RAW'
    assert df.count() == df.select(key).distinct().count(), 'Chave auxiliar duplicada'
    return df

# 1. ERP
erp = csv_read(
    'erp',
    'linha_id string, pedido_id string, cliente_id string, '
    'data_venda string, valor string, status string'
)

erp = (
    erp.withColumn('arquivo_origem', F.input_file_name())
    .withColumn('fonte', F.lit('erp'))
)

n = (
    erp.withColumn('valor_original', F.col('valor'))
    .withColumn('data_original', F.col('data_venda'))
    .withColumn('status_original', F.col('status'))
    .withColumn('pedido_id', F.upper(F.trim('pedido_id')))
    .withColumn('cliente_id', F.upper(F.trim('cliente_id')))
    .withColumn('status', F.lower(F.trim('status')))
    .withColumn('valor', money('valor'))
    .withColumn(
        'data_venda',
        F.coalesce(
            F.to_date('data_venda', 'yyyy-MM-dd'),
            F.to_date('data_venda', 'dd/MM/yyyy')
        )
    )
)

motivo = (
    F.when(F.coalesce(F.col('pedido_id'), F.lit('')) == '', 'id_ausente')
    .when(F.coalesce(F.col('cliente_id'), F.lit('')) == '', 'cliente_ausente')
    .when(F.col('data_venda').isNull(), 'data_invalida')
    .when(F.col('valor').isNull() | (F.col('valor') < 0), 'valor_invalido')
    .when(
        F.col('status').isNull() |
        ~F.col('status').isin('aprovado', 'cancelado'),
        'status_invalido'
    )
    .otherwise('ok')
)

n = n.withColumn('motivo', motivo)

write(
    n.filter("motivo <> 'ok'"),
    QUARANTINE,
    f'{L}/job1_invalidos'
)

valid = n.filter("motivo = 'ok'").drop('motivo')

# Deduplicação
sig = F.struct('cliente_id', 'data_venda', 'valor', 'status')

g = valid.groupBy('pedido_id').agg(
    F.countDistinct(sig).alias('versoes')
)

valid = valid.join(g, 'pedido_id')

write(
    valid.filter('versoes > 1').withColumn(
        'motivo', F.lit('conflito_mesma_chave')
    ),
    QUARANTINE,
    f'{L}/job1_conflitos'
)

w = Window.partitionBy('pedido_id').orderBy('linha_id')

ranked = valid.filter('versoes = 1').withColumn(
    'ordem', F.row_number().over(w)
)

write(
    ranked.filter('ordem > 1').withColumn(
        'motivo', F.lit('duplicata_identica')
    ),
    AUDIT,
    f'{L}/duplicatas'
)

stage = ranked.filter('ordem = 1').drop('versoes', 'ordem')
write(stage, STAGE, f'{L}/erp')

# 2. CLIENTES
cli = csv_read(
    'clientes',
    'cliente_id string, cidade string, telefone string'
)

cli = (
    cli.withColumn('cliente_id', F.upper(F.trim('cliente_id')))
    .withColumn('cidade_original', F.col('cidade'))
    .withColumn('cidade', F.initcap(F.trim('cidade')))
)

cli = checked(
    cli, 'cliente_id',
    F.col('cliente_id').isNull() |
    (F.col('cliente_id') == '') |
    F.col('cidade').isNull() |
    ~F.col('cidade').isin('Picos', 'Teresina', 'Parnaíba')
)

write(cli, STAGE, f'{L}/clientes')

# 3. LOJA (JSONL)
loja = (
    spark.read.schema(
        'order_id string, customer_id string, '
        'total string, currency string'
    )
    .option('mode', 'FAILFAST')
    .json(f'{RAW}/loja/{L}/')
)

loja = loja.select(
    F.upper(F.trim('order_id')).alias('pedido_id'),
    F.upper(F.trim('customer_id')).alias('cliente_loja'),
    F.col('total').cast('decimal(12,2)').alias('valor_loja'),
    F.upper(F.trim('currency')).alias('moeda_loja')
)

loja = checked(
    loja, 'pedido_id',
    F.col('pedido_id').isNull() |
    (F.col('pedido_id') == '') |
    F.col('cliente_loja').isNull() |
    (F.col('cliente_loja') == '') |
    F.col('valor_loja').isNull() |
    (F.col('valor_loja') < 0) |
    F.col('moeda_loja').isNull() |
    (F.col('moeda_loja') != 'BRL')
)

write(loja, STAGE, f'{L}/loja')

# 4. PAGAMENTOS
pag = csv_read(
    'pagamentos',
    'transacao_id string, pedido_id string, '
    'valor_pago string, moeda string, status_pagamento string'
)

pag = (
    pag.withColumn('pedido_id', F.upper(F.trim('pedido_id')))
    .withColumn('valor_pago', money('valor_pago'))
    .withColumn('moeda', F.upper(F.trim('moeda')))
    .withColumn('status_pagamento', F.lower(F.trim('status_pagamento')))
)

pag = checked(
    pag, 'pedido_id',
    F.col('pedido_id').isNull() |
    (F.col('pedido_id') == '') |
    F.col('transacao_id').isNull() |
    F.col('valor_pago').isNull() |
    (F.col('valor_pago') < 0) |
    F.col('moeda').isNull() |
    (F.col('moeda') != 'BRL') |
    F.col('status_pagamento').isNull() |
    ~F.col('status_pagamento').isin('aprovado', 'estornado')
)

write(pag, STAGE, f'{L}/pagamentos')

# Conferência do lote didático
if L == 'lote01':
    assert erp.count() == 10 and stage.count() == 6, \
        'Conferir lote didatico inicial'

print(
    f'CHECKPOINT_JOB1: RAW={erp.count()} '
    f'STAGE_ERP={stage.count()}'
)

job.commit()
