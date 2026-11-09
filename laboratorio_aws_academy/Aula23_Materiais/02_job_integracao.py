
# Aula 23 - InfoVis
# AWS Glue Job 2: integrar, conciliar e publicar
# Compativel com o Job 1 de sete buckets por matricula.
#
# Parametros obrigatorios:
# --MATRICULA
# --LOTE
# --PUBLICAR_PARCIAL
#
# Parametro opcional:
# --PREFIXO (padrao: ufpi-cead-infovis)

import re
import sys
from decimal import Decimal

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# 1. Parametros do job e inicializacao do Spark

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'MATRICULA', 'LOTE', 'PUBLICAR_PARCIAL']
)

prefixo = (
    getResolvedOptions(sys.argv, ['PREFIXO'])['PREFIXO']
    .strip().lower()
    if '--PREFIXO' in sys.argv
    else 'ufpi-cead-infovis'
)

matricula = args['MATRICULA'].strip().lower()
lote = args['LOTE'].strip()
publicar_parcial = args['PUBLICAR_PARCIAL'].strip().lower()

if not re.fullmatch(r'[a-z0-9]+', matricula):
    raise ValueError(
        'MATRICULA deve conter somente letras minusculas e numeros'
    )

if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]*', lote):
    raise ValueError(
        'LOTE deve conter somente letras, numeros, _ e -'
    )

if publicar_parcial not in ('true', 'false'):
    raise ValueError('PUBLICAR_PARCIAL deve ser true ou false')


# 2. Gerar os nomes dos sete buckets

buckets = {
    tipo: f'{prefixo}-{tipo}-{matricula}'
    for tipo in [
        'raw',
        'stage',
        'final',
        'quarantine',
        'audit',
        'athena',
        'apoio'
    ]
}

if not all(
    re.fullmatch(r'[a-z0-9][a-z0-9-]{1,61}[a-z0-9]', nome)
    for nome in buckets.values()
):
    raise ValueError('Os nomes dos buckets S3 sao invalidos')

STAGE = 's3://' + buckets['stage']
FINAL = 's3://' + buckets['final']
QUARANTINE = 's3://' + buckets['quarantine']
AUDIT = 's3://' + buckets['audit']


# 3. Inicializar Glue e Spark

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
spark = gc.spark_session

job = Job(gc)
job.init(args['JOB_NAME'], args)


# 4. Funcoes auxiliares

def ler_stage(fonte):
    return (
        spark.read
        .parquet(f'{STAGE}/{lote}/{fonte}/')
        .drop('lote')
    )


def conferir_chave(df, chave, fonte):

    chave_ausente = (
        F.col(chave).isNull() |
        (F.trim(F.col(chave)) == '')
    )

    if df.filter(chave_ausente).limit(1).count():
        raise ValueError(
            f'{fonte}: chave {chave} ausente'
        )

    total = df.count()
    distintas = df.select(chave).distinct().count()

    if total != distintas:
        raise ValueError(
            f'{fonte}: chave {chave} duplicada; '
            'JOIN multiplicaria linhas'
        )


def gravar(df, destino, caminho):

    # Nao sobrescrever evidencias anteriores.
    df.write.mode('errorifexists').parquet(
        f'{destino}/{caminho}/'
    )


# 5. Ler os dados produzidos pelo Job 1

erp = ler_stage('erp')
clientes = ler_stage('clientes')
loja = ler_stage('loja')
pagamentos = ler_stage('pagamentos')


# 6. Validar unicidade das chaves

for df, chave, fonte in [
    (erp, 'pedido_id', 'ERP'),
    (clientes, 'cliente_id', 'clientes'),
    (loja, 'pedido_id', 'loja'),
    (pagamentos, 'pedido_id', 'pagamentos')
]:
    conferir_chave(df, chave, fonte)


# 7. Integrar as quatro fontes

# LEFT JOIN preserva pedidos sem correspondencia.

clientes = clientes.withColumn(
    'cliente_encontrado',
    F.lit(True)
)

integ = (
    erp
    .join(clientes, 'cliente_id', 'left')
    .join(loja, 'pedido_id', 'left')
    .join(pagamentos, 'pedido_id', 'left')
)

qtd_erp = erp.count()

if integ.count() != qtd_erp:
    raise ValueError(
        'JOIN alterou a quantidade de pedidos ERP'
    )


# 8. Detectar fontes ausentes

# Comparacoes com NULL no Spark podem resultar
# em NULL. Por isso verificamos explicitamente.

sem_loja = (
    F.col('cliente_loja').isNull() |
    F.col('valor_loja').isNull() |
    F.col('moeda_loja').isNull()
)

sem_pagamento = (
    F.col('transacao_id').isNull() |
    F.col('valor_pago').isNull() |
    F.col('moeda').isNull() |
    F.col('status_pagamento').isNull()
)


# 9. Reconciliar os pedidos

motivo = (
    F.when(
        F.col('status') == 'cancelado',
        'fora_indicador'
    )
    .when(
        F.col('cliente_encontrado').isNull(),
        'cliente_orfao'
    )
    .when(
        sem_loja | sem_pagamento,
        'fonte_ausente'
    )
    .when(
        F.col('cliente_id') != F.col('cliente_loja'),
        'cliente_divergente'
    )
    .when(
        (F.col('moeda_loja') != 'BRL') |
        (F.col('moeda') != 'BRL'),
        'moeda_divergente'
    )
    .when(
        F.col('status_pagamento') != 'aprovado',
        'pagamento_nao_aprovado'
    )
    .when(
        (F.col('valor') != F.col('valor_loja')) |
        (F.col('valor') != F.col('valor_pago')),
        'valor_divergente'
    )
    .otherwise('publicavel')
)

integ = (
    integ
    .withColumn('motivo', motivo)
    .withColumn('lote', F.lit(lote))
)


# 10. Separar pedidos pendentes

pendentes = integ.filter(
    ~F.col('motivo').isin(
        'publicavel',
        'fora_indicador'
    )
)


# 11. Preparar os pedidos publicaveis

final = (
    integ
    .filter(F.col('motivo') == 'publicavel')
    .select(
        'pedido_id',
        'cliente_id',
        'data_venda',
        'cidade',
        F.col('valor').alias('receita'),
        'transacao_id',
        'arquivo_origem',
        'lote'
    )
)


# 12. Portao de qualidade

# Executar validacoes ANTES de gravar os arquivos.

elegiveis = erp.filter(
    F.col('status') == 'aprovado'
).count()

publicaveis = final.count()

if elegiveis == 0 or publicaveis == 0:
    raise ValueError(
        'Sem pedidos elegiveis/publicaveis: '
        'publicacao bloqueada'
    )

campos_nulos = (
    F.col('pedido_id').isNull() |
    F.col('cidade').isNull() |
    F.col('data_venda').isNull() |
    F.col('receita').isNull() |
    F.col('transacao_id').isNull()
)

if final.filter(campos_nulos).limit(1).count():
    raise ValueError(
        'FINAL possui campos obrigatorios nulos'
    )

chaves_finais = (
    final.select('pedido_id').distinct().count()
)

if chaves_finais != publicaveis:
    raise ValueError(
        'FINAL contem pedidos duplicados'
    )


# 13. Calcular a receita total

total_receita = (
    final
    .agg(F.sum('receita').alias('total'))
    .first()['total']
)


# 14. Conferencia do lote didatico

# Gabarito exclusivo dos dados sinteticos da aula.

if lote == 'lote01':

    if (
        qtd_erp != 6 or
        elegiveis != 5 or
        publicaveis != 3 or
        total_receita != Decimal('420.00') or
        pendentes.count() != 2
    ):
        raise ValueError(
            'Lote didatico: conferir as '
            'contagens e R$ 420,00'
        )


# 15. Verificar autorizacao de publicacao

parcial = publicaveis < elegiveis
cobertura = publicaveis / elegiveis

if parcial and publicar_parcial != 'true':

    raise RuntimeError(
        'Publicacao parcial nao autorizada: '
        'habilitar --PUBLICAR_PARCIAL=true '
        'apos conferir o checkpoint'
    )


# 16. Adicionar metadados da publicacao

final = (
    final
    .withColumn(
        'cobertura',
        F.lit(cobertura)
    )
    .withColumn(
        'publicacao_parcial',
        F.lit(parcial)
    )
)


# 17. Gravar as evidencias e o resultado

# Todas as verificacoes anteriores passaram.
# Gravacoes Parquet separadas nao sao atomicas.

gravar(
    integ,
    AUDIT,
    f'{lote}/integracao'
)

gravar(
    pendentes,
    QUARANTINE,
    f'{lote}/job2'
)

gravar(
    final,
    FINAL,
    f'{lote}/vendas_integradas'
)


# 18. Registrar o checkpoint no log

print(
    f'CHECKPOINT_JOB2: '
    f'ERP_STAGE={qtd_erp} '
    f'FINAL={publicaveis} '
    f'ELEGIVEIS={elegiveis} '
    f'PENDENTES={pendentes.count()} '
    f'RECEITA={total_receita} '
    f'COBERTURA={cobertura:.2f} '
    f'PUBLICACAO_PARCIAL={str(parcial).lower()}'
)

job.commit()
