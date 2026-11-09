"""Glue Python Shell: inicializa e verifica o modelo antes de liberar a carga."""
import argparse
import json
import time


TABLE_COLUMNS = {
    'dim_tempo': 'sk_tempo data ano mes',
    'dim_canal': 'sk_canal canal',
    'fato_pedidos': 'pedido_id sk_tempo sk_canal valor receita resultado',
    'dw_pedidos_origem': 'pedido_id data valor status canal row_hash',
    'dw_pagamentos_estado': 'pedido_id resultado atualizado_em',
    'stg_pedidos': 'pedido_id data valor status canal row_hash attempt_id',
    'stg_pagamentos': 'pedido_id resultado atualizado_em attempt_id',
    'etl_lotes': 'lote_id manifesto_sha256 attempt_id processado_em',
    'etl_execucoes': 'attempt_id workflow_attempt_id lote_id step_execution_arn glue_job_run_id manifesto_key manifesto_version manifesto_sha256 inicio termino status recebidos aceitos rejeitados erro_codigo mensagem_erro evidencia_s3',
    'stg_proveniencia': 'pedido_id tipo arquivo versao_s3 attempt_id',
    'dw_proveniencia': 'pedido_id lote_id attempt_id tipo arquivo versao_s3 ingerido_em operacao',
}
TABLES = frozenset(TABLE_COLUMNS)


def initialize(s3, redshift, bucket, cluster, database, db_user='awsuser'):
    """DDL idempotente; nenhuma insercao de dados de negocio."""
    connection = {'ClusterIdentifier': cluster, 'Database': database, 'DbUser': db_user}

    def execute(sql, batch=False):
        result = (redshift.batch_execute_statement(Sqls=sql, **connection) if batch
                  else redshift.execute_statement(Sql=sql, **connection))
        ident = result['Id']
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            state = redshift.describe_statement(Id=ident)
            if state['Status'] == 'FINISHED':
                return ident
            if state['Status'] in ('FAILED', 'ABORTED'):
                raise RuntimeError('Data API falhou: ' + state.get('Error', state['Status']))
            time.sleep(2)
        redshift.cancel_statement(Id=ident)
        raise TimeoutError('Timeout inicializando esquema')

    def asset(key):
        return s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

    sql = asset('sql/01_modelo.sql')
    execute([statement.strip() for statement in sql.split(';') if statement.strip()], batch=True)
    execute(asset('sql/05_publicar.sql'))
    names = ','.join("'" + name + "'" for name in sorted(TABLES))
    ident = execute("SELECT table_name,column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name IN (" + names + ")")
    rows, token = [], None
    while True:
        page = redshift.get_statement_result(Id=ident, **({'NextToken': token} if token else {}))
        rows.extend(page['Records'])
        token = page.get('NextToken')
        if not token:
            break
    found = {}
    for row in rows:
        found.setdefault(row[0]['stringValue'], set()).add(row[1]['stringValue'])
    missing = TABLES - found.keys()
    if missing:
        raise RuntimeError('Tabelas ausentes: ' + ', '.join(sorted(missing)))
    for table, columns in TABLE_COLUMNS.items():
        missing_columns = set(columns.split()) - found[table]
        if missing_columns:
            raise RuntimeError('Colunas ausentes em ' + table + ': ' + ', '.join(sorted(missing_columns)))
    ident = execute("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid "
                    "WHERE n.nspname='public' AND p.proname='publicar_lote'")
    rows = redshift.get_statement_result(Id=ident)['Records']
    if not rows or rows[0][0].get('longValue', 0) < 1:
        raise RuntimeError('Procedure publicar_lote ausente')
    print(json.dumps({'status': 'MODELO_VERIFICADO', 'tabelas': sorted(TABLES),
                      'procedure': 'public.publicar_lote'}), flush=True)


def main():
    import boto3
    parser = argparse.ArgumentParser()
    for name in ('ASSET_BUCKET', 'REDSHIFT_CLUSTER', 'REDSHIFT_DATABASE', 'REDSHIFT_USER', 'REGION'):
        parser.add_argument('--' + name, required=True)
    args, _ = parser.parse_known_args()
    session = boto3.Session(region_name=args.REGION)
    initialize(session.client('s3'), session.client('redshift-data'), args.ASSET_BUCKET,
               args.REDSHIFT_CLUSTER, args.REDSHIFT_DATABASE, args.REDSHIFT_USER)


if __name__ == '__main__':
    main()
