"""Teste AWS opt-in em schema novo: inicializacao, verificacao e depois carga."""
import os
import sys
import time
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from inicializar_modelo import initialize
from test_bootstrap import FakeS3


@unittest.skipUnless(os.getenv('AULA23_TEST_REDSHIFT') == '1', 'Teste AWS exige opt-in explicito')
class BootstrapLiveTest(unittest.TestCase):
    def test_setup_then_insert(self):
        import boto3
        api = boto3.client('redshift-data', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        schema = 'a23_bootstrap_' + uuid.uuid4().hex[:10]
        conn = {'ClusterIdentifier': os.getenv('AULA23_TEST_CLUSTER', 'aula23-dw'),
                'Database': os.getenv('AULA23_TEST_DATABASE', 'aula23'),
                'DbUser': os.getenv('AULA23_TEST_USER', 'awsuser')}

        class Adapter:
            def translate(self, sql):
                return sql.replace('public.', schema + '.').replace("='public'", "='" + schema + "'")
            def execute_statement(self, Sql, **kwargs):
                return api.execute_statement(Sql=self.translate(Sql), **conn)
            def batch_execute_statement(self, Sqls, **kwargs):
                return api.batch_execute_statement(Sqls=[self.translate(sql) for sql in Sqls], **conn)
            def describe_statement(self, **kwargs): return api.describe_statement(**kwargs)
            def cancel_statement(self, **kwargs): return api.cancel_statement(**kwargs)
            def get_statement_result(self, **kwargs): return api.get_statement_result(**kwargs)

        def execute(sql, query=False):
            ident = api.execute_statement(Sql=sql, **conn)['Id']
            for _ in range(120):
                state = api.describe_statement(Id=ident)
                if state['Status'] == 'FINISHED':
                    return api.get_statement_result(Id=ident)['Records'] if query else None
                if state['Status'] in ('FAILED', 'ABORTED'):
                    raise AssertionError(state.get('Error'))
                time.sleep(.5)
            raise TimeoutError(ident)

        execute('CREATE SCHEMA ' + schema)
        print('Schema sintetico persistente:', schema, flush=True)
        # Primeira fase: executa o mesmo codigo do job de bootstrap.
        initialize(FakeS3(), Adapter(), 'bucket-nao-usado', 'workgroup-nao-usado', conn['Database'])
        before = execute('SELECT COUNT(*) FROM ' + schema + '.fato_pedidos', query=True)
        self.assertEqual(before[0][0]['longValue'], 0)
        # Segunda fase: um lote sintetico; a procedure e a mesma usada pelo Glue.
        execute(f"INSERT INTO {schema}.stg_pedidos VALUES ('P1','2026-10-20',10,'pago','site','{'a'*64}','tentativa')")
        execute(f"INSERT INTO {schema}.stg_pagamentos VALUES ('P1','aprovado','2026-10-20 12:00:00','tentativa')")
        execute(f"CALL {schema}.publicar_lote('lote_001','{'b'*64}','tentativa')")
        result = execute(f'SELECT count(*),sum(receita) FROM {schema}.fato_pedidos', query=True)
        self.assertEqual(result[0][0]['longValue'], 1)
        self.assertEqual(result[0][1]['stringValue'], '10.00')


if __name__ == '__main__': unittest.main()
