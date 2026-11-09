"""Contratos SQL reais em schema isolado. Executar explicitamente com AULA23_TEST_REDSHIFT=1.

Cria schema de teste com dados sinteticos no cluster indicado; nao apaga schemas
ou tabelas existentes. A procedure continua sendo o SQL original com prefixo
public substituido pelo schema de teste.
"""
import os
import time
import unittest
import uuid
from pathlib import Path


@unittest.skipUnless(os.environ.get('AULA23_TEST_REDSHIFT') == '1', 'Teste AWS exige opt-in explicito')
class RedshiftLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import boto3
        cls.api = boto3.client('redshift-data', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        cls.cluster = os.getenv('AULA23_TEST_CLUSTER', 'aula23-dw')
        cls.database = os.getenv('AULA23_TEST_DATABASE', 'aula23')
        cls.user = os.getenv('AULA23_TEST_USER', 'awsuser')
        cls.schema = 'a23_test_' + uuid.uuid4().hex[:12]
        cls.root = Path(__file__).resolve().parents[1]
        cls.execute('create schema ' + cls.schema)
        model = (cls.root/'sql/01_modelo.sql').read_text().replace('public.', cls.schema + '.')
        for stmt in model.split(';'):
            if stmt.strip():
                cls.execute(stmt)
        procedure = (cls.root/'sql/05_publicar.sql').read_text().replace('public.', cls.schema + '.')
        cls.execute(procedure)
        print('Schema de teste persistente:', cls.schema, flush=True)

    @classmethod
    def execute(cls, sql, fail=False):
        result = cls.api.execute_statement(ClusterIdentifier=cls.cluster, Database=cls.database,
                                           DbUser=cls.user, Sql=sql)
        for _ in range(120):
            state = cls.api.describe_statement(Id=result['Id'])
            if state['Status'] == 'FINISHED':
                if fail:
                    raise AssertionError('Era esperada falha: ' + sql)
                if state.get('HasResultSet'):
                    return cls.api.get_statement_result(Id=result['Id'])['Records']
                return []
            if state['Status'] in ('FAILED', 'ABORTED'):
                if fail:
                    return state.get('Error', '')
                raise AssertionError(state.get('Error'))
            time.sleep(0.5)
        raise TimeoutError(result['Id'])

    def test_incremental_contract_and_recovery(self):
        s = self.schema
        a, b = 'a'*64, 'b'*64
        def order(attempt, pedido='P1', h='x'*64):
            self.execute(f"INSERT INTO {s}.stg_pedidos VALUES ('{pedido}','2026-10-20',10,'pago','site','{h}','{attempt}')")
        def payment(attempt, pedido='P1', when='2026-10-21 10:00:00', result='aprovado'):
            self.execute(f"INSERT INTO {s}.stg_pagamentos VALUES ('{pedido}','{result}','{when}','{attempt}')")
        def call(lote, h, attempt, fail=False):
            return self.execute(f"CALL {s}.publicar_lote('{lote}','{h}','{attempt}')", fail=fail)
        def scalar(sql):
            cell = self.execute(sql)[0][0]
            return next(iter(cell.values()))
        order('a1'); payment('a1')
        self.execute(f"INSERT INTO {s}.stg_proveniencia VALUES ('P1','pedidos','entrada/lote1/p.csv','v1','a1')")
        call('lote1', a, 'a1')
        call('lote1', a, 'a1')  # repeticao apos commit/evidencia S3 hipoteticamente falha
        self.assertEqual(scalar(f'SELECT COUNT(*) FROM {s}.fato_pedidos'), 1)
        self.assertEqual(scalar(f'SELECT COUNT(*) FROM {s}.dw_proveniencia'), 1)
        self.assertIn('outro conteudo', call('lote1', b, 'a2', fail=True))
        order('a2', h='y'*64)
        self.assertIn('conflitante', call('lote2', b, 'a2', fail=True))
        self.assertEqual(scalar(f"SELECT COUNT(*) FROM {s}.etl_lotes WHERE lote_id='lote2'"), 0)
        payment('a3', 'P2')
        self.assertIn('inexistente', call('lote3', a, 'a3', fail=True))
        self.assertEqual(scalar(f"SELECT COUNT(*) FROM {s}.etl_lotes WHERE lote_id='lote3'"), 0)
        order('a4', 'P2'); payment('a4', 'P2')
        call('lote3', a, 'a4')  # reprocessamento apos lote rejeitado, mesmo manifesto
        payment('a5', when='2026-10-20 10:00:00', result='estornado')
        call('lote4', a, 'a5')
        self.assertEqual(scalar(f"SELECT resultado FROM {s}.dw_pagamentos_estado WHERE pedido_id='P1'"), 'aprovado')
        self.assertEqual(scalar(f'SELECT COUNT(*) FROM {s}.fato_pedidos'), 2)
        self.assertEqual(scalar(f'SELECT SUM(receita) FROM {s}.fato_pedidos'), '20.00')


if __name__ == '__main__':
    unittest.main()
