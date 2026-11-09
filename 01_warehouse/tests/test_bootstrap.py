"""Primeira fase: cria e verifica esquema, sem carregar dados de negocio."""
import io
import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from inicializar_modelo import initialize, TABLE_COLUMNS

spec = importlib.util.spec_from_file_location('esperar_bootstrap', Path(__file__).resolve().parents[1] / 'scripts' / 'esperar_bootstrap.py')
waiter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(waiter)


class FakeS3:
    def get_object(self, Bucket, Key):
        root = Path(__file__).resolve().parents[1]
        return {'Body': io.BytesIO((root / Key).read_bytes())}


class FakeDataAPI:
    def __init__(self, missing=None, procedure=True):
        self.missing, self.procedure = missing, procedure
        self.statements = []
        self.ids = {}
    def batch_execute_statement(self, Sqls, **kwargs):
        self.statements.extend(Sqls)
        return {'Id': 'batch'}
    def execute_statement(self, Sql, **kwargs):
        self.statements.append(Sql)
        ident = str(len(self.statements))
        self.ids[ident] = Sql
        return {'Id': ident}
    def describe_statement(self, Id):
        return {'Status': 'FINISHED'}
    def get_statement_result(self, Id, **kwargs):
        if 'information_schema.columns' in self.ids[Id]:
            rows = [[{'stringValue': table}, {'stringValue': col}]
                    for table, columns in TABLE_COLUMNS.items() for col in columns.split()
                    if (table, col) != self.missing]
            return {'Records': rows}
        return {'Records': [[{'longValue': int(self.procedure)}]]}


class BootstrapTest(unittest.TestCase):
    def test_creates_and_verifies_before_load(self):
        api = FakeDataAPI()
        initialize(FakeS3(), api, 'bucket', 'workgroup', 'loja')
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS public.fato_pedidos' in q for q in api.statements))
        self.assertTrue(any('CREATE OR REPLACE PROCEDURE public.publicar_lote' in q for q in api.statements))
        self.assertFalse(any(q.lstrip().upper().startswith('CALL ') for q in api.statements))
        self.assertFalse(any(q.lstrip().upper().startswith('INSERT ') for q in api.statements))

    def test_missing_column_blocks_first_phase(self):
        api = FakeDataAPI(missing=('fato_pedidos', 'receita'))
        with self.assertRaisesRegex(RuntimeError, 'Colunas ausentes em fato_pedidos'):
            initialize(FakeS3(), api, 'bucket', 'workgroup', 'loja')

    def test_missing_procedure_blocks_first_phase(self):
        with self.assertRaisesRegex(RuntimeError, 'Procedure publicar_lote ausente'):
            initialize(FakeS3(), FakeDataAPI(procedure=False), 'bucket', 'workgroup', 'loja')

    def test_terraform_waits_for_success(self):
        responses = [{'JobRunId': 'jr-test'}, {'JobRun': {'JobRunState': 'RUNNING'}},
                     {'JobRun': {'JobRunState': 'SUCCEEDED'}}]
        with patch.dict(os.environ, {'A23_BOOTSTRAP_JOB': 'job', 'A23_REGION': 'us-east-1'}), \
             patch.object(waiter, 'cli', side_effect=responses) as cli, patch.object(waiter.time, 'sleep'):
            waiter.main()
        self.assertEqual(cli.call_count, 3)

    def test_terraform_fails_when_bootstrap_fails(self):
        responses = [{'JobRunId': 'jr-test'}, {'JobRun': {'JobRunState': 'FAILED'}}]
        with patch.dict(os.environ, {'A23_BOOTSTRAP_JOB': 'job', 'A23_REGION': 'us-east-1'}), \
             patch.object(waiter, 'cli', side_effect=responses):
            with self.assertRaisesRegex(RuntimeError, 'Inicializacao falhou: FAILED'):
                waiter.main()


if __name__ == '__main__': unittest.main()
