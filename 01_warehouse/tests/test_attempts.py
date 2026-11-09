"""Fault injection do driver Glue, sem chamar servicos nem gravar dados reais."""
import hashlib
import io
import json
import runpy
import sys
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))


class Body:
    def __init__(self, data): self.data = data
    def read(self, *unused): return self.data
    def iter_chunks(self, chunk_size): yield self.data


class FakeS3:
    def __init__(self, failure):
        self.failure = failure
        self.writes = {}
        self.source = 'pedido sintético'.encode()
        self.manifest = json.dumps({'schema_version': 1, 'lote_id': 'lote_001', 'files': [
            {'kind': 'pedidos', 'key': 'entrada/lote_001/p.csv', 'version_id': 'v1',
             'sha256': hashlib.sha256(self.source).hexdigest()}]}).encode()
    def get_object(self, Bucket, Key, **kwargs):
        if Key.startswith('manifestos/'):
            if self.failure == 'manifesto': raise RuntimeError('erro sensivel')
            return {'Body': Body(self.manifest)}
        if Key.startswith('entrada/'): return {'Body': Body(self.source)}
        return {'Body': Body(b'SELECT 1;')}
    def copy_object(self, **kwargs): pass
    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.failure == 'evidencia' and Key.startswith('evidencias/'):
            raise RuntimeError('erro sensivel depois do commit')
        self.writes[Key] = json.loads(Body)


class FakeRedshift:
    def __init__(self): self.sql = []
    def execute_statement(self, Sql, **kwargs):
        self.sql.append(Sql)
        return {'Id': '1'}
    def batch_execute_statement(self, Sqls, **kwargs):
        self.sql.extend(Sqls)
        return {'Id': '1'}
    def describe_statement(self, **kwargs): return {'Status': 'FINISHED'}
    def get_statement_result(self, **kwargs): return {'Records': []}


class FakeFrame:
    def count(self): return 1
    def distinct(self): return self
    def unionByName(self, other): return self


class AttemptsTest(unittest.TestCase):
    def run_job(self, failure):
        s3, redshift = FakeS3(failure), FakeRedshift()
        class Session:
            def __init__(self, **kwargs): pass
            def client(self, name, **kwargs):
                if name == 's3': return s3
                if name == 'redshift-data': return redshift
                return types.SimpleNamespace(get_cluster_credentials=lambda **kwargs: {'DbUser': 'u', 'DbPassword': 'p'})
        class Job:
            def __init__(self, context): pass
            def init(self, *args): pass
            def commit(self): pass
        spark = types.SimpleNamespace(conf=types.SimpleNamespace(set=lambda *a: None), stop=lambda: None)
        context = types.SimpleNamespace(spark_session=spark, write_dynamic_frame=types.SimpleNamespace(
            from_options=lambda **kwargs: None))
        def resolve(argv, names):
            if 'JOB_RUN_ID' in names: raise RuntimeError('reserved Glue argument')
            data = {key[2:]: argv[index+1] for index, key in enumerate(argv[:-1]) if key.startswith('--')}
            return {name: data[name] for name in names} | {'JOB_RUN_ID': data.get('JOB_RUN_ID')}
        modules = {
            'boto3': types.SimpleNamespace(Session=Session),
            'awsglue': types.ModuleType('awsglue'),
            'awsglue.context': types.SimpleNamespace(GlueContext=lambda *_: context),
            'awsglue.dynamicframe': types.SimpleNamespace(DynamicFrame=types.SimpleNamespace(fromDF=lambda df, *_: df)),
            'awsglue.job': types.SimpleNamespace(Job=Job),
            'awsglue.utils': types.SimpleNamespace(getResolvedOptions=resolve),
            'pyspark.context': types.SimpleNamespace(SparkContext=types.SimpleNamespace(getOrCreate=lambda: None)),
        }
        from warehouse_spark import QualityError
        modules['warehouse_spark'] = types.SimpleNamespace(QualityError=QualityError,
            prepare=lambda *a, **kw: (FakeFrame(), FakeFrame(), {'recebidos': 1, 'aceitos': 1, 'rejeitados': 0}))
        options = {'JOB_NAME': 'job', 'ASSET_BUCKET': 'bucket', 'REDSHIFT_CLUSTER': 'cluster', 'REDSHIFT_USER': 'awsuser',
                   'REDSHIFT_DATABASE': 'loja', 'REDSHIFT_HOST': 'host', 'REDSHIFT_S3_ROLE': 'role',
                   'REGION': 'us-east-1', 'MANIFEST_BUCKET': 'bucket',
                   'MANIFEST_KEY': 'manifestos/lote_001/ready.json', 'MANIFEST_VERSION': 'vm1',
                   'JOB_RUN_ID': 'jr-test', 'ATTEMPT_ID': 'workflow-1', 'STEP_EXECUTION_ARN': 'execution-1'}
        argv = ['warehouse_etl.py'] + [item for key, value in options.items() for item in ('--'+key, value)]
        with patch.dict(sys.modules, modules), patch.object(sys, 'argv', argv), redirect_stdout(io.StringIO()) as output:
            with self.assertRaises(RuntimeError):
                runpy.run_path(str(Path(__file__).resolve().parents[1] / 'jobs/warehouse_etl.py'), run_name='__main__')
        return s3.writes, redshift.sql, output.getvalue()

    def test_failure_before_redshift_has_external_audit(self):
        writes, sql, output = self.run_job('manifesto')
        failures = [value for key, value in writes.items() if key.endswith('/falha.json')]
        self.assertEqual(failures[0]['status'], 'FALHOU')
        self.assertEqual(failures[0]['manifesto_version'], 'vm1')
        self.assertEqual(failures[0]['glue_job_run_id'], 'jr-test')
        self.assertEqual(sql, [])
        self.assertNotIn('erro sensivel', output)

    def test_failure_after_commit_is_not_reported_as_rollback(self):
        writes, sql, output = self.run_job('evidencia')
        failures = [value for key, value in writes.items() if key.endswith('/falha.json')]
        self.assertEqual(failures[0]['status'], 'PUBLICADO_EVIDENCIA_PENDENTE')
        self.assertEqual(failures[0]['workflow_attempt_id'], 'workflow-1')
        self.assertTrue(any(statement.startswith('CALL public.publicar_lote') for statement in sql))
        self.assertNotIn('erro sensivel', output)


if __name__ == '__main__': unittest.main()
