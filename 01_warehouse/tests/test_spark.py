"""Testa as transformacoes reais do Glue em Spark local, sem servicos AWS."""
import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from pyspark.sql import SparkSession
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from warehouse_spark import prepare


class SparkTransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('aula23-validation').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.spark.conf.set('spark.sql.session.timeZone','UTC')
        cls.temp = tempfile.TemporaryDirectory(prefix='a23-spark-')
        cls.root = Path(__file__).resolve().parents[2]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop(); cls.temp.cleanup()

    def test_actual_initial_and_latest_payment(self):
        orders = prepare(self.spark,[str(self.root/'dados/inicial/pedidos/lote_001.csv')],'pedidos','run1',self.temp.name+'/reject').collect()
        self.assertEqual(len(orders),2)
        self.assertEqual(sum(x.valor for x in orders),Decimal('208.90'))
        self.assertEqual({r.attempt_id for r in orders},{'run1'})
        payments = prepare(self.spark,[str(self.root/'dados/inicial/pagamentos/lote_001.jsonl'),str(self.root/'dados/delta/pagamentos/lote_002.jsonl')],'pagamentos','run1',self.temp.name+'/reject').collect()
        by_id={r.pedido_id:r.resultado for r in payments}
        self.assertEqual(by_id,{'P1042':'estornado','P1043':'sem_cobranca'})

    def test_conflicting_order_is_rejected(self):
        path=Path(self.temp.name)/'conflict.csv'
        path.write_text('id;dia;valor;status;canal\nP1;20/10/2026;10,00;pago;site\nP1;20/10/2026;20,00;pago;site\n')
        with self.assertRaisesRegex(ValueError,'conflitantes'):
            prepare(self.spark,[str(path)],'pedidos','run2',self.temp.name+'/reject')

    def test_same_time_conflicting_payment_is_rejected(self):
        path=Path(self.temp.name)/'conflict.jsonl'
        path.write_text('{"pedido_id":"P1","resultado":"aprovado","atualizado_em":"2026-10-20T10:00:00"}\n{"pedido_id":"P1","resultado":"estornado","atualizado_em":"2026-10-20T10:00:00"}\n')
        with self.assertRaisesRegex(ValueError,'conflitantes'):
            prepare(self.spark,[str(path)],'pagamentos','run3',self.temp.name+'/reject')

    def test_invalid_record_is_saved_outside_staging(self):
        path=Path(self.temp.name)/'invalid.csv'
        path.write_text('id;dia;valor;status;canal\nP1;31/02/2026;10,00;pago;site\n')
        with self.assertRaisesRegex(ValueError,'invalidos'):
            prepare(self.spark,[str(path)],'pedidos','run4',self.temp.name+'/reject')
        self.assertTrue(list((Path(self.temp.name)/'reject/pedidos').glob('part-*.json')))

    def test_partial_rejection_protects_source_and_captures_provenance(self):
        path=Path(self.temp.name)/'partial.csv'
        path.write_text('id;dia;valor;status;canal\nP1;20/10/2026;10,00;pago;site\nP2;31/02/2026;99,00;pago;site\n')
        metadata = {path.as_uri(): {'key': 'entrada/lote_teste/pedidos.csv', 'version_id': 'versao-123'}}
        clean, provenance, stats = prepare(self.spark, [str(path)], 'pedidos', 'run5', self.temp.name+'/reject',
                                           lote='lote_teste', allow_partial=True, with_details=True,
                                           source_metadata=metadata)
        self.assertEqual(stats, {'recebidos': 2, 'aceitos': 1, 'rejeitados': 1})
        self.assertEqual(clean.count(), 1)
        self.assertEqual(provenance.first().pedido_id, 'P1')
        self.assertEqual(provenance.first().arquivo, 'entrada/lote_teste/pedidos.csv')
        self.assertEqual(provenance.first().versao_s3, 'versao-123')
        records = self.spark.read.json(self.temp.name+'/reject/pedidos/').collect()
        self.assertEqual(records[0].motivo, 'DATA_INVALIDA')
        self.assertEqual(records[0].lote_id, 'lote_teste')
        self.assertEqual(records[0].arquivo, 'entrada/lote_teste/pedidos.csv')
        self.assertNotIn('P2', str(records[0]))

if __name__ == '__main__':unittest.main()
