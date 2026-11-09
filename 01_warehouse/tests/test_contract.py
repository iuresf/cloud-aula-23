import hashlib
import json
import sys
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from warehouse_core import clean_order, clean_payment, parse_manifest, failure_status


class ContractTest(unittest.TestCase):
    def order(self, **changes):
        return dict(id='P1042', dia='20/10/2026', valor='129,90', status='pago', canal='site', **{}) | changes

    def manifest(self, **changes):
        return dict(schema_version=1, lote_id='lote_001', files=[dict(kind='pedidos', key='entrada/lote_001/pedidos.csv', version_id='v123', sha256='a'*64)]) | changes

    def test_real_fixture_duplicates_converge(self):
        import csv
        f = Path(__file__).resolve().parents[2] / 'dados/inicial/pedidos/lote_001.csv'
        with f.open() as source:
            rows = [clean_order(r) for r in csv.DictReader(source, delimiter=';')]
        self.assertEqual(len({r['pedido_id'] for r in rows}), 2)
        self.assertEqual(rows[0]['row_hash'], rows[2]['row_hash'])
        self.assertEqual(rows[0]['valor'], Decimal('129.90'))

    def test_canonical_hash_ignores_cosmetic_noise(self):
        a = clean_order(self.order())
        b = clean_order(self.order(canal='  SITE ', valor='129,9', status=' PAGO '))
        self.assertEqual(a['row_hash'], b['row_hash'])

    def test_changed_business_content_has_distinct_hash(self):
        self.assertNotEqual(clean_order(self.order())['row_hash'], clean_order(self.order(valor='130,00'))['row_hash'])

    def test_invalid_date_and_ambiguous_amount_are_rejected(self):
        for changes in [dict(dia='31/02/2026'), dict(valor='1.129,90'), dict(valor='129.90'), dict(valor='-10,00'), dict(valor='1,999'), dict(canal=''), dict(status='desconhecido')]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                clean_order(self.order(**changes))

    def test_required_identifier_and_payment_format(self):
        for invalid in ('', 'P 1', 'P/1', 'x' * 41):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                clean_order(self.order(id=invalid))
        for invalid in ('', 'P 1', 'P/1'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                clean_payment(dict(pedido_id=invalid, resultado='aprovado', atualizado_em='2026-10-20T10:00:00Z'))
        with self.assertRaises(ValueError):
            clean_payment(dict(pedido_id='P1042', resultado='aprovado', atualizado_em='nao-e-data'))

    def test_failure_after_commit_or_ambiguous_commit(self):
        self.assertEqual(failure_status(True, None, 'EVIDENCIA'), 'PUBLICADO_EVIDENCIA_PENDENTE')
        self.assertEqual(failure_status(False, True, 'PUBLICACAO'), 'PUBLICADO_EVIDENCIA_PENDENTE')
        self.assertEqual(failure_status(False, None, 'PUBLICACAO'), 'COMMIT_DESCONHECIDO')
        self.assertEqual(failure_status(False, False, 'PUBLICACAO'), 'FALHOU')

    def test_timestamp_is_normalized_to_utc(self):
        a = clean_payment(dict(pedido_id='P1042', resultado='aprovado', atualizado_em='2026-10-20T07:00:00-03:00'))
        b = clean_payment(dict(pedido_id='P1042', resultado='aprovado', atualizado_em='2026-10-20T10:00:00Z'))
        self.assertEqual(a['atualizado_em'], b['atualizado_em'])
        self.assertEqual(b['atualizado_em'], datetime(2026,10,20,10))

    def test_manifest_pins_content_and_version(self):
        raw = json.dumps(self.manifest()).encode()
        m, h = parse_manifest(raw, 'manifestos/lote_001/ready.json')
        self.assertEqual(h, hashlib.sha256(raw).hexdigest())
        self.assertEqual(m['files'][0]['version_id'], 'v123')

    def test_manifest_cannot_cross_lot_or_use_unversioned_object(self):
        for key, ver in [('entrada/outro/p.csv','v1'),('entrada/lote_001/../p.csv','v1'),('entrada/lote_001/p.csv','null')]:
            m = self.manifest();m['files'][0].update(key=key,version_id=ver)
            with self.subTest(key=key,ver=ver), self.assertRaises(ValueError):
                parse_manifest(json.dumps(m).encode(),'manifestos/lote_001/ready.json')

    def test_delta_accepts_payment_only(self):
        m=self.manifest(lote_id='lote_002')
        m['files'][0].update(kind='pagamentos',key='entrada/lote_002/p.jsonl')
        self.assertEqual(parse_manifest(json.dumps(m).encode(),'manifestos/lote_002/ready.json')[0]['lote_id'],'lote_002')

    def test_manifest_duplicate_files_and_wrong_trigger_key_rejected(self):
        m=self.manifest();m['files']*=2
        with self.assertRaises(ValueError):parse_manifest(json.dumps(m).encode(),'manifestos/lote_001/ready.json')
        with self.assertRaises(ValueError):parse_manifest(json.dumps(self.manifest()).encode(),'manifestos/outro/ready.json')

if __name__ == '__main__':unittest.main()
