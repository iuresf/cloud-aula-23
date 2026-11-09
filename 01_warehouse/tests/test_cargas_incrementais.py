"""Valida offline os arquivos e a evolução didática dos lotes 001–007.
Não simula a engine SQL, a IAM, o Spark nem a execução AWS.
"""
import csv
import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE / '01_warehouse' / 'jobs'))
from warehouse_core import clean_order, clean_payment, parse_manifest

LOTES = [
    ('lote_001', 'dados/inicial/pedidos/lote_001.csv', 'dados/inicial/pagamentos/lote_001.jsonl', 2, '129.90'),
    ('lote_002', None, 'dados/delta/pagamentos/lote_002.jsonl', 2, '0.00'),
    ('lote_003', 'dados/dia3/pedidos.csv', 'dados/dia3/pagamentos.jsonl', 4, '75.00'),
    ('lote_004', 'dados/dia4/pedidos.csv', 'dados/dia4/pagamentos.jsonl', 5, '134.90'),
    ('lote_005', 'dados/dia5/pedidos.csv', 'dados/dia5/pagamentos.jsonl', 7, '240.30'),
    ('lote_006', 'dados/dia6/pedidos.csv', 'dados/dia6/pagamentos.jsonl', 9, '362.80'),
    ('lote_007', None, 'dados/dia7/pagamentos.jsonl', 9, '342.90'),
]


def read_orders(rel):
    if not rel:
        return []
    with (BASE / rel).open(encoding='utf-8', newline='') as f:
        return [clean_order(line) for line in csv.DictReader(f, delimiter=';')]


def read_payments(rel):
    with (BASE / rel).open(encoding='utf-8') as f:
        return [clean_payment(json.loads(line)) for line in f if line.strip()]


def calculate(orders, payments):
    total = Decimal('0.00')
    for id, o in orders.items():
        p = payments[id]
        if o['status'] == 'pago' and p['resultado'] == 'aprovado':
            total += o['valor']
    return total


def simulate():
    orders, payments = {}, {}
    snapshots = []
    for lote, input_orders, input_payments, count_expected, amount_expected in LOTES:
        # Semântica compatível com regras de imutabilidade e versão da procedure.
        for o in read_orders(input_orders):
            key = o['pedido_id']
            if key in orders:
                assert orders[key]['row_hash'] == o['row_hash'], f'Pedido {key} mudou no {lote}'
            else:
                orders[key] = o
        for p in read_payments(input_payments):
            key = p['pedido_id']
            assert key in orders, f'Pagamento sem pedido: {key}'
            if key in payments:
                old = payments[key]
                assert p['atualizado_em'] != old['atualizado_em'] or p['resultado'] == old['resultado'], 'Conflito de versao'
                if p['atualizado_em'] > old['atualizado_em']:
                    payments[key] = p
            else:
                payments[key] = p
        assert set(orders) == set(payments), 'Pedido sem pagamento'
        total = calculate(orders, payments)
        assert len(orders) == count_expected, f'{lote} tem {len(orders)} pedidos; esperado {count_expected}'
        assert total == Decimal(amount_expected), f'{lote} soma {total}; esperado {amount_expected}'
        snapshots.append((lote, len(orders), str(total)))
    return orders, payments, snapshots


class IncrementalFixtureTest(unittest.TestCase):
    def test_incremental_inserts_updates(self):
        orders, payments, snapshots = simulate()
        self.assertEqual([x[2] for x in snapshots], ['129.90','0.00','75.00','134.90','240.30','362.80','342.90'])
        self.assertEqual(orders['P1049']['canal'], 'marketplace')
        self.assertEqual(orders['P1048']['data'].isoformat(), '2026-10-24')
        self.assertEqual(payments['P1048']['resultado'], 'estornado')
        self.assertEqual(orders['P1048']['valor'], Decimal('19.90'))

    def test_lote_007_delta_only(self):
        self.assertIsNone(LOTES[-1][1])
        self.assertEqual(len(read_payments('dados/dia7/pagamentos.jsonl')), 1)

    def test_unique_new_order_ids(self):
        seen = set()
        for lote, orderfile, _, _, _ in LOTES:
            for record in read_orders(orderfile):
                # A repetição exata de P1042 no lote inicial é intencional.
                if record['pedido_id'] in seen:
                    self.assertEqual(record['pedido_id'], 'P1042')
                seen.add(record['pedido_id'])
        self.assertEqual(len(seen), 9)

    def test_manifest_contracts_for_each_new_lot(self):
        for number in [5,6,7]:
            lote = f'lote_{number:03d}'
            inputs = [x for x in LOTES if x[0] == lote][0]
            files=[]
            for kind, path in [('pedidos',inputs[1]),('pagamentos',inputs[2])]:
                if path:
                    files.append({'kind':kind,'key':f'entrada/{lote}/{kind}.csv' if kind=='pedidos' else f'entrada/{lote}/{kind}.jsonl', 'version_id':'testversion123', 'sha256':'a'*64})
            m = {'schema_version':1,'lote_id':lote,'files':files}
            self.assertEqual(parse_manifest(json.dumps(m).encode(), f'manifestos/{lote}/ready.json')[0]['lote_id'], lote)


if __name__ == '__main__':
    unittest.main()
