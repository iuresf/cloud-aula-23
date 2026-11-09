import unittest
from pathlib import Path
from pipeline import transform, load_sql


class PipelineTests(unittest.TestCase):
    def setUp(self):
        base = Path(__file__).resolve().parents[1] / "Aula23_Materiais"
        self.sources = {name: (base / name).read_text() for name in ("erp.csv", "clientes.csv", "loja.jsonl", "pagamentos.csv")}

    def test_original_lote(self):
        stage, published, rejected, counts = transform(self.sources)
        self.assertEqual((len(stage), len(published), len(rejected)), (6, 3, 5))
        self.assertEqual({r["pedido_id"] for r in published}, {"P101", "P102", "P108"})
        self.assertEqual(counts["receita"], "420.00")
        self.assertEqual(counts["cobertura_aprovados_validos"], 0.6)
        self.assertEqual((counts["raw"], counts["invalidos"], counts["duplicatas"], counts["pendentes"]), (10, 3, 1, 2))

    def test_duplicate_auxiliary_blocks(self):
        self.sources["clientes.csv"] += "C1;Picos;\n"
        with self.assertRaises(ValueError):
            transform(self.sources)

    def test_conflicting_duplicate_blocks(self):
        self.sources["erp.csv"] += "011;P101;C1;2026-10-01;121,00;aprovado\n"
        with self.assertRaises(ValueError):
            transform(self.sources)

    def test_sql_escapes_literals(self):
        sql = load_sql([], "lote'01")
        self.assertIn("'lote''01'", sql[-1])
        self.assertEqual(sql[0], "lock table dw.fato_pedidos")


if __name__ == "__main__":
    unittest.main()
