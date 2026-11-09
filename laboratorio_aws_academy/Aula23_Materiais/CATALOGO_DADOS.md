# Catálogo dos dados didáticos

Os arquivos desta pasta são sintéticos; não devem ser substituídos por dados
pessoais ou de produção.

| Arquivo | Papel | Uso principal |
| --- | --- | --- |
| `erp.csv` | pedidos do ERP | pedido, cliente, data, valor e status. |
| `clientes.csv` | cadastro auxiliar | cliente e localidade. |
| `loja.jsonl` | pedidos da loja | origem adicional de pedidos. |
| `pagamentos.csv` | situação de pagamento | pedido e aprovação. |

## Resultado esperado do lote original

| Medida | Resultado |
| --- | ---: |
| Registros RAW | 10 |
| Registros de estágio válidos | 6 |
| Pedidos publicados | 3 (`P101`, `P102`, `P108`) |
| Rejeitados | 5 |
| Inválidos | 3 |
| Duplicatas tratadas | 1 |
| Pagamentos pendentes | 2 |
| Receita publicada | R$ 420,00 |
| Cobertura de aprovados válidos | 60% |

`LEIA-ME.md` e os scripts numerados são o roteiro de integração Glue/Athena.
O laboratório do Warehouse reutiliza esses arquivos em `warehouse/` e usa a
infraestrutura em `terraform/`.
