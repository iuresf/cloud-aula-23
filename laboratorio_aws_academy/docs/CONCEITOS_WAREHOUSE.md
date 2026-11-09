# Conceitos e fluxo do Data Warehouse

## Objetivo

O laboratório carrega pedidos em um Data Warehouse. O grão de
`dw.fato_pedidos` é **uma linha por pedido publicado**. Dados inválidos,
pendentes ou duplicados ficam em evidências no S3, sem sobrescrever o RAW.

## Fluxo

```text
Arquivos de origem → manifesto versionado no S3 → EventBridge
  → Step Functions → Glue (valida e transforma) → Redshift
                                     └→ rejeitados/evidências no S3
```

1. `warehouse/upload_lote.py` envia as fontes e registra versão S3 e SHA-256.
2. `manifestos/<lote>/ready.json` inicia o processamento.
3. O Glue valida, transforma e guarda as evidências.
4. O Redshift recebe somente pedidos elegíveis.

O manifesto pode ser reexecutado sem duplicar pedidos porque a publicação usa
bloqueio e upsert. Novo conteúdo deve utilizar um novo `lote_id`.

## Camadas e modelo

| Camada | Local | Responsabilidade |
| --- | --- | --- |
| RAW | `Aula23_Materiais/` e `entrada/` | fontes preservadas. |
| Controle | `manifestos/` | lote, versão e checksum. |
| Qualidade | `rejeitados/` e `evidencias/` | motivos e métricas. |
| Analítica | Redshift, schema `dw` | dimensões e fato de pedidos. |

As dimensões são `dw.dim_tempo` e `dw.dim_canal`; a fato é
`dw.fato_pedidos`. Como as fontes atuais não informam canal, o valor publicado
é `NAO_INFORMADO`.

## Política de qualidade

- Campos obrigatórios, datas, valores e status são validados no pipeline.
- Duplicidade conflitante de pedido ou em fonte auxiliar bloqueia a carga.
- Pedidos sem pagamento aprovado ficam como pendentes nas evidências.
- O workflow permite publicação parcial por finalidade didática; a execução
  manual usa o modo conservador por padrão.

O conjunto atual usa Glue Python Shell e Redshift Data API. CDC, DataZone e
OpenLineage são evoluções posteriores: não há alegação de linhagem automática
sem produção e publicação real desses eventos.
