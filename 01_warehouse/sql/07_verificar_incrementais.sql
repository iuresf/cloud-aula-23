-- Executar no Redshift Query Editor v2 apos publicacao do lote_007.
-- O projeto atual possui FATO_PEDIDOS com 1 linha por pedido e estado atual do pagamento.
-- Este arquivo consulta APENAS as tabelas criadas pelo projeto Terraform.

-- Controle de publicacoes (esperado: lotes 001 a 007).
SELECT lote_id, processado_em
FROM public.etl_lotes
ORDER BY processado_em;

-- Esperado apos lote_007: 9 pedidos e receita reconhecida R$ 342,90.
SELECT COUNT(*) AS quantidade_pedidos, SUM(receita) AS receita_reconhecida
FROM public.fato_pedidos;

-- A dim_canal deve conter marketplace, criado ao publicar P1049.
SELECT sk_canal, canal
FROM public.dim_canal
ORDER BY canal;

-- Receita ATUAL agrupada pela data de realizacao do pedido.
-- O estorno de P1048 afeta receita no dia 24/10 (data original do pedido).
SELECT t.data, c.canal,
       COUNT(*) AS pedidos,
       SUM(f.receita) AS receita_atual
FROM public.fato_pedidos f
JOIN public.dim_tempo t ON f.sk_tempo=t.sk_tempo
JOIN public.dim_canal c ON f.sk_canal=c.sk_canal
GROUP BY t.data, c.canal
ORDER BY t.data, c.canal;

-- Demonstra insercao (P1047, P1048, P1049, P1050) e atualizacao (P1048).
SELECT f.pedido_id, t.data AS data_pedido, c.canal,
       f.valor, f.resultado, f.receita
FROM public.fato_pedidos f
JOIN public.dim_tempo t ON f.sk_tempo=t.sk_tempo
JOIN public.dim_canal c ON f.sk_canal=c.sk_canal
WHERE f.pedido_id IN ('P1047','P1048','P1049','P1050')
ORDER BY f.pedido_id;

-- Situacao atual do pedido cuja receita foi alterada no lote_007.
SELECT pedido_id, resultado, atualizado_em
FROM public.dw_pagamentos_estado
WHERE pedido_id='P1048';
