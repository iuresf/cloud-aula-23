-- Envie o pagamento estornado e o manifesto lote_002 ao S3.
-- O Glue preserva a versao mais recente e a procedure recalcula a receita.
SELECT * FROM public.etl_lotes WHERE lote_id='lote_002';
SELECT pedido_id,resultado,atualizado_em FROM public.dw_pagamentos_estado;
SELECT pedido_id,receita,resultado FROM public.fato_pedidos ORDER BY pedido_id;
