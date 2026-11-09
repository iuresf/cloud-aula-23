-- A carga agora e disparada pelo manifesto S3 e executada no Glue.
-- Nao insira dados diretamente na estrela para simular esta pratica.
-- Para acompanhar a carga inicial apos o workflow:
SELECT * FROM public.etl_lotes WHERE lote_id='lote_001';
SELECT * FROM public.dim_tempo;
SELECT * FROM public.dim_canal;
SELECT * FROM public.fato_pedidos ORDER BY pedido_id;
