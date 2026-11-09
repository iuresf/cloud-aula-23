SELECT * FROM public.etl_lotes ORDER BY processado_em;
SELECT attempt_id,workflow_attempt_id,lote_id,step_execution_arn,glue_job_run_id,manifesto_version,
       inicio,termino,status,recebidos,aceitos,rejeitados,erro_codigo,evidencia_s3
FROM public.etl_execucoes ORDER BY inicio DESC;
-- Substitua P1042 pelo pedido investigado. Recebido nao implica que pagamento
-- antigo substituiu o estado: consulte tambem dw_pagamentos_estado.
SELECT pedido_id,lote_id,attempt_id,tipo,arquivo,versao_s3,ingerido_em,operacao
FROM public.dw_proveniencia WHERE pedido_id='P1042' ORDER BY ingerido_em,tipo,arquivo;
SELECT pedido_id,COUNT(*) FROM public.fato_pedidos GROUP BY pedido_id HAVING COUNT(*)>1;
SELECT sk_canal,COUNT(*) FROM public.dim_canal GROUP BY sk_canal HAVING COUNT(*)>1;
SELECT f.pedido_id FROM public.fato_pedidos f
LEFT JOIN public.dim_canal c ON f.sk_canal=c.sk_canal
LEFT JOIN public.dim_tempo t ON f.sk_tempo=t.sk_tempo
WHERE c.sk_canal IS NULL OR t.sk_tempo IS NULL;
SELECT pedido_id,resultado,atualizado_em FROM public.dw_pagamentos_estado ORDER BY pedido_id;
