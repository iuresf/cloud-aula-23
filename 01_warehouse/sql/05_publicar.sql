CREATE OR REPLACE PROCEDURE public.publicar_lote(p_lote VARCHAR(64), p_hash VARCHAR(64), p_attempt VARCHAR(100))
LANGUAGE plpgsql
AS $$
DECLARE
 anterior VARCHAR(64);
BEGIN
 -- Procedure atomica: sem COMMIT intermediario. Nao execute em modo NONATOMIC.
 LOCK TABLE public.etl_lotes;
 LOCK TABLE public.dim_canal;
 SELECT manifesto_sha256 INTO anterior FROM public.etl_lotes WHERE lote_id = p_lote;
 IF anterior IS NOT NULL THEN
   IF anterior <> p_hash THEN
     RAISE EXCEPTION 'Lote ja registrado com outro conteudo';
   END IF;
   RETURN;
 END IF;
 -- PK/FK no Redshift sao informativas. A carga valida o modelo explicitamente.
 IF EXISTS (SELECT 1 FROM public.dim_canal GROUP BY sk_canal HAVING COUNT(*)>1)
 OR EXISTS (SELECT 1 FROM public.dim_canal GROUP BY canal HAVING COUNT(*)>1)
 OR EXISTS (SELECT 1 FROM public.dim_tempo GROUP BY sk_tempo HAVING COUNT(*)>1)
 OR EXISTS (SELECT 1 FROM public.dim_tempo GROUP BY data HAVING COUNT(*)>1)
 OR EXISTS (SELECT 1 FROM public.fato_pedidos GROUP BY pedido_id HAVING COUNT(*)>1) THEN
   RAISE EXCEPTION 'Modelo existente contem chaves duplicadas';
 END IF;
 IF EXISTS (
   SELECT 1 FROM public.stg_pedidos s JOIN public.dw_pedidos_origem d USING(pedido_id)
   WHERE s.attempt_id=p_attempt AND s.row_hash<>d.row_hash
 ) THEN RAISE EXCEPTION 'Pedido imutavel recebeu conteudo conflitante'; END IF;
 INSERT INTO public.dw_pedidos_origem
 SELECT s.pedido_id,s.data,s.valor,s.status,s.canal,s.row_hash
 FROM public.stg_pedidos s WHERE s.attempt_id=p_attempt
 AND NOT EXISTS (SELECT 1 FROM public.dw_pedidos_origem d WHERE d.pedido_id=s.pedido_id);
 IF EXISTS (
   SELECT 1 FROM public.stg_pagamentos s LEFT JOIN public.dw_pedidos_origem d USING(pedido_id)
   WHERE s.attempt_id=p_attempt AND d.pedido_id IS NULL
 ) THEN RAISE EXCEPTION 'Pagamento referencia pedido ainda inexistente'; END IF;
 IF EXISTS (
   SELECT 1 FROM public.stg_pagamentos s JOIN public.dw_pagamentos_estado d USING(pedido_id)
   WHERE s.attempt_id=p_attempt AND s.atualizado_em=d.atualizado_em AND s.resultado<>d.resultado
 ) THEN RAISE EXCEPTION 'Pagamento com estados diferentes no mesmo instante'; END IF;
 UPDATE public.dw_pagamentos_estado d SET resultado=s.resultado,atualizado_em=s.atualizado_em
 FROM public.stg_pagamentos s WHERE s.attempt_id=p_attempt AND d.pedido_id=s.pedido_id
 AND s.atualizado_em>d.atualizado_em;
 INSERT INTO public.dw_pagamentos_estado(pedido_id,resultado,atualizado_em)
 SELECT s.pedido_id,s.resultado,s.atualizado_em FROM public.stg_pagamentos s
 WHERE s.attempt_id=p_attempt AND NOT EXISTS(
   SELECT 1 FROM public.dw_pagamentos_estado d WHERE d.pedido_id=s.pedido_id
 );
 IF EXISTS (
   SELECT 1 FROM public.dw_pedidos_origem p LEFT JOIN public.dw_pagamentos_estado g USING(pedido_id)
   WHERE g.pedido_id IS NULL
 ) THEN RAISE EXCEPTION 'Pedido sem estado de pagamento'; END IF;
 INSERT INTO public.dim_tempo(sk_tempo,data,ano,mes)
 SELECT DISTINCT CAST(TO_CHAR(p.data,'YYYYMMDD') AS INT),p.data,
 CAST(EXTRACT(YEAR FROM p.data) AS INT),CAST(EXTRACT(MONTH FROM p.data) AS INT)
 FROM public.dw_pedidos_origem p
 WHERE NOT EXISTS(SELECT 1 FROM public.dim_tempo t WHERE t.data=p.data);
 -- Mantem chaves existentes; aloca somente para canais novos, sob lock.
 INSERT INTO public.dim_canal(sk_canal,canal)
 SELECT CAST(base.max_sk + ROW_NUMBER() OVER(ORDER BY novos.canal) AS INT),novos.canal
 FROM (SELECT DISTINCT p.canal FROM public.dw_pedidos_origem p
       WHERE NOT EXISTS(SELECT 1 FROM public.dim_canal c WHERE c.canal=p.canal)) novos
 CROSS JOIN (SELECT COALESCE(MAX(sk_canal),0) AS max_sk FROM public.dim_canal) base;
 MERGE INTO public.fato_pedidos USING (
   SELECT p.pedido_id,t.sk_tempo,c.sk_canal,p.valor,
   CASE WHEN p.status='pago' AND g.resultado='aprovado' THEN p.valor ELSE CAST(0 AS DECIMAL(12,2)) END AS receita,
   g.resultado
   FROM public.dw_pedidos_origem p JOIN public.dw_pagamentos_estado g USING(pedido_id)
   JOIN public.dim_tempo t ON p.data=t.data JOIN public.dim_canal c ON p.canal=c.canal
   WHERE p.pedido_id IN (
     SELECT pedido_id FROM public.stg_pedidos WHERE attempt_id=p_attempt
     UNION SELECT pedido_id FROM public.stg_pagamentos WHERE attempt_id=p_attempt
   )
 ) s ON fato_pedidos.pedido_id=s.pedido_id
 WHEN MATCHED THEN UPDATE SET sk_tempo=s.sk_tempo,sk_canal=s.sk_canal,valor=s.valor,receita=s.receita,resultado=s.resultado
 WHEN NOT MATCHED THEN INSERT(pedido_id,sk_tempo,sk_canal,valor,receita,resultado)
 VALUES(s.pedido_id,s.sk_tempo,s.sk_canal,s.valor,s.receita,s.resultado);
 IF EXISTS(SELECT 1 FROM public.fato_pedidos f LEFT JOIN public.dim_canal c ON f.sk_canal=c.sk_canal
           LEFT JOIN public.dim_tempo t ON f.sk_tempo=t.sk_tempo WHERE c.sk_canal IS NULL OR t.sk_tempo IS NULL)
 THEN RAISE EXCEPTION 'Fato contem referencia dimensional ausente'; END IF;
 -- Mantem linhagem por registro separada do fato; so e gravada se o commit vencer.
 INSERT INTO public.dw_proveniencia(pedido_id,lote_id,attempt_id,tipo,arquivo,versao_s3,ingerido_em,operacao)
 SELECT DISTINCT pedido_id,p_lote,p_attempt,tipo,arquivo,versao_s3,GETDATE(),
   CASE WHEN tipo='pedidos' THEN 'PEDIDO_RECEBIDO' ELSE 'PAGAMENTO_RECEBIDO' END
 FROM public.stg_proveniencia WHERE attempt_id=p_attempt;
 INSERT INTO public.etl_lotes VALUES(p_lote,p_hash,p_attempt,GETDATE());
 DELETE FROM public.stg_pedidos WHERE attempt_id=p_attempt;
 DELETE FROM public.stg_pagamentos WHERE attempt_id=p_attempt;
 DELETE FROM public.stg_proveniencia WHERE attempt_id=p_attempt;
END;
$$;
