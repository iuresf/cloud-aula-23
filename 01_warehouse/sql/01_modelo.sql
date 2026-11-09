CREATE TABLE IF NOT EXISTS public.dim_tempo(sk_tempo INT, data DATE, ano INT, mes INT);
CREATE TABLE IF NOT EXISTS public.dim_canal(sk_canal INT, canal VARCHAR(40));
CREATE TABLE IF NOT EXISTS public.fato_pedidos(
 pedido_id VARCHAR(40), sk_tempo INT, sk_canal INT,
 valor DECIMAL(12,2), receita DECIMAL(12,2), resultado VARCHAR(40)
);
CREATE TABLE IF NOT EXISTS public.dw_pedidos_origem(
 pedido_id VARCHAR(40), data DATE, valor DECIMAL(12,2), status VARCHAR(20),
 canal VARCHAR(40), row_hash VARCHAR(64)
);
CREATE TABLE IF NOT EXISTS public.dw_pagamentos_estado(
 pedido_id VARCHAR(40), resultado VARCHAR(40), atualizado_em TIMESTAMP
);
CREATE TABLE IF NOT EXISTS public.stg_pedidos(
 pedido_id VARCHAR(40), data DATE, valor DECIMAL(12,2), status VARCHAR(20),
 canal VARCHAR(40), row_hash VARCHAR(64), attempt_id VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS public.stg_pagamentos(
 pedido_id VARCHAR(40), resultado VARCHAR(40), atualizado_em TIMESTAMP, attempt_id VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS public.etl_lotes(
 lote_id VARCHAR(64), manifesto_sha256 VARCHAR(64), attempt_id VARCHAR(100), processado_em TIMESTAMP
);
-- Auditoria e proveniencia sao separadas do grao de fato_pedidos.
CREATE TABLE IF NOT EXISTS public.etl_execucoes(
 attempt_id VARCHAR(100), workflow_attempt_id VARCHAR(100), lote_id VARCHAR(64), step_execution_arn VARCHAR(512),
 glue_job_run_id VARCHAR(128), manifesto_key VARCHAR(1024), manifesto_version VARCHAR(1024),
 manifesto_sha256 VARCHAR(64), inicio TIMESTAMP, termino TIMESTAMP, status VARCHAR(64),
 recebidos BIGINT, aceitos BIGINT, rejeitados BIGINT, erro_codigo VARCHAR(100),
 mensagem_erro VARCHAR(1024), evidencia_s3 VARCHAR(2048)
);
CREATE TABLE IF NOT EXISTS public.stg_proveniencia(
 pedido_id VARCHAR(40), tipo VARCHAR(20), arquivo VARCHAR(1024), versao_s3 VARCHAR(1024),
 attempt_id VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS public.dw_proveniencia(
 pedido_id VARCHAR(40), lote_id VARCHAR(64), attempt_id VARCHAR(100), tipo VARCHAR(20),
 arquivo VARCHAR(1024), versao_s3 VARCHAR(1024), ingerido_em TIMESTAMP, operacao VARCHAR(32)
);
