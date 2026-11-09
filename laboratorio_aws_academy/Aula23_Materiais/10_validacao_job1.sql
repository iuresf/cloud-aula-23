
-- AULA 23 / INFO VIS — CHECKPOINT PRATICO DO JOB 1
-- Matricula de exemplo: 202602 | Lote: lote01
-- Regiao AWS: mesma do Glue/S3
-- Execute UMA instrucao por vez no Amazon Athena Query editor.
-- Fonte de dados: AwsDataCatalog.
-- Configure antes a saida das consultas no bucket:
-- s3://ufpi-cead-infovis-athena-202602/resultados/
--
-- Estas tabelas EXTERNAS registram metadados;
-- nao copiam os arquivos Parquet.
-- Se sua matricula for diferente, substitua 202602
-- em todos os S3 LOCATION.


-- PASSO 1 — DATABASE DE VALIDACAO (criar uma vez)

CREATE DATABASE IF NOT EXISTS stage_db;


-- PASSO 2 — STAGE:
-- Seis ocorrencias ERP tratadas e sem copias

CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job1_erp (
    linha_id          string,
    pedido_id         string,
    cliente_id        string,
    data_venda        date,
    valor             decimal(12,2),
    status            string,
    arquivo_origem    string,
    fonte             string,
    valor_original    string,
    data_original     string,
    status_original   string,
    lote              string
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-stage-202602/lote01/erp/';


-- PASSO 3 — QUARANTINE:
-- Ocorrencias rejeitadas por regra de qualidade

CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job1_invalidos (
    linha_id          string,
    pedido_id         string,
    cliente_id        string,
    data_venda        date,
    valor             decimal(12,2),
    status            string,
    arquivo_origem    string,
    fonte             string,
    valor_original    string,
    data_original     string,
    status_original   string,
    motivo            string,
    lote              string
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-quarantine-202602/lote01/job1_invalidos/';


-- PASSO 4 — AUDIT:
-- Uma copia identica de P101, nao uma nova venda

CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job1_duplicatas (
    pedido_id         string,
    linha_id          string,
    cliente_id        string,
    data_venda        date,
    valor             decimal(12,2),
    status            string,
    arquivo_origem    string,
    fonte             string,
    valor_original    string,
    data_original     string,
    status_original   string,
    versoes           bigint,
    ordem             integer,
    motivo            string,
    lote              string
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-audit-202602/lote01/duplicatas/';


-- PASSO 5 — CHECAR CONTAGENS
-- Resultado esperado: 6, 3, 1

SELECT 'STAGE_ERP' AS destino, COUNT(*) AS ocorrencias
FROM stage_db.chk_job1_erp
UNION ALL
SELECT 'QUARANTINE_INVALIDOS', COUNT(*)
FROM stage_db.chk_job1_invalidos
UNION ALL
SELECT 'AUDIT_DUPLICATAS', COUNT(*)
FROM stage_db.chk_job1_duplicatas;


-- PASSO 6 — VER OS ERROS QUE FORAM ISOLADOS

SELECT linha_id, pedido_id, motivo, valor_original, data_original
FROM stage_db.chk_job1_invalidos
ORDER BY linha_id;


-- PASSO 7 — COMPARAR P101 NO STAGE E NA AUDITORIA

SELECT 'STAGE' AS destino, linha_id, pedido_id, cliente_id, valor, status
FROM stage_db.chk_job1_erp
WHERE pedido_id = 'P101'
UNION ALL
SELECT 'AUDIT' AS destino, linha_id, pedido_id, cliente_id, valor, status
FROM stage_db.chk_job1_duplicatas
WHERE pedido_id = 'P101';


-- PASSO 8 — VALIDAR A CONSERVACAO DAS OCORRENCIAS DO ERP

SELECT (SELECT COUNT(*) FROM stage_db.chk_job1_erp)
     + (SELECT COUNT(*) FROM stage_db.chk_job1_invalidos)
     + (SELECT COUNT(*) FROM stage_db.chk_job1_duplicatas)
       AS total_classificado;

-- Esperado: 10, igual ao ERP RAW e a contagem do log Glue.


-- PASSO 9 — OPCIONAL: OUTRAS FONTES STAGE
-- Catalogo minimo para contagens

CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job1_clientes (
    cliente_id       string,
    cidade           string,
    telefone         string,
    cidade_original  string,
    lote             string
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-stage-202602/lote01/clientes/';


CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job1_loja (
    pedido_id       string,
    cliente_loja    string,
    valor_loja      decimal(12,2),
    moeda_loja      string,
    lote            string
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-stage-202602/lote01/loja/';


CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job1_pagamentos (
    transacao_id     string,
    pedido_id        string,
    valor_pago       decimal(12,2),
    moeda            string,
    status_pagamento string,
    lote             string
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-stage-202602/lote01/pagamentos/';


SELECT 'clientes' AS fonte, COUNT(*) AS registros
FROM stage_db.chk_job1_clientes
UNION ALL
SELECT 'loja', COUNT(*)
FROM stage_db.chk_job1_loja
UNION ALL
SELECT 'pagamentos', COUNT(*)
FROM stage_db.chk_job1_pagamentos;

-- Esperado: clientes = 4; loja = 8; pagamentos = 8.


-- LEMBRETES

-- 1) COUNT(*) consulta linhas internas ao Parquet.
--    Contar objetos no S3 nao serve.

-- 2) Esses DDLs criam metadados externos no Glue Data Catalog;
--    nada se move no S3.

-- 3) Se ja existir uma tabela com este nome,
--    IF NOT EXISTS nao corrige seu LOCATION;
--    confirme em Glue > Tables > Storage information
--    antes de consultar.

-- 4) O Job 1 NAO fez conciliacao de receita:
--    somente o Job 2 publica FINAL.

-- 5) Nao rode o Job 1 de novo:
--    errorifexists bloqueia sobrescrita de saidas.
