
-- ============================================================
-- UFPI / CEAD - Aula 23 (InfoVis)
-- 11_validacao_job2.sql
-- Matricula: 202602 | Lote: lote01 | Regiao: us-east-1
-- ============================================================

-- PRE-REQUISITOS:
-- Job 2 executado com SUCCEEDED e os parametros:
--   --MATRICULA        = 202602
--   --LOTE             = lote01
--   --PUBLICAR_PARCIAL = true
--
-- Athena: fonte de dados AwsDataCatalog
-- Resultados:
-- s3://ufpi-cead-infovis-athena-202602/resultados/
--
-- EXECUTAR UMA INSTRUCAO SQL POR VEZ.
-- CREATE EXTERNAL TABLE nao copia dados do S3.


-- ============================================================
-- PASSO 1 - CRIAR SOMENTE O BANCO FINAL
-- ============================================================

-- stage_db JA EXISTE desde a validacao do Job 1.

CREATE DATABASE IF NOT EXISTS clean_db;


-- ============================================================
-- PASSO 2 - REGISTRAR A TABELA FINAL
-- ============================================================

-- Esperado: P101, P102 e P108.
-- Total: 3 vendas, receita de R$ 420,00.

CREATE EXTERNAL TABLE IF NOT EXISTS clean_db.vendas_integradas (
    pedido_id           STRING,
    cliente_id          STRING,
    data_venda          DATE,
    cidade              STRING,
    receita             DECIMAL(12,2),
    transacao_id        STRING,
    arquivo_origem      STRING,
    lote                STRING,
    cobertura           DOUBLE,
    publicacao_parcial  BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-final-202602/lote01/vendas_integradas/';


-- ============================================================
-- PASSO 3 - REGISTRAR QUARANTINE DO JOB 2
-- ============================================================

-- Tabela nova dentro de stage_db, que ja existe.
-- Os arquivos ficam no bucket QUARANTINE.
-- Schema minimo para consultar os campos necessarios.
--
-- P105: cliente_orfao.
-- P106: valor_divergente.

CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job2_pendentes (
    pedido_id           STRING,
    cliente_id          STRING,
    status              STRING,
    valor               DECIMAL(12,2),
    cliente_loja        STRING,
    valor_loja          DECIMAL(12,2),
    valor_pago          DECIMAL(12,2),
    status_pagamento    STRING,
    cidade              STRING,
    motivo              STRING,
    lote                STRING
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-quarantine-202602/lote01/job2/';


-- ============================================================
-- PASSO 4 - REGISTRAR AUDIT DO JOB 2
-- ============================================================

-- Tabela nova dentro de stage_db, que ja existe.
-- Os arquivos ficam no bucket AUDIT.
-- Schema minimo para consultar os campos necessarios.
-- Registra as decisoes dos 6 pedidos.

CREATE EXTERNAL TABLE IF NOT EXISTS stage_db.chk_job2_auditoria (
    linha_id            STRING,
    pedido_id           STRING,
    cliente_id          STRING,
    status              STRING,
    valor               DECIMAL(12,2),
    cliente_loja        STRING,
    valor_loja          DECIMAL(12,2),
    valor_pago          DECIMAL(12,2),
    status_pagamento    STRING,
    cidade              STRING,
    motivo              STRING,
    lote                STRING
)
STORED AS PARQUET
LOCATION 's3://ufpi-cead-infovis-audit-202602/lote01/integracao/';


-- ============================================================
-- PASSO 5 - VERIFICAR AS TABELAS NO CATALOGO
-- ============================================================

-- Executar cada SHOW separadamente.

SHOW TABLES IN clean_db;

SHOW TABLES IN stage_db;


-- ============================================================
-- PASSO 6 - CONTAR OS REGISTROS
-- ============================================================

-- Esperado:
-- FINAL = 3
-- QUARANTINE = 2
-- AUDIT = 6

SELECT
    'FINAL' AS destino,
    COUNT(*) AS linhas
FROM clean_db.vendas_integradas

UNION ALL

SELECT
    'QUARANTINE',
    COUNT(*)
FROM stage_db.chk_job2_pendentes

UNION ALL

SELECT
    'AUDIT',
    COUNT(*)
FROM stage_db.chk_job2_auditoria;


-- ============================================================
-- PASSO 7 - CONSULTAR OS PEDIDOS PENDENTES
-- ============================================================

-- P105: cliente C99 ausente do cadastro.
--       ERP 80, loja 80, pagamento 80.
--
-- P106: divergencia de valores.
--       ERP 90, loja 90, pagamento 95.

SELECT
    pedido_id,
    cliente_id,
    motivo,
    valor,
    valor_loja,
    valor_pago,
    cidade
FROM stage_db.chk_job2_pendentes
ORDER BY pedido_id;


-- ============================================================
-- PASSO 8 - CONSULTAR TODAS AS DECISOES
-- ============================================================

-- Esperado:
-- P101: publicavel
-- P102: publicavel
-- P103: fora_indicador
-- P105: cliente_orfao
-- P106: valor_divergente
-- P108: publicavel

SELECT
    pedido_id,
    status,
    status_pagamento,
    motivo,
    valor,
    valor_loja,
    valor_pago
FROM stage_db.chk_job2_auditoria
ORDER BY pedido_id;


-- ============================================================
-- PASSO 9 - AGRUPAR OS PEDIDOS POR MOTIVO
-- ============================================================

-- Esperado:
-- publicavel = 3
-- cliente_orfao = 1
-- valor_divergente = 1
-- fora_indicador = 1

SELECT
    motivo,
    COUNT(*) AS pedidos
FROM stage_db.chk_job2_auditoria
GROUP BY motivo
ORDER BY motivo;


-- ============================================================
-- PASSO 10 - VALIDAR RECEITA E COBERTURA
-- ============================================================

-- Esperado:
-- pedidos = 3
-- chaves_unicas = 3
-- receita_total = 420.00
-- cobertura = 0.60
-- parcial_autorizada = 1

SELECT
    COUNT(*) AS pedidos,
    COUNT(DISTINCT pedido_id) AS chaves_unicas,
    SUM(receita) AS receita_total,
    MIN(cobertura) AS cobertura,
    MAX(
        CASE
            WHEN publicacao_parcial THEN 1
            ELSE 0
        END
    ) AS parcial_autorizada
FROM clean_db.vendas_integradas;


-- ============================================================
-- PASSO 11 - EXIBIR AS VENDAS CONFIRMADAS
-- ============================================================

-- Esperado:
-- P101 | Picos    | 120.00
-- P102 | Teresina | 200.00
-- P108 | Picos    | 100.00

SELECT
    pedido_id,
    cidade,
    data_venda,
    receita,
    publicacao_parcial
FROM clean_db.vendas_integradas
ORDER BY pedido_id;


-- ============================================================
-- PASSO 12 - RECEITA POR CIDADE
-- ============================================================

-- Esperado:
-- Picos: 2 pedidos, R$ 220,00
-- Teresina: 1 pedido, R$ 200,00

SELECT
    cidade,
    COUNT(*) AS pedidos,
    SUM(receita) AS receita_total
FROM clean_db.vendas_integradas
GROUP BY cidade
ORDER BY receita_total DESC;


-- ============================================================
-- PASSO 13 - CONFERIR O FECHAMENTO DO JOB 2
-- ============================================================

-- Esperado:
-- 6 pedidos STAGE:
-- 3 publicaveis
-- 2 pendentes
-- 1 cancelado

SELECT
    COUNT(*) AS pedidos_stage_auditados,

    SUM(
        CASE
            WHEN motivo = 'publicavel' THEN 1
            ELSE 0
        END
    ) AS publicaveis,

    SUM(
        CASE
            WHEN motivo NOT IN (
                'publicavel',
                'fora_indicador'
            ) THEN 1
            ELSE 0
        END
    ) AS pendentes,

    SUM(
        CASE
            WHEN motivo = 'fora_indicador' THEN 1
            ELSE 0
        END
    ) AS fora_indicador

FROM stage_db.chk_job2_auditoria;


-- ============================================================
-- OBSERVACOES
-- ============================================================

-- Job 1:
-- 10 RAW = 6 STAGE + 3 invalidos + 1 duplicata.
--
-- Job 2:
-- 6 STAGE = 3 FINAL + 2 pendentes + 1 cancelado.
--
-- AUDIT Job 2 registra as decisoes dos 6 pedidos.
-- Nao somar AUDIT como se fossem vendas adicionais.
--
-- stage_db nao esta sendo recriado.
-- Apenas recebe duas tabelas novas de validacao.
--
-- O bucket fisico e definido pela clausula LOCATION.
--
-- Nao reexecute o Job 2 apenas para consultar no Athena.
--
-- Se uma tabela ja existir, IF NOT EXISTS nao
-- atualiza seu LOCATION ou seu esquema.
