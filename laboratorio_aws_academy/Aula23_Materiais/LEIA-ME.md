# Aula 23 — laboratorio da Aula 22

Dados inteiramente sinteticos. Uma venda por pedido no ERP; uma ocorrencia por pedido na loja e no gateway; um cliente por ID. Nao e um modelo de pagamentos com parcelas nem um conector real de API. O JSONL simula uma exportacao da loja.

1. No AWS Academy, confirmar servicos, regiao e role existente que permita Glue/S3/Catalog/CloudWatch e iam:PassRole. Nao criar chaves nem roles. Glue nao e garantido por este material: conferir regras do curso.
2. Criar bucket privado; enviar erp.csv para raw/erp/lote01/, clientes.csv para raw/clientes/lote01/, loja.jsonl para raw/loja/lote01/, pagamentos.csv para raw/pagamentos/lote01/. Nao enviar o ZIP inteiro para RAW. Desativar uso de dados reais.
3. No Athena, configurar destino de resultados autorizado em athena-results/ e criar raw_db, stage_db, clean_db. Criar crawlers por fonte apontando ao prefixo exato e para raw_db; execucao sob demanda. Revisar nomes, delimitador ;, cabecalho e esquema. Corrigir classificadores CSV se necessario. RAW deve preservar valores como texto; o script le direto do S3 com esquema explicito, sem depender da inferencia.
4. Glue Studio > ETL jobs > Script editor > Spark/Python. Colar 01_job_limpeza.py. Role existente autorizada, versao Spark suportada pelo lab, workers minimos permitidos, retries 0, bookmarks desabilitados, timeout de acordo com limite do curso. Parametros --BUCKET (somente nome) e --LOTE lote01. Diretorios de script/temporarios em scripts/ e tmp/. Salvar e executar. So prosseguir com SUCCEEDED e os quatro prefixos STAGE.
5. Rodar crawlers nos quatro prefixos clean/stage/lote01/<fonte>/ e destino stage_db, se permitidos. Spark le os Parquet diretamente; catalogacao nao transforma. RAW 10 = ERP STAGE 6 + 3 invalidos + 1 duplicata. Fontes auxiliares: 4 clientes, 8 pedidos loja e 8 pagamentos. Falha em fonte auxiliar bloqueia o job: corrigir RAW em nova versao; nao descartar em silencio.
6. Criar o segundo job com 02_job_integracao.py. Mesmas configuracoes e parametros; adicionar --PUBLICAR_PARCIAL true APENAS para a politica didatica desta pratica. O padrao de decisao real deve ser aprovado pelo responsavel do indicador. Com false, o job registra auditoria/quarentena e bloqueia FINAL. Nao executar antes do Job 1 terminar.
7. Resultado esperado: 6 pedidos ERP STAGE = 3 publicados + 1 cancelado fora do indicador + 2 pendentes em quarentena. P105 nao tem cliente; P106 tem R$90 no ERP e R$95 no gateway. FINAL=R$420: Picos R$220; Teresina R$200. Cobertura dos aprovados tecnicamente validos=3/5=60%; os 3 invalidos RAW sao divulgados separadamente. Nao interpretar 60% como qualidade global nem receita total do negocio.
8. Crawler FINAL no prefixo clean/final/lote01/vendas_integradas/ -> clean_db; confirmar esquema e nome. Se nome diferente, ajustar as consultas. Alternativa: DDL de 03_consultas.sql. Nunca cadastrar audit/, quarantine/ ou athena-results/ como tabela final.
9. Executar consultas uma por vez. Conferir contagem=chaves=3, receita=420, cobertura=0.6. Exportar receita por cidade e executar 05_visualizar.py (pandas e matplotlib), ou criar o grafico no Metabase ja configurado pela turma. Nao exigir QuickSight.
10. Reprocessar: manter RAW antigo; subir dados corrigidos em raw/<fonte>/lote02/ e usar --LOTE lote02 nos dois jobs. Atualizar LOCATION da tabela somente apos aprovacao do lote. errorifexists evita sobrescrever saida; uma execucao parcial exige diagnostico e novo lote. Esta pratica nao implementa transacao atomica de publicacao, orquestracao automatica, regras DQDL ou SNS.

## Se Glue estiver bloqueado
Nao contornar IAM. Executar 06_alternativa_athena.sql: implementa o mesmo contrato em SQL/CTAS no Athena (ELT), com verificacoes e portao manuais. O arquivo deve ser executado uma instrucao por vez, interrompendo em qualquer teste que retorne problemas. Como verificacao local dos dados, executar python 04_referencia_local.py; isso NAO comprova execucao Glue/Athena. A trilha completa acima depende de Glue autorizado. Registre o bloqueio e conclua as evidencias permitidas.

## Validacao do material
Foi feita conferencia local independente das contagens e dos totais e verificacao sintatica dos scripts. Nao houve execucao na conta AWS do aluno. Os asserts de 10/6/3/420 so se aplicam ao lote01 inicial, nao a lotes corrigidos nem a regras universais. Nao combinar varios lotes nos mesmos prefixos de leitura.

## Documentacao
https://docs.aws.amazon.com/glue/latest/dg/add-job.html
https://docs.aws.amazon.com/glue/latest/dg/edit-nodes-script.html
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html
https://docs.aws.amazon.com/athena/latest/ug/create-table.html
https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html
https://spark.apache.org/docs/3.5.6/api/sql/
Conceitos: Aula 22; Munzner (2014); Wilke (2019); ISO/IEC 25012 e 25024 (metas e destinos definidos pela politica didatica, nao pelas normas).


## Revisão WWH da aula
Leia 09_contexto_perguntas_wwh.md e execute 07_perguntas_wwh.sql após a trilha escolhida. O bloco A atende Glue; o bloco B atende ELT; execute apenas um. As seis perguntas e o gabarito do lote estão em 08_resultados_wwh.json.
