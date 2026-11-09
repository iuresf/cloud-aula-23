# Validacao do deploy no Learner Lab

Em 08/11/2026, conta 795636653176, regiao us-east-1:

- O cluster provisionado existente `aula23-dw`, banco `aula23`, foi reutilizado; nao pertence ao state deste projeto e nao e destruido por ele.
- O Terraform criou os componentes do projeto, incluindo bucket versionado, Glue, Step Functions, alarmes e endpoints privados. A LabRole foi utilizada pelos servicos. O cluster e os endpoints geram custos.
- O bootstrap confirmou 11 tabelas, colunas e procedure; antes das cargas, `fato_pedidos`, `etl_lotes` e `dim_canal` tinham zero linhas.
- `lote_001` publicado: 2 pedidos, receita 129,90, 1 lote e 4 registros de proveniencia.
- `lote_002` (estorno) publicado: 2 pedidos, receita 0,00, 2 lotes e 5 registros de proveniencia.
- A primeira tentativa de `lote_001` foi interrompida apos uma falha de conexao observada em diagnostico; o reenvio foi publicado.
- A primeira tentativa de `lote_002` foi interrompida por engano apos seu commit, pois os logs chegaram com atraso. O reenvio foi reconhecido como `JA_PROCESSADO`, sem duplicar a publicacao. A Step Functions registra a tentativa interrompida como falha e a segunda como sucesso. A verificacao do commit no Redshift prevalece sobre o status do workflow.
- Foi observado `ConnectionClosedError` em um diagnostico da Data API; uma nova tentativa na mesma rede e role funcionou. A causa nao foi determinada. Timeouts explicitos foram configurados, mas nao eliminam eventual problema de rede.
- `terraform validate` e dois testes Terraform simulados passaram. A suite local executou 29 testes, com dois testes AWS opcionais ignorados; nao interprete isso como teste integrado de todos os cenarios de falha.
- Os lotes 003 a 007 nao foram enviados nesta validacao. O DataZone/OpenLineage continua apenas como evolucao opcional, sem captura automatica implantada.

Os dois jobs temporarios de diagnostico foram removidos. Scripts de diagnostico e suas versoes permanecem nos buckets, juntamente com as evidencias e originais; nao foram apagados dados da aula.
