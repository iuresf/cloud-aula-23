-- Aula 23: modelo minimo exigido pelo slide a23_ext_marts_facts.
-- Execute no Amazon Redshift (ou pelo Data API) em uma transacao. Este arquivo
-- nao cria cluster/workgroup nem credenciais; AWS Academy pode restringi-los.

begin;

create schema if not exists dw;

create table if not exists dw.dim_tempo (
  tempo_sk       integer identity(1, 1) primary key,
  data_calendario date not null unique,
  ano            smallint not null,
  mes            smallint not null,
  dia            smallint not null,
  trimestre      smallint not null
);

create table if not exists dw.dim_canal (
  canal_sk integer identity(1, 1) primary key,
  canal_cd varchar(50) not null unique,
  canal_nm varchar(100) not null
);

-- Grão: exatamente uma linha para cada pedido aprovado publicado.
create table if not exists dw.fato_pedidos (
  pedido_id  varchar(64) not null,
  tempo_sk   integer not null references dw.dim_tempo(tempo_sk),
  canal_sk   integer not null references dw.dim_canal(canal_sk),
  cliente_id varchar(64) not null,
  receita    decimal(12, 2) not null,
  lote_id    varchar(64) not null,
  carregado_em timestamp not null default getdate(),
  primary key (pedido_id)
);

-- No Redshift, PK/UK/FK sao informativas. O ETL valida chaves e serializa
-- publicacoes com LOCK antes do upsert; veja validation.sql para auditoria.

-- Dimensao conformada: o mesmo tempo pode relacionar vendas e pagamentos.
insert into dw.dim_canal (canal_cd, canal_nm)
select 'NAO_INFORMADO', 'Nao informado'
where not exists (
  select 1 from dw.dim_canal where canal_cd = 'NAO_INFORMADO'
);

commit;
