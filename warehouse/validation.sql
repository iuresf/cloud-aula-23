-- Cada consulta deve ser executada via Data API ou Query Editor v2.
select count(*) pedidos, count(distinct pedido_id) chaves,
       sum(receita) receita from dw.fato_pedidos;

select t.data_calendario, c.canal_cd, count(*) pedidos, sum(f.receita) receita
from dw.fato_pedidos f
join dw.dim_tempo t on t.tempo_sk = f.tempo_sk
join dw.dim_canal c on c.canal_sk = f.canal_sk
group by 1,2 order by 1,2;

-- Deve retornar zero linhas: Redshift nao impoe PK/UK/FK automaticamente.
select pedido_id from dw.fato_pedidos group by 1 having count(*) <> 1;

select f.pedido_id from dw.fato_pedidos f
left join dw.dim_tempo t on t.tempo_sk=f.tempo_sk
left join dw.dim_canal c on c.canal_sk=f.canal_sk
where t.tempo_sk is null or c.canal_sk is null;
