SELECT t.data,c.canal,SUM(f.receita) AS receita
FROM fato_pedidos f
JOIN dim_tempo t ON f.sk_tempo=t.sk_tempo
JOIN dim_canal c ON f.sk_canal=c.sk_canal
GROUP BY t.data,c.canal
ORDER BY t.data,c.canal;
