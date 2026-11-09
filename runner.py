"""Aula 23: consulta o Redshift deste projeto, sem carregar ou modificar dados."""
import argparse, subprocess, json, time
from pathlib import Path
import boto3

def query(client, output, sql):
    statement=client.execute_statement(WorkgroupName=output['redshift_workgroup'],Database=output['redshift_database'],Sql=sql)['Id']
    for _ in range(180):
        state=client.describe_statement(Id=statement)
        if state['Status']=='FINISHED':break
        if state['Status'] in ('FAILED','ABORTED'):raise RuntimeError(state.get('Error',state['Status']))
        time.sleep(2)
    else:raise TimeoutError(statement)
    result=client.get_statement_result(Id=statement)
    print(json.dumps({'query':sql,'statement_id':statement,'records':result['Records']},indent=2,default=str))

def main():
    a=argparse.ArgumentParser();a.add_argument('action',choices=['verify']);arg=a.parse_args()
    project=Path(__file__).resolve().parent/'01_warehouse'
    output={k:v['value'] for k,v in json.loads(subprocess.check_output(['terraform','output','-json'],cwd=project)).items()}
    redshift=boto3.Session(region_name=output['region']).client('redshift-data')
    for sql in ['SELECT lote_id,processado_em FROM public.etl_lotes ORDER BY processado_em',
                'SELECT COUNT(*) AS pedidos,SUM(receita) AS receita FROM public.fato_pedidos',
                'SELECT c.canal,COUNT(*) AS pedidos,SUM(f.receita) AS receita FROM public.fato_pedidos f JOIN public.dim_canal c ON f.sk_canal=c.sk_canal GROUP BY c.canal ORDER BY c.canal']:
        query(redshift,output,sql)
if __name__=='__main__':main()
