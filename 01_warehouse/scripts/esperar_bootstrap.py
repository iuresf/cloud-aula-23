"""Executado pelo Terraform: inicia o bootstrap e aguarda o resultado do Glue."""
import json
import os
import subprocess
import sys
import time


def cli(*args):
    result = subprocess.run(['aws', *args, '--region', os.environ['A23_REGION'], '--output', 'json'],
                            check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def main():
    job = os.environ['A23_BOOTSTRAP_JOB']
    response = cli('glue', 'start-job-run', '--job-name', job)
    run = response['JobRunId']
    print('Inicializacao do modelo iniciada: ' + run, flush=True)
    deadline = time.monotonic() + int(os.environ.get('A23_BOOTSTRAP_TIMEOUT', '1800'))
    while time.monotonic() < deadline:
        status = cli('glue', 'get-job-run', '--job-name', job, '--run-id', run)['JobRun']
        state = status['JobRunState']
        if state == 'SUCCEEDED':
            print('Esquema e procedure verificados. JobRun: ' + run, flush=True)
            return
        if state in ('FAILED', 'STOPPED', 'TIMEOUT', 'ERROR', 'EXPIRED'):
            raise RuntimeError('Inicializacao falhou: ' + state + '; JobRun: ' + run)
        time.sleep(10)
    # Nao cancela o job automaticamente: o operador pode acompanhar o ID nos logs.
    raise TimeoutError('Timeout aguardando Glue; verifique o JobRun: ' + run)


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print('AWS CLI falhou ao iniciar/consultar o bootstrap: ' + (exc.stderr or str(exc)).strip(), file=sys.stderr)
        sys.exit(1)
    except (RuntimeError, TimeoutError, KeyError) as exc:
        print('Erro ao inicializar o modelo: ' + str(exc), file=sys.stderr)
        sys.exit(1)
