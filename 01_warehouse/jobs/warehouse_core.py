"""Contrato e validacao pura; usado pelo job Glue e pelos testes sem AWS."""
import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal


class QualityViolation(ValueError):
    """Codigo estavel sem incluir valores sensiveis do registro."""


def checked_id(value):
    value = (value or '').strip()
    if not value:
        raise QualityViolation('ID_OBRIGATORIO')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,40}', value):
        raise QualityViolation('ID_FORMATO_INVALIDO')
    return value


def failure_status(committed, reconciled, phase):
    if committed or reconciled is True:
        return 'PUBLICADO_EVIDENCIA_PENDENTE'
    if phase == 'PUBLICACAO' and reconciled is None:
        return 'COMMIT_DESCONHECIDO'
    return 'FALHOU'


def parse_manifest(raw, key):
    if len(raw) > 65536:
        raise ValueError('Manifesto excede 64 KiB')
    m = json.loads(raw)
    lote = m.get('lote_id', '')
    if m.get('schema_version') != 1 or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', lote):
        raise ValueError('schema_version ou lote_id invalido')
    if key != f'manifestos/{lote}/ready.json':
        raise ValueError('Chave do manifesto nao corresponde ao lote')
    files = m.get('files')
    if not isinstance(files, list) or not 1 <= len(files) <= 20:
        raise ValueError('Informe de 1 a 20 arquivos')
    seen = set()
    for f in files:
        k = f.get('key', '')
        if f.get('kind') not in ('pedidos', 'pagamentos') or not k.startswith(f'entrada/{lote}/') or '..' in k.split('/'):
            raise ValueError('Arquivo fora do lote ou tipo desconhecido')
        if k in seen or not f.get('version_id') or f['version_id'] == 'null' or not re.fullmatch('[0-9a-f]{64}', f.get('sha256', '')):
            raise ValueError('Arquivo duplicado ou sem versao/hash valido')
        seen.add(k)
    return m, hashlib.sha256(raw).hexdigest()


def clean_order(row):
    pedido = checked_id(row.get('id'))
    canal = (row.get('canal') or '').strip().lower()
    status = (row.get('status') or '').strip().lower()
    if not canal or len(canal) > 40:
        raise QualityViolation('CANAL_OBRIGATORIO_OU_INVALIDO')
    if status not in ('pago', 'cancelado'):
        raise QualityViolation('STATUS_INVALIDO')
    try:
        date = datetime.strptime((row.get('dia') or '').strip(), '%d/%m/%Y').date()
    except ValueError as exc:
        raise QualityViolation('DATA_INVALIDA') from exc
    value = (row.get('valor') or '').strip()
    if not re.fullmatch(r'\d{1,10}(,\d{1,2})?', value):
        raise QualityViolation('VALOR_MONETARIO_INVALIDO')
    amount = Decimal(value.replace(',', '.')).quantize(Decimal('.01'))
    canonical = json.dumps([pedido, date.isoformat(), str(amount), status, canal], separators=(',', ':'))
    return dict(pedido_id=pedido, data=date, valor=amount, status=status, canal=canal,
                row_hash=hashlib.sha256(canonical.encode()).hexdigest())


def clean_payment(row):
    pedido = checked_id(row.get('pedido_id'))
    result = (row.get('resultado') or '').strip().lower()
    if result not in ('aprovado', 'sem_cobranca', 'estornado'):
        raise QualityViolation('RESULTADO_PAGAMENTO_INVALIDO')
    value = (row.get('atualizado_em') or '').strip()
    if not re.match(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}', value):
        raise QualityViolation('TIMESTAMP_INVALIDO')
    try:
        timestamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise QualityViolation('TIMESTAMP_INVALIDO') from exc
    if timestamp.tzinfo:
        timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    return dict(pedido_id=pedido, resultado=result, atualizado_em=timestamp)
