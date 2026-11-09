#!/bin/bash

MATRICULA="202602"
REGIAO="us-east-1"

aws s3 mb "s3://ufpi-cead-infovis-raw-$MATRICULA" --region "$REGIAO"
aws s3 mb "s3://ufpi-cead-infovis-stage-$MATRICULA" --region "$REGIAO"
aws s3 mb "s3://ufpi-cead-infovis-final-$MATRICULA" --region "$REGIAO"
aws s3 mb "s3://ufpi-cead-infovis-quarantine-$MATRICULA" --region "$REGIAO"
aws s3 mb "s3://ufpi-cead-infovis-audit-$MATRICULA" --region "$REGIAO"
aws s3 mb "s3://ufpi-cead-infovis-athena-$MATRICULA" --region "$REGIAO"
aws s3 mb "s3://ufpi-cead-infovis-apoio-$MATRICULA" --region "$REGIAO"