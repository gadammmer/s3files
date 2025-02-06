#!/bin/bash

## Script de envoltura para move_flux_snowflake.sh

inicio=$(date -d 'yesterday' +'%Y-%m-%dT00:00:00.%N%z')
fin=$(date -d 'yesterday' +'%Y-%m-%dT23:59:59.%N%z')

/opt/GEAOSP_PY_POC_TURISMO/scripts/move_flux_snowflake2.sh "$inicio" "$fin"

