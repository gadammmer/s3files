#!/bin/bash

/opt/GEAOSP_PY_POC_TURISMO/scripts/move_flux_snowflake.sh "$(date -d 'yesterday' +'%Y-%m-%dT00:00:00.%N%z')" "$(date -d 'yesterday' +'%Y-%m-%dT23:59:59.%N%z')"

