#!/bin/bash

start_time=$(date +%s)

sleep 61

end_time=$(date +%s)

execution_time=$((end_time - start_time))


if [[ execution_time > 60 ]] ; then
     minutos=$((execution_time / 60))
     echo "El script SQL se ejecutó en ${minutos} minutos."
else
     echo "El script SQL se ejecutó en ${execution_time} segundos."
fi

