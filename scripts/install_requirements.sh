#!/bin/bash

cd ..

venv_path="venv"

# Check if the virtual environment exists
if [ ! -d "${venv_path}" ]; then
    echo "Creating virtual environment in ${venv_path}..."
    python3.9 -m venv "${venv_path}"
else
  echo "Virtual environment exists, skipping virtual environment creation."
fi

# Activate the virtual environment
echo "Activating the virtual environment..."
source "${venv_path}"/bin/activate

python3.9 -m pip install --upgrade pip
# Install the dependencies
echo "Installing dependencies..."
python3.9 -m pip install -r requirements.txt

deactivate

cd scripts/ || exit
