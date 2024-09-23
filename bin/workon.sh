#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
PROJ_ROOT="${THIS_DIR}/.."

# Define the name of the virtual environment
VENV_PATH="${PROJ_ROOT}/venv"

# Create the virtual environment
echo "Creating virtual environment named venv..."
PIP=${VENV_PATH}/bin/pip3

function pip_cmd {
    source ${VENV_PATH}/bin/activate && ${PIP} "$@"
}

pushd ${PROJ_ROOT} >> /dev/null

if [ ! -d ${VENV_PATH} ]; then
    python3 -m venv ${VENV_PATH}
fi

pip_cmd install -U pip setuptools wheel
pip_cmd install -r requirements.txt

echo "Virtual environment created successfully."

source ${VENV_PATH}/bin/activate
