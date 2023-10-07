#!/bin/bash

echo This script is meant to be run within a virtual environment only
cd ./shared/src || exit
python -m build --no-isolation --wheel
