#!/bin/bash

echo This script is meant to be run within a virtual environment only
poetry remove cvm-shared && poetry add ./shared/src/
