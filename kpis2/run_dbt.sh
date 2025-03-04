#!/bin/bash

# Script to run dbt commands using the conda environment "dbt"

# Check if a command was provided
if [ $# -eq 0 ]; then
    echo "Usage: ./run_dbt.sh <dbt_command> [additional_args]"
    echo "Example: ./run_dbt.sh run"
    echo "Example: ./run_dbt.sh test --select arpu"
    exit 1
fi

# Store the dbt command and arguments
DBT_COMMAND=$1
shift
ADDITIONAL_ARGS=$@

# Run the dbt command in the conda environment
echo "Running: dbt $DBT_COMMAND $ADDITIONAL_ARGS"
conda run -n dbt dbt $DBT_COMMAND $ADDITIONAL_ARGS

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "Command completed successfully!"
else
    echo "Command failed with error code $?"
fi
