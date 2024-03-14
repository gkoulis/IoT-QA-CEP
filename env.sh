#!/bin/bash

# Get the current directory path
cur_path="$(pwd)"

# Concatenate the current directory path with the desired directory
dir_path="${cur_path}/iotvm-local-data/simulations"

# Set the environment variable
export SIMULATIONS_DIRECTORY_PATH="$dir_path"

# Check if the variable already exists in the shell configuration file
if ! grep -qxF "export SIMULATIONS_DIRECTORY_PATH=\"$SIMULATIONS_DIRECTORY_PATH\"" ~/.bashrc; then
    # If not, append the export command to the shell configuration file
    echo "export SIMULATIONS_DIRECTORY_PATH=\"$SIMULATIONS_DIRECTORY_PATH\"" >> ~/.bashrc
fi

# Display a message indicating that the environment variable is set
echo "Environment variables set:"
echo "SIMULATIONS_DIRECTORY_PATH=$SIMULATIONS_DIRECTORY_PATH"
