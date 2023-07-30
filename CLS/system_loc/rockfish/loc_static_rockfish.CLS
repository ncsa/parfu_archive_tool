#!/usr/bin/bash

#SBATCH --partition=defq                         # Partition (queue)
#SBATCH --output=serial_%j.out                  # stdout from job is written to this file
#SBATCH --error=serial_%j.err                   # stderr from job is written to this file
