#!/bin/bash

# Create output directory
mkdir -p /tmp/output

# Convert Jupyter notebook to .py and run
jupyter nbconvert --to script assignment1.ipynb
spark-submit assignment1.py

echo "✅ Notebook executed. Output available in /tmp/output/"
