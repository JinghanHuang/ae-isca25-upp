#!/bin/bash

echo "Generating figure 6 and 7"
python process_results.py

echo "Generating figure 8 (a) and (b)"
python process_results_fig8.py

echo "All figures are generated"