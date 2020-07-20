#!/bin/bash

cd /home/ping3/Streamlit_app
module load python-3.7.3
python3.7 BatchAlim.py
nohup streamlit run streamlit_test.py