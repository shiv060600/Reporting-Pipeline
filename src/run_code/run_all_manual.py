#type:ignore
import os
import subprocess
import sys
#note: for run_all_manual the order of the scripts must be correct
from ..helpers.all_scripts import ROOT_FOLDER,PYTHON_EXECUTABLES

#global python should have all we need for all scripts, no need to activate any venv
python_system_executable = r"C:\anaconda3\envs\reportingenv\python.exe"

for path in PYTHON_EXECUTABLES:
    full_file_path = os.path.join(ROOT_FOLDER,path)
    if os.path.exists(full_file_path):
        subprocess.run([python_system_executable,full_file_path],check=True)
    else:
        print(f"{full_file_path} not found")




