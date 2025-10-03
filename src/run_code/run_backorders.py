#type:ignore
import os
import subprocess
import sys
#note: for run_all_manual the order of the scripts must be correct
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT_FOLDER = "H:\\Upgrading_Database_Reporting_Systems\\REPORTING_PIPELINE\\src"

#for now we do not include create_ing_sales or monthly_sales_upload

PYTHON_EXECUTABLES = [
    "backorders\\upload_backorders.py"
]

#global python should have all we need for all scripts, no need to activate any venv
python_system_executable = "C:\\anaconda3\\envs\\reportingenv\\python.exe"

for path in PYTHON_EXECUTABLES:
    full_file_path = os.path.join(ROOT_FOLDER,path)
    if os.path.exists(full_file_path):
        subprocess.run([python_system_executable,full_file_path],check=True,cwd=ROOT_FOLDER)
    else:
        print(f"{full_file_path} not found")