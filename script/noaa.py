import os
import subprocess

source = ['noaa-orc', 'noaa-parquet']
target = ['1', '2', '4', '7', '14']

for s in source:
    for t in target:
        base_dir = s + '-' + t
        os.chdir(base_dir)
        print(os.getcwd())

        cmd = "java -cp BenchTSDB.jar cn.edu.thu.MainLoad"
        print(cmd)
        process = subprocess.Popen(cmd, shell=True)
        process.wait()

        os.chdir('..')
        
