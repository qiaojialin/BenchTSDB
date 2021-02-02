import os
import subprocess

source = ['geolife', 'noaa', 'redd', 'tdrive']
target = ['orc', 'parquet', 'tsfile']

for s in source:
    for t in target:
        base_dir = s + '-' + t
        os.chdir(base_dir)
        print(os.getcwd())

        cmd = "java -cp BenchTSDB.jar cn.edu.thu.MainQuery"
        print(cmd)
        process = subprocess.Popen(cmd, shell=True)
        process.wait()

        os.chdir('..')
        
