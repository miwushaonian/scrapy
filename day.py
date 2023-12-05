import time
import subprocess

while True:
    shell_cmd = "python main.py -s=127.0.0.1 -p=6333 -page=1 -ep=5"
    for i in range(3600):
        subprocess.Popen(shell_cmd).wait()
        print(f"第{i}次更新")
        time.sleep(60)
