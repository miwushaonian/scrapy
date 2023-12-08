import time
import subprocess

while True:
    shell_cmd = "python main.py -s=127.0.0.1 -p=6333 -page=1 -ep=20"
    for i in range(60):
        time.sleep(60)
    print(f"第{i}次更新")
    subprocess.Popen(shell_cmd).wait()
