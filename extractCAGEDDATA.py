import os
import py7zr
from time import time
if not os.path.exists("DATA-UNZIPED"): os.makedirs("DATA-UNZIPED")

start = time()
for file in os.listdir('CAGED'):
    extract_path = 'DATA-UNZIPED'
    try:
        with py7zr.SevenZipFile(os.path.join("CAGED", file), "r") as zip_ref:
            zip_ref.extractall(path=extract_path)
    except:
        print(file)
end = time()
exec_time = end - start
print(f"Tempo de execução: {exec_time} segundos")
        