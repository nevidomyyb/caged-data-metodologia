import pandas as pd
import glob
import csv
import os
import unidecode
import json

if not os.path.exists("RAW"): os.makedirs("RAW")

diretorios = ['CAGEDEXC', 'CAGEDMOV', 'CAGEDFOR']

def transformar_json(diretorio):
    chunksize = 100000
    for i in ['2020', '2021', '2022', '2023', '2024']:
        files = glob.glob(os.path.join('DATA-UNZIPED', f"{diretorio}{i}*.txt"))
        print(f"Encontrado os arquivos para: {diretorio}{i}*.txt")
        output_file = os.path.join('RAW', f"{diretorio}-{i}.json")
        first_chunk = True
        with open(output_file, 'w') as f_out:
            f_out.write('[')
            for file in files:
                if os.stat(file).st_size == 0:
                    print("Tinha um arquivo com size 0 bytes, pulando...")
                    continue
                for chunk in pd.read_csv(file, delimiter=';', header=0, chunksize=chunksize, encoding='latin1', on_bad_lines='skip', skip_blank_lines=True):
                    
                    chunk.columns = [unidecode.unidecode(col).replace(" ", "_") for col in chunk.columns]
                    json_str = chunk.to_json(orient='records')
                    json_str = json_str[1:-1]
                    if not first_chunk:
                        f_out.write(',')
                    f_out.write(json_str)
                    first_chunk= False
            f_out.write(']')
            print('Finalizado')
try:
    from time import time
    start= time()
    for diretorio in diretorios:
        transformar_json(diretorio)
    end = time()
    exec_time = end - start
    print(f"Tempo de execução: {exec_time} segundos")
except Exception as e:
    print('quebrou')
    print(str(e))
    end = time()
    exec_time = end - start
    print(f"Tempo de execução: {exec_time} segundos")