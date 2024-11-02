import pandas as pd
import ijson
import os
import json
from time import time
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

colunas = {
    "competAnciamov": "competênciamov",
    "regiAPSo": "região",
    "seASSAPSo": "seção",
    "cbo2002ocupaASSAPSo": "cbo2002ocupação",
    "raASSacor": "raçacor",
    "tipomovimentaASSAPSo": "tipomovimentação",
    "tipodedeficiAancia": "tipodeficiência",
    "competAanciadec": "competênciadec",
}

def processar_arquivo(file: str):
    backup_file = f"{file.replace('.json', '')}-backup.json"
    erros = 0
    print(f"Analisando e processando o arquivo: {file}")
    print(f"Ele possui {os.path.getsize(file) / 1024}kb")
    os.rename(file, backup_file)
    with open(backup_file, 'r') as f_in, open(file, 'w') as f_out:
        f_out.write('[')
        is_first_item=True
        for obj in ijson.items(f_in, 'item'):
            df = pd.json_normalize(obj)
            columns_to_rename = {
                k: v for k, v in colunas.items() if k in df.columns
            }
            if columns_to_rename:
                df.rename(columns=columns_to_rename, inplace=True)
                processed_obj = df.to_dict(orient='records')[0]
                if not is_first_item:
                    f_out.write(',\n')
                else:
                    is_first_item = False
                
                json.dump(processed_obj, f_out, default=convert_decimal)
            else:
                processed_obj = df.to_dict(orient='records')[0]
                if not is_first_item:
                    f_out.write(',\n')
                else:
                    is_first_item = False
                json.dump(processed_obj, f_out, default=convert_decimal)
        f_out.write(']')
        os.remove(backup_file)
        print(f"Arquivo {json} finalizado com {erros} linhas perdidas.")
        print(f"Tamanho final do arquivo: {os.path.getsize(json) / 1024} kb")
        print("----" * 10)
def main():
    start = time()
    files = [f"./RAW/{file}" for file in os.listdir('./RAW') if file.endswith('.json')]
    
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(processar_arquivo, file) for file in files]
        for future in futures:
            future.result()  # Espera cada tarefa terminar

    
    end = time()
    print(f"Tempo total de execução: {end - start} segundos")

if __name__ == "__main__":
    main()
