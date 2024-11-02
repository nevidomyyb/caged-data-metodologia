import pandas as pd
import ijson
import os
import json
from time import time, sleep
from decimal import Decimal

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

excluir = [
    'uf', 'municipio', 'saldomovimentacao', 'graudeinstrucao', 'horascontratuais', 'tipoempregador', 'tipoestabelecimento', 
    'indtrabintermitente', 'indtrabparcial', 'salario', 'tamestabjan', 'indicadoraprendiz', 'origemdainformacao', 'unidadesalariocodigo', 'valorsalariofixo'
]
excluir_ = [
    'uf', 'municApio', 'saldomovimentaASSAPSo', 'graudeinstruASSAPSo', 'horascontratuais', 'tipoempregador', 'tipoestabelecimento', 
    'indtrabintermitente', 'indtrabparcial', 'salA!rio', 'tamestabjan', 'indicadoraprendiz', 'origemdainformaASSAPSo', 'unidadesalA!riocA3digo', 'valorsalA!riofixo'
]
files = os.listdir('./RAW')
backup_file = './RAW/arquivo_backup.json'

start = time()
for json_file in files:
    json_file = f"./RAW/{json_file}"
    erros = 0
    print(f'Analisando e processando arquivo: {json_file}')
    print(f'Ele possui: {os.path.getsize(json_file)/1024}kb')
    os.rename(json_file, backup_file)
    with open(backup_file, 'r') as f_in, open(json_file, 'w') as f_out:
        f_out.write('[')
        is_first_item = True
        
        for obj in ijson.items(f_in, 'item'):
            df = pd.json_normalize(obj)
            try:
                df.drop(columns=excluir, inplace=True, axis=1)
            except:
                try:
                    df.drop(columns=excluir_, inplace=True, axis=1)
                except:
                    import traceback
                    print("ERRRO ao remover colunas... INTERNAS")
                    print("Desconsiderando linha.")
                    traceback.print_exc()
                    erros+=1
                    sleep(3)
                    continue
            processed_obj = df.to_dict(orient='records')[0]
            if not is_first_item:
                f_out.write(',\n')
            else:
                is_first_item = False
            json.dump(processed_obj, f_out, default=convert_decimal)
        f_out.write(']')
    os.remove(backup_file)
    print(f"Finalizado com {erros} linhas perdidas.")
    print(f"O arquivo final tem: {os.path.getsize(json_file)/1024}kb")
    print("----"*10)
end = time()
exec_time = end - start
print(f"Tempo de execução: {exec_time} segundos")