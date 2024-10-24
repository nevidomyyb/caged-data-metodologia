import pandas as pd
import ijson
import os
import json


excluir = [
    'uf', 'municApio', 'saldomovimentaASSAPSo', 'graudeinstruASSAPSo', 'horascontratuais', 'tipoempregador', 'tipoestabelecimento', 
    'indtrabintermitente', 'indtrabparcial', 'salA!rio', 'tamestabjan', 'indicadoraprendiz', 'origemdainformaASSAPSo', 'unidadesalA!riocA3digo', 'valorsalA!riofixo'
]
files = os.listdir('./RAW')
backup_file = './RAW/arquivo_backup.json'

    
for json_file in files:
    json_file = f"./RAW/{json_file}"
    erros = 0
    print(f'Analisando e processando arquivo: {json_file[0:json_file.find(".")]}')
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
                print("ERRRO ao remover colunas...")
                print("Desconsiderando linha.")
                erros+=1
                continue
            processed_obj = df.to_dict(orient='records')[0]
            if not is_first_item:
                f_out.write(',\n')
            else:
                is_first_item = False
            json.dump(processed_obj, f_out)
        f_out.write(']')
    print(f"Finalizado com {erros} linhas perdidas.")
    print(f"O arquivo final tem: {os.path.getsize(json_file)/1024}kb")
    print("----"*10)