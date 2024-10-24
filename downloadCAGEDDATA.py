from ftplib import FTP, error_perm
import os
from time import time

base_dir = 'pdet/microdados/NOVO CAGED'
ftp = FTP('ftp.mtps.gov.br', timeout=3600)
ftp.encoding = 'latin1'
ftp.login()
ftp.cwd(base_dir)

if not os.path.exists("CAGED"): os.makedirs("CAGED")

def download_file(ftp, remote_file, local_file, blocksize=8192):
    local_file_size = 0
    if os.path.exists(local_file):
        local_file_size = os.path.getsize(local_file)
    remote_file_size = ftp.size(remote_file)
    
    if local_file_size >= remote_file_size:
        print(f"O arquivo {local_file} já foi completamente baixado")
        return
    
    with open(local_file, 'ab') as f:
        def callback(data):
            f.write(data)
        print(f"Reiniciando download a partir de: {local_file_size} bytes...")
        ftp.retrbinary(f"RETR {remote_file}", callback, blocksize=blocksize, rest=local_file_size)
    print(f"Download concluido: {local_file}")
    
start = time()
for item in ftp.nlst():
    original_dir = ftp.pwd()
    if item in ['2020', '2021','2022', '2023', '2024']:
        print(f"Baixando dados de: {item}")
        ftp.cwd(item)
        l_dir = ftp.pwd()
        for item_interno in ftp.nlst():
            print(f"Em: {item_interno}")
            medium_dir = ftp.pwd()
            ftp.cwd(item_interno)
            for item_rar in ftp.nlst():
                local_file = os.path.join("CAGED", item_rar)
                try:
                    download_file(ftp, item_rar, local_file)
                except error_perm:
                    print(f"Erro de permissao")
                except Exception as e:
                    print(f"ERro: {e}") 
                    break
                        
                print(f"Acabei de baixar o: {item_rar}")
            ftp.cwd(l_dir)
        ftp.cwd(original_dir)
end = time()
exec_time = end - start
print(f"Tempo de execução: {exec_time} segundos")