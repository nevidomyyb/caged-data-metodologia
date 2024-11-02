import pandas as pd
import ijson
import json
from time import time
import os
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import sys
from stagedLayer import convert_decimal
import random
import string

def gerar_nome_aleatorio(tamanho=10):
    caracteres = string.ascii_letters + string.digits
    nome_aleatorio = ''.join(random.choice(caracteres) for _ in range(tamanho))
    return nome_aleatorio

files = {
    2020: ['./RAW/CAGEDEXC-2020.json', './RAW/CAGEDFOR-2020.json', './RAW/CAGEDMOV-2021.json'],
    2021: ['./RAW/CAGEDEXC-2021.json', './RAW/CAGEDFOR-2021.json', './RAW/CAGEDMOV-2022.json'],
    2022: ['./RAW/CAGEDEXC-2022.json', './RAW/CAGEDFOR-2022.json', './RAW/CAGEDMOV-2023.json'],
    2023: ['./RAW/CAGEDEXC-2023.json', './RAW/CAGEDFOR-2023.json', './RAW/CAGEDMOV-2024.json'],
    2024: ['./RAW/CAGEDEXC-2024.json', './RAW/CAGEDFOR-2024.json', './RAW/CAGEDMOV-2025.json'],
    2025: ['./RAW/CAGEDEXC-2025.json', './RAW/CAGEDFOR-2025.json', './RAW/CAGEDMOV-2026.json'],
}

def iniciar_processo(arquivos: dict):
    inicio = time()
    EXC = arquivos[0]
    FOR = arquivos[1]
    MOV = arquivos[2]        
    exc_df = pd.read_json(EXC)
    for_df = pd.read_json(FOR)
    mov_df = pd.read_json(MOV)
    all_df = pd.concat([mov_df, for_df], axis=0, ignore_index=True)
    mov_final_df = all[~['ID_movimentacao']]
    

XLS = pd.ExcelFile('./codes_description.xlsx')

