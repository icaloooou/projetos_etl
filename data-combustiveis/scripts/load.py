# -*- coding: UTF-8 -*-

# IMPORTS
import logging
import pandas as pd

# FUNCTIONS
def read_file(path):
    return pd.read_parquet(path)

def write_data(data):
    data.to_csv('resources/gold_revendedores_combustiveis.csv', index=False, header=[*data.columns], sep=';')

# LOGGER
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger('Load data > anp revendedores')
logger.setLevel(logging.INFO)

# READ
logger.info('Lendo arquivo silver (tratado)')
file = read_file('resources/silver_revendedores_combustiveis.parquet')

# MANIPULATION
file = file.fillna('').astype(str)

# WRITE
write_data(file)
logger.info('Escrita concluída!')
