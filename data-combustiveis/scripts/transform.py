# -*- coding: UTF-8 -*-

# IMPORTS
import logging
import pandas as pd

# FUNCTIONS
def read_file(path):
    return pd.read_parquet(path)

def write_data(data):
    data.to_parquet('resources/silver_revendedores_combustiveis.parquet')

# LOGGER
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger('Transform data > anp revendedores')
logger.setLevel(logging.INFO)

# READ
logger.info('Lendo arquivo bronze (bruto)')
file = read_file('resources/bronze_revendedores_combustiveis.parquet')

# MANIPULATION
logger.info('Transformando coluna CNPJ')
file['CNPJ'] = file['CNPJ'].astype(str).apply(lambda x: x.zfill(14))
logger.info('Transformando coluna CEP')
file['CEP'] = file['CEP'].astype(str).apply(lambda x: x.zfill(8))

# WRITE
write_data(file)
logger.info('Escrita concluída!')
