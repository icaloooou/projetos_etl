# -*- coding: UTF-8 -*-

# IMPORTS
import os
import io
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

# FUNCTIONS
def access_url(link):
    return requests.get(link)

def get_html(html):
    return BeautifulSoup(html, 'html.parser')

def get_links(soup, tagOne, tagTwo):
    links = soup.find_all(tagOne)
    return [i.get(tagTwo) for i in links]

def download_and_write_data(link_selected):
    df = pd.read_csv(link_selected[0], sep=';', header=0)
    df.to_parquet('/resources/bronze_revendedores_combustiveis.parquet')

# LOGGER
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger('Extract data > anp revendedores')
logger.setLevel(logging.INFO)

# READ
logger.info('Acessando site do governo...')
req = access_url('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos')
soup = get_html(req.text)
links = get_links(soup, 'a', 'href')
link_selected = [i for i in links if 'dados-cadastrais-dos-revendedores' in i if '.csv' in i]

# DOWNLOAD - WRITE
if len(link_selected) == 1:
    download_and_write_data(link_selected)
    logger.info('Download e escrita concluídos!')
else:
    logger.info('Verificar a quantidade de links disponibilizados.')
