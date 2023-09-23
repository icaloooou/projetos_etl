# -*- coding: utf-8 -*-

import json
import requests
import pandas as pd


# FUNCTIONS
def requests_(url):
    return requests.get(url)


def load_json(json_text):
    return json.loads(json_text)


def read_json(dict_, type_):
    list_all = []
    for item in dict_:
        code_ibge = item.get('localidade').get('id')
        city_state = item.get('localidade').get('nome')
        value = item.get('serie').get('2022')
        city = city_state.split('-')[0].strip()
        state = city_state.split('-')[1].strip()
        list_all.append({'code_ibge': code_ibge,
                        type_: value,
                        'city': city,
                        'state': state})
    return list_all


# READ DATA
people = requests_('https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[all]')
homes = requests_('https://servicodados.ibge.gov.br/api/v3/agregados/4711/periodos/2022/variaveis/617?localidades=N6[all]&classifica cao=3[59993,9697,2504,2505,103995]')

json_people = load_json(people.text)
people_values = json_people[0]['resultados'][0]['series']

json_homes = load_json(homes.text)
homes_values = json_homes[0]['resultados'][0]['series']

# MANIPULATION
df_people = pd.DataFrame(read_json(people_values, 'pessoas'))
df_homes = pd.DataFrame(read_json(homes_values, 'domicilios'))

# TRANSFORMATION
df_join = pd.merge(df_people, df_homes, on=['code_ibge', 'city', 'state'])
df_final = df_join[['code_ibge', 'city', 'state', 'domicilios', 'pessoas']].astype(str)

# WRITE
df_final.to_csv('./resources/censo_2022.csv')
