# Importação dos pacotes necessários
import numpy as np
import pandas as pd
%autoindent OFF  # configuração específica da minha IDE (Neovim)

"""
###
# PROCEDIMENTOS INICIAIS DE IMPORTAÇÃO E LIMPEZA/AJUSTE DOS DATASETS
# 
###

saresp_2017 = pd.read_csv('databases/saresp_2017.csv', delimiter=';')
saresp_2018 = pd.read_csv('databases/saresp_2018.csv', delimiter=',')
saresp_2019 = pd.read_csv('databases/saresp_2019.csv', delimiter=';')
saresp_2021 = pd.read_csv('databases/saresp_2021.csv', delimiter=',')
saresp_2022 = pd.read_csv('databases/saresp_2022.csv', delimiter=';')
saresp_2023 = pd.read_csv('databases/saresp_2023.csv', delimiter=';')

# 2018 teve caracteres não identificados, transformando ao padrão
# com a utilização de máscaras (vetores True/False)
# Feature 'ds_comp'
mask_port = saresp_2018['ds_comp'] == 'L?NGUA PORTUGUESA'
mask_mat = saresp_2018['ds_comp'] == 'MATEM?TICA'
saresp_2018.loc[mask_port, 'ds_comp'] = 'LINGUA PORTUGUESA'
saresp_2018.loc[mask_mat, 'ds_comp'] = 'MATEMATICA'

# Feature 'SERIE_ANO'
mask_em = saresp_2018['SERIE_ANO'] == 'EM-3? s?rie'
mask_9 = saresp_2018['SERIE_ANO'] == '9? Ano EF'
mask_5 = saresp_2018['SERIE_ANO'] == '5? Ano EF'
mask_3 = saresp_2018['SERIE_ANO'] == '3? Ano EF'
saresp_2018.loc[mask_em, 'SERIE_ANO'] = 'EM-3ª série'
saresp_2018.loc[mask_9, 'SERIE_ANO'] = '9ª Ano EF'
saresp_2018.loc[mask_5, 'SERIE_ANO'] = '5ª Ano EF'
saresp_2018.loc[mask_3, 'SERIE_ANO'] = '3ª Ano EF'

# Eliminando coluna que não aparece em nenhum outro ano
saresp_2018.drop('Unnamed: 12', axis=1, inplace=True)

# Algumas colunas possuem nomes divergentes (maiúsculo/minúsculo),
# deixando todas no mesmo padrão
saresp_2019.columns = saresp_2018.columns
saresp_2021.columns = saresp_2018.columns
saresp_2022.columns = saresp_2018.columns
saresp_2023.columns = saresp_2018.columns

# Registro dos anos de cada dataset para posterior agrupamento
saresp_2017['ano'] = '2017'
saresp_2018['ano'] = '2018'
saresp_2019['ano'] = '2019'
saresp_2021['ano'] = '2021'
saresp_2022['ano'] = '2022'
saresp_2023['ano'] = '2023'

# Agrupando todos os anos em um único dataset
saresp_completo = pd.concat([saresp_2017, saresp_2018, saresp_2019,
                             saresp_2021, saresp_2022, saresp_2023],
                            ignore_index=True)

saresp_completo.to_csv('databases/saresp_completo.csv', index_label=False)
"""
saresp = pd.read_csv('databases/saresp_completo.csv')

saresp.loc[0]
