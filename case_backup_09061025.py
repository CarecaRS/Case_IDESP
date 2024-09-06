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
saresp_2018.loc[mask_9, 'SERIE_ANO'] = '9º Ano EF'
saresp_2018.loc[mask_5, 'SERIE_ANO'] = '5º Ano EF'
saresp_2018.loc[mask_3, 'SERIE_ANO'] = '3º Ano EF'

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

# Feature 'mun' tem um nome equivocado ainda
mask_mun = saresp_completo['mun'] == 'FLOR?NEA'
saresp_completo.loc[mask_mun, 'mun'] = 'FLORINIA'
mask_mun = saresp_completo['mun'] == 'FLORINEA'
saresp_completo.loc[mask_mun, 'mun'] = 'FLORINIA'
mask_mun = saresp_completo['mun'] == 'FLORÍNEA'
saresp_completo.loc[mask_mun, 'mun'] = 'FLORINIA'

# Exclusão das instituições que não sejam estaduais ou municipais
excluir = ['Particulares & Sesi', 'Rede SESI', 'Escolas Particulares']
mask_dropar = saresp_completo.NomeDepBol.isin(excluir)
mask_dropar = saresp_completo.index[mask_dropar]
saresp = saresp_completo.drop(mask_dropar, axis=0, inplace=True)
saresp = saresp.reset_index(drop=True)

# Seguindo aqui o padrão descrito em 'Nota tecnica_2019.pdf'
# disponível na página de IDESP por Escola

# Exclusão das observações em 'ds_comp' == CIÊNCIAS
mask_ciencias = saresp.ds_comp.isin(['CIÊNCIAS'])
mask_ciencias = saresp.index[mask_ciencias]
saresp.drop(mask_ciencias, axis=0, inplace=True)
saresp = saresp.reset_index(drop=True)

# Exclusão também das observações do 2º e 3º Ano EF
mask_ef = saresp.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF'])
mask_ef = saresp.index[mask_ef]
saresp.drop(mask_ef, axis=0, inplace=True)
saresp = saresp.reset_index(drop=True)

# Ajuste caracteres acentuados > não acentuados
saresp = saresp.replace({'ds_comp': {'Í': 'I', 'Á': 'A'}}, regex=True)
saresp = saresp.replace({'periodo': {'Ã': 'A'}}, regex=True)

# Ajuste feature 'medprof' para float
saresp = saresp.replace({'medprof': {',': '.'}}, regex=True)
saresp.medprof = saresp.medprof.astype(float)

###
# NÍVEIS DE DESEMPENHO
# Obs.: optei por fazer através de vetores e não por loops pois
# fica muito mais ágil para procesamento.
###
# Dos filtros:
# - primeira linha: disciplina
# - segunda linha: turma
# - terceira linha: amplitude da nota

# Português, 5o ano EF
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof < 150)
saresp.loc[mask, 'ID'] = 'Abaixo do basico'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof < 200) & (saresp.medprof >= 150)
saresp.loc[mask, 'ID'] = 'Basico'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof < 250) & (saresp.medprof >= 200)
saresp.loc[mask, 'ID'] = 'Adequado'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof >= 250)
saresp.loc[mask, 'ID'] = 'Avancado'

# Português, 9o ano EF
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof < 200)
saresp.loc[mask, 'ID'] = 'Abaixo do basico'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof < 275) & (saresp.medprof >= 200)
saresp.loc[mask, 'ID'] = 'Basico'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof < 325) & (saresp.medprof >= 275)
saresp.loc[mask, 'ID'] = 'Adequado'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof >= 325)
saresp.loc[mask, 'ID'] = 'Avancado'

# Português, 3o ano EM
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof < 250)
saresp.loc[mask, 'ID'] = 'Abaixo do basico'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof < 300) & (saresp.medprof >= 250)
saresp.loc[mask, 'ID'] = 'Basico'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof < 375) & (saresp.medprof >= 300)
saresp.loc[mask, 'ID'] = 'Adequado'
mask = (saresp.ds_comp == 'LINGUA PORTUGUESA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof >= 375)
saresp.loc[mask, 'ID'] = 'Avancado'


# Matemática, 5o ano EF
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof < 175)
saresp.loc[mask, 'ID'] = 'Abaixo do basico'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof < 225) & (saresp.medprof >= 175)
saresp.loc[mask, 'ID'] = 'Basico'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof < 275) & (saresp.medprof >= 225)
saresp.loc[mask, 'ID'] = 'Adequado'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '5º Ano EF') & \
        (saresp.medprof >= 275)
saresp.loc[mask, 'ID'] = 'Avancado'

# Matemática, 9o ano EF
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof < 225)
saresp.loc[mask, 'ID'] = 'Abaixo do basico'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof < 300) & (saresp.medprof >= 225)
saresp.loc[mask, 'ID'] = 'Basico'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof < 350) & (saresp.medprof >= 300)
saresp.loc[mask, 'ID'] = 'Adequado'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == '9º Ano EF') & \
        (saresp.medprof >= 350)
saresp.loc[mask, 'ID'] = 'Avancado'

# Matemática, 3o ano EM
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof < 275)
saresp.loc[mask, 'ID'] = 'Abaixo do basico'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof < 350) & (saresp.medprof >= 275)
saresp.loc[mask, 'ID'] = 'Basico'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof < 400) & (saresp.medprof >= 350)
saresp.loc[mask, 'ID'] = 'Adequado'
mask = (saresp.ds_comp == 'MATEMATICA') & \
        (saresp.SERIE_ANO == 'EM-3ª série') & \
        (saresp.medprof >= 400)
saresp.loc[mask, 'ID'] = 'Avancado'

# Carregando os dados de fluxo dos municípios
fluxo_2017 = pd.read_csv('databases/fluxo_2017.csv')
fluxo_2018 = pd.read_csv('databases/fluxo_2018.csv', delimiter=';')
fluxo_2019 = pd.read_csv('databases/fluxo_2019.csv')
fluxo_2021 = pd.read_csv('databases/fluxo_2021.csv')
fluxo_2022 = pd.read_csv('databases/fluxo_2022.csv')
fluxo_2023 = pd.read_csv('databases/fluxo_2023.csv')

# Ajuste das features
fluxo_2018.columns = fluxo_2017.columns
fluxo_2019.columns = fluxo_2017.columns
fluxo_2021 = fluxo_2021[fluxo_2021.columns.drop(list(fluxo_2021.filter(regex='Unnamed')))]
fluxo_2021.columns = fluxo_2017.columns
fluxo_2022 = fluxo_2022[fluxo_2022.columns.drop(list(fluxo_2022.filter(regex='Unnamed')))]
fluxo_2022.columns = fluxo_2017.columns
fluxo_2023 = fluxo_2023[fluxo_2023.columns.drop(list(fluxo_2023.filter(regex='Unnamed')))]
fluxo_2023.columns = fluxo_2017.columns

# União dos fluxos em um só dataset
fluxo_completo = pd.concat([fluxo_2017, fluxo_2018, fluxo_2019,
                            fluxo_2021, fluxo_2022, fluxo_2023],
                           ignore_index=True)

# Substituição da vírula por ponto nas features numéricas
for col in fluxo_completo.filter(regex='_').drop('NM_MUNICIPIO', axis=1).columns:
    fluxo_completo = fluxo_completo.replace({col: {',': '.'}}, regex=True)

# Drop de algumas features não fundamentais para o caso
saresp = saresp.drop(['DEPADM', 'DepBol', 'codRMet', 'codmun', 'cod_per', 'co_comp'], axis=1)
fluxo = fluxo_completo.drop(['NM_DIRETORIA', 'CD_REDE_ENSINO'], axis=1)

# Salva os datasets já trabalhados
fluxo.to_csv('databases/fluxo_completo.csv', index_label=False)
saresp.to_csv('databases/saresp_completo.csv', index_label=False)

"""

fluxo = pd.read_csv('databases/fluxo_completo.csv')
saresp = pd.read_csv('databases/saresp_completo.csv')

########################
# SEGUE A PARTIR DAQUI #
########################

for col in saresp.columns:
    print('\n')
    print(saresp[col].value_counts())

for col in fluxo.columns:
    print('\n')
    print(fluxo[col].value_counts())

saresp[saresp.periodo == 'GERAL'].drop('periodo', axis=1).groupby(['ano', 'mun', 'NomeDepBol', 'SERIE_ANO', 'ds_comp'])['medprof'].mean()

fluxo.groupby(['Ano', 'NM_MUNICIPIO']).value_counts()


# Carregamento do dataset já com os ajustes acima
saresp = pd.read_csv('databases/saresp_completo.csv')
saresp = saresp.dropna()
saresp = saresp.reset_index(drop=True)

# Gráfico do histórico das proficiências de cada cidade ao longo do tempo
# Registrar nível de desempenho

