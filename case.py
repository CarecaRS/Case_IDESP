# Importação dos pacotes necessários
import time  # apenas para que eu possa mensurar eficiência do processamento
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.ar_model import AutoReg, AutoRegResults
%autoindent OFF  # configuração específica da minha IDE (Neovim)


###############################################
#  MANIPULAÇÃO DOS BANCOS DE DADOS DO SARESP  #
###############################################

saresp_2017 = pd.read_csv('databases/saresp_2017.csv', delimiter=',')
saresp_2018 = pd.read_csv('databases/saresp_2018.csv', delimiter=',')
saresp_2019 = pd.read_csv('databases/saresp_2019.csv', delimiter=',')
saresp_2021 = pd.read_csv('databases/saresp_2021.csv', delimiter=',')
saresp_2022 = pd.read_csv('databases/saresp_2022.csv', delimiter=',')
saresp_2023 = pd.read_csv('databases/saresp_2023.csv', delimiter=',')

# Registro dos Anos de cada dataset para posterior agrupamento
saresp_2017['Ano'] = '2017'
saresp_2018['Ano'] = '2018'
saresp_2019['Ano'] = '2019'
saresp_2021['Ano'] = '2021'
saresp_2022['Ano'] = '2022'
saresp_2023['Ano'] = '2023'

# 2023 possui uma variável extra (CODRMET), desconsiderada:
saresp_2023 = saresp_2023.drop('CODRMET', axis=1)

# 2019 possui uma variável extra (CodRMet), desconsiderada:
saresp_2019 = saresp_2019.drop('CodRMet', axis=1)

# Os datasets não possuem nomenclatura padrão para as features,
# deixando todas no mesmo padrão
saresp_2017.columns = saresp_2018.columns
saresp_2019.columns = saresp_2018.columns
saresp_2021.columns = saresp_2018.columns
saresp_2022.columns = saresp_2018.columns
saresp_2023.columns = saresp_2018.columns

# Salvando cada dataset individual em arquivo
saresp_2017.to_parquet('databases/saresp_2017.parquet')
saresp_2018.to_parquet('databases/saresp_2018.parquet')
saresp_2019.to_parquet('databases/saresp_2019.parquet')
saresp_2021.to_parquet('databases/saresp_2021.parquet')
saresp_2022.to_parquet('databases/saresp_2022.parquet')
saresp_2023.to_parquet('databases/saresp_2023.parquet')

# Agrupando todos os Anos em um único dataset
saresp_completo = pd.concat([saresp_2017, saresp_2018, saresp_2019,
                             saresp_2021, saresp_2022, saresp_2023],
                            ignore_index=True)

# Seguindo aqui o padrão descrito em 'Nota tecnica_2019.pdf'
# disponível na página de IDESP por Escola

# Exclusão das observações em 'ds_comp' == CIÊNCIAS
mask_ciencias = saresp_completo.ds_comp.isin(['CIÊNCIAS'])
mask_ciencias = saresp_completo.index[mask_ciencias]
saresp_completo.drop(mask_ciencias, axis=0, inplace=True)
saresp = saresp_completo.reset_index(drop=True)

# Exclusão também das observações do 2º, 3º e 7º Ano EF
mask_ef = saresp.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF', '7º Ano EF'])
mask_ef = saresp.index[mask_ef]
saresp.drop(mask_ef, axis=0, inplace=True)

# Ajuste caracteres acentuados > não acentuados
saresp = saresp.replace({'ds_comp': {'Í': 'I', 'Á': 'A'}}, regex=True)
saresp = saresp.replace({'periodo': {'Ã': 'A'}}, regex=True)

# Ajuste feature 'medprof' para float
saresp = saresp.replace({'medprof': {',': '.'}}, regex=True)
saresp.medprof = saresp.medprof.astype(float)

# Banco de dados possui algumas observações NaN, dropando essas
# linhas e reestruturando o índice
saresp = saresp.dropna()
saresp = saresp.reset_index(drop=True)


###
# NÍVEIS DE DESEMPENHO
# Obs.: optei por fazer através de vetores e não por loops pois
# fica muito mais ágil para procesamento.
###
# Dos filtros:
# - primeira linha: disciplina
# - segunda linha: turma
# - terceira linha: amplitude da nota

# Português, 5o Ano EF
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

# Português, 9o Ano EF
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

# Português, 3o Ano EM
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


# Matemática, 5o Ano EF
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

# Matemática, 9o Ano EF
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

# Matemática, 3o Ano EM
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

# Salvando o banco de dados completo para posterior utilização
saresp.to_parquet('databases/saresp_completo.parquet')


##############################################
# MANIPULAÇÃO DOS BANCOS DE DADOS DOS ALUNOS #
##############################################

# Definição dos universos de observações possíveis
serie_possivel = [12, 9, 5]
rendimento_possivel = [1, 2, 3, 4, 5, 6, 7, 8]
# situacao_possivel = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
#                     11, 12, 13, 14, 15, 16, 17, 18, 19,
#                     20, 21, 22, 31, 99, 105, 118]
situacao_possivel = [0, 2, 5, 16, 17, 18,
                     20, 21, 105, 118]
colunas = ['CD_ALUNO', 'CD_ESCOLA', 'RENDIMENTO', 'SERIE', 'FLAG_SIT_ALUNO']

###
# Wrangling do banco de dados de 2017
###
# Carregamento do arquivo original apenas com o nome de arquivo alterado
alunos_2017 = pd.read_csv('databases/alunos_2017.csv', on_bad_lines='skip',
                          sep=';', usecols=colunas)
alunos_2017 = alunos_2017.dropna()  # elimina as observações com valores 'NaN'
alunos_2017 = alunos_2017.astype(int)  # transforma as observações para números inteiros

# Verifica registros inválidos
(alunos_2017.RENDIMENTO.isin(rendimento_possivel) == False).sum()  # possui registros inválidos
(alunos_2017.SERIE.isin(serie_possivel) == False).sum()  # possui registros inválidos
(alunos_2017.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2017.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2017.loc[mask_rend].index
alunos_2017 = alunos_2017.drop(mask_rend)

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2017.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2017.loc[mask_serie].index
alunos_2017 = alunos_2017.drop(mask_serie)

# Limpeza das observações inválidas em 'FLAG_SIT_ALUNO'
mask_situacao = alunos_2017.FLAG_SIT_ALUNO.isin(situacao_possivel) == False
mask_situacao = alunos_2017.loc[mask_situacao].index
alunos_2017 = alunos_2017.drop(mask_situacao)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2017['Ano'] = '2017'
alunos_2017 = alunos_2017.reset_index(drop=True)
alunos_2017 = alunos_2017.drop(['CD_ALUNO', 'FLAG_SIT_ALUNO'], axis=1)

# Salva o banco de dados limpo em formato parquet (menor espaço/memória)
alunos_2017.to_parquet('databases/alunos_2017.parquet')


###
# Wrangling do banco de dados de 2018
###
# Carregamento do arquivo original apenas com o nome de arquivo alterado
alunos_2018 = pd.read_csv('databases/alunos_2018.csv', on_bad_lines='skip',
                          sep=';', usecols=colunas)
alunos_2018 = alunos_2018.dropna()  # elimina as observações com valores 'NaN'
alunos_2018 = alunos_2018.astype(int)  # transforma as observações para números inteiros

# Verifica registros inválidos
(alunos_2018.RENDIMENTO.isin(rendimento_possivel) == False).sum()  # possui registros inválidos
(alunos_2018.SERIE.isin(serie_possivel) == False).sum()  # possui registros inválidos
(alunos_2018.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2018.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2018.loc[mask_serie].index
alunos_2018 = alunos_2018.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2018.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2018.loc[mask_rend].index
alunos_2018 = alunos_2018.drop(mask_rend)

# Limpeza das observações inválidas em 'FLAG_SIT_ALUNO'
mask_situacao = alunos_2018.FLAG_SIT_ALUNO.isin(situacao_possivel) == False
mask_situacao = alunos_2018.loc[mask_situacao].index
alunos_2018 = alunos_2018.drop(mask_situacao)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2018['Ano'] = '2018'
alunos_2018 = alunos_2018.reset_index(drop=True)
alunos_2018 = alunos_2018.drop(['CD_ALUNO', 'FLAG_SIT_ALUNO'], axis=1)

# Salva o banco de dados limpo em formato parquet (menor espaço/memória)
alunos_2018.to_parquet('databases/alunos_2018.parquet')


###
# Wrangling do banco de dados de 2019
###
# Carregamento do arquivo original apenas com o nome de arquivo alterado
alunos_2019 = pd.read_csv('databases/alunos_2019.csv', on_bad_lines='skip',
                          sep=';', usecols=colunas)
alunos_2019 = alunos_2019.dropna()  # elimina as observações com valores 'NaN'
# alunos_2019 = alunos_2019.astype(int)  # registro indevido impossibilita a alteração para inteiros por enquanto

# Verifica registros inválidos
(alunos_2019.RENDIMENTO.isin(rendimento_possivel) == False).sum()  # possui registros inválidos
(alunos_2019.SERIE.isin(serie_possivel) == False).sum()  # possui registros inválidos
(alunos_2019.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2019.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2019.loc[mask_serie].index
alunos_2019 = alunos_2019.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2019.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2019.loc[mask_rend].index
alunos_2019 = alunos_2019.drop(mask_rend)

# Limpeza das observações inválidas em 'FLAG_SIT_ALUNO'
mask_situacao = alunos_2019.FLAG_SIT_ALUNO.isin(situacao_possivel) == False
mask_situacao = alunos_2019.loc[mask_situacao].index
alunos_2019 = alunos_2019.drop(mask_situacao)

# Agora sim a alteração para tipo inteiro é possível
alunos_2019 = alunos_2019.astype(int)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2019['Ano'] = '2019'
alunos_2019 = alunos_2019.reset_index(drop=True)
alunos_2019 = alunos_2019.drop(['CD_ALUNO', 'FLAG_SIT_ALUNO'], axis=1)

# Salva o banco de dados limpo em formato parquet (menor espaço/memória)
alunos_2019.to_parquet('databases/alunos_2019.parquet')


###
# Wrangling do banco de dados de 2020
###
# Carregamento do arquivo original apenas com o nome de arquivo alterado
alunos_2020 = pd.read_csv('databases/alunos_2020.csv', on_bad_lines='skip',
                          sep=';', usecols=colunas)
alunos_2020 = alunos_2020.dropna()  # elimina as observações com valores 'NaN'
alunos_2020 = alunos_2020.astype(int)  # transforma as observações para números inteiros

# Verifica registros inválidos
(alunos_2020.RENDIMENTO.isin(rendimento_possivel) == False).sum()  # possui registros inválidos
(alunos_2020.SERIE.isin(serie_possivel) == False).sum()  # possui registros inválidos
(alunos_2020.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2020.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2020.loc[mask_serie].index
alunos_2020 = alunos_2020.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2020.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2020.loc[mask_rend].index
alunos_2020 = alunos_2020.drop(mask_rend)

# Limpeza das observações inválidas em 'FLAG_SIT_ALUNO'
mask_situacao = alunos_2020.FLAG_SIT_ALUNO.isin(situacao_possivel) == False
mask_situacao = alunos_2020.loc[mask_situacao].index
alunos_2020 = alunos_2020.drop(mask_situacao)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2020['Ano'] = '2020'
alunos_2020 = alunos_2020.reset_index(drop=True)
alunos_2020 = alunos_2020.drop(['CD_ALUNO', 'FLAG_SIT_ALUNO'], axis=1)

# Salva o banco de dados limpo em formato parquet (menor espaço/memória)
alunos_2020.to_parquet('databases/alunos_2020.parquet')


###
# Wrangling do banco de dados de 2021
###
# Carregamento do arquivo original apenas com o nome de arquivo alterado
alunos_2021 = pd.read_csv('databases/alunos_2021.csv', on_bad_lines='skip',
                          sep=';', usecols=colunas)
alunos_2021 = alunos_2021.dropna()  # elimina as observações com valores 'NaN'
alunos_2021 = alunos_2021.astype(int)  # transforma as observações para números inteiros

# Verifica registros inválidos
(alunos_2021.RENDIMENTO.isin(rendimento_possivel) == False).sum()  # possui registros inválidos
(alunos_2021.SERIE.isin(serie_possivel) == False).sum()  # possui registros inválidos
(alunos_2021.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2021.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2021.loc[mask_serie].index
alunos_2021 = alunos_2021.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2021.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2021.loc[mask_rend].index
alunos_2021 = alunos_2021.drop(mask_rend)

# Limpeza das observações inválidas em 'FLAG_SIT_ALUNO'
mask_situacao = alunos_2021.FLAG_SIT_ALUNO.isin(situacao_possivel) == False
mask_situacao = alunos_2021.loc[mask_situacao].index
alunos_2021 = alunos_2021.drop(mask_situacao)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2021['Ano'] = '2021'
alunos_2021 = alunos_2021.reset_index(drop=True)
alunos_2021 = alunos_2021.drop(['CD_ALUNO', 'FLAG_SIT_ALUNO'], axis=1)

# Salva o banco de dados limpo em formato parquet (menor espaço/memória)
alunos_2021.to_parquet('databases/alunos_2021.parquet')


###
# Wrangling do banco de dados de 2022
###
# Carregamento do arquivo original apenas com o nome de arquivo alterado
alunos_2022 = pd.read_csv('databases/alunos_2022.csv', on_bad_lines='skip',
                          sep=';', usecols=colunas)
alunos_2022 = alunos_2022.dropna()  # elimina as observações com valores 'NaN'
alunos_2022 = alunos_2022.astype(int)  # transforma as observações para números inteiros

# Verifica registros inválidos
(alunos_2022.RENDIMENTO.isin(rendimento_possivel) == False).sum()  # possui registros inválidos
(alunos_2022.SERIE.isin(serie_possivel) == False).sum()  # possui registros inválidos
(alunos_2022.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # NÃO possui registros inválidos

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2022.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2022.loc[mask_serie].index
alunos_2022 = alunos_2022.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2022.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2022.loc[mask_rend].index
alunos_2022 = alunos_2022.drop(mask_rend)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2022['Ano'] = '2022'
alunos_2022 = alunos_2022.reset_index(drop=True)
alunos_2022 = alunos_2022.drop(['CD_ALUNO', 'FLAG_SIT_ALUNO'], axis=1)

# Salva o banco de dados limpo em formato parquet (menor espaço/memória)
alunos_2022.to_parquet('databases/alunos_2022.parquet')


###
# União dos datasets todos em um único arquivo
###
alunos = pd.concat([alunos_2017, alunos_2018, alunos_2019,
                   alunos_2020, alunos_2021, alunos_2022],
                   ignore_index=True)

# Salvando o banco de dados agrupado para utilização futura
alunos.to_parquet('databases/alunos_completo.parquet')


##############################################################
#  MANIPULAÇÃO DOS BANCOS DE DADOS DE MICRODADOS DOS ALUNOS  #
##############################################################

###
# MICRODADOS DE 2017
###
# Carregamento do banco de dados
microdados_2017 = pd.read_csv('databases/microdados_2017.csv', sep=';')

# Filtro das features a se manter
manter = ['CODESC', 'SERIE_ANO', 'nivel_profic_lp', 'nivel_profic_mat']
microdados_2017 = microdados_2017[manter]

# Exclusão das observações do 2º, 3º e 7º Ano EF
mask_ef = microdados_2017.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF', '7º Ano EF'])
mask_ef = microdados_2017.index[mask_ef]
microdados_2017.drop(mask_ef, axis=0, inplace=True)
microdados_2017 = microdados_2017.reset_index(drop=True)

# Marcação do ano do dataset
microdados_2017['Ano'] = '2017'

# Salva o banco de dados ajustado para uso futuro
microdados_2017.to_parquet('databases/microdados_2017.parquet')


###
# MICRODADOS DE 2018
###
# Carregamento do banco de dados
microdados_2018 = pd.read_csv('databases/microdados_2018.csv', sep=';')

# Filtro das features a se manter
manter = ['CODESC', 'SERIE_ANO', 'nivel_profic_lp', 'nivel_profic_mat']
microdados_2018 = microdados_2018[manter]

# Exclusão das observações do 2º, 3º e 7º Ano EF
mask_ef = microdados_2018.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF', '7º Ano EF'])
mask_ef = microdados_2018.index[mask_ef]
microdados_2018.drop(mask_ef, axis=0, inplace=True)
microdados_2018 = microdados_2018.reset_index(drop=True)

# Marcação do ano do dataset
microdados_2018['Ano'] = '2018'

# Salva o banco de dados ajustado para uso futuro
microdados_2018.to_parquet('databases/microdados_2018.parquet')


###
# MICRODADOS DE 2019
###
# Carregamento do banco de dados
microdados_2019 = pd.read_csv('databases/microdados_2019.csv', sep=';')

# Filtro das features a se manter
manter = ['CODESC', 'SERIE_ANO', 'nivel_profic_lp', 'nivel_profic_mat']
microdados_2019 = microdados_2019[manter]

# Exclusão das observações do 2º, 3º e 7º Ano EF
mask_ef = microdados_2019.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF', '7º Ano EF'])
mask_ef = microdados_2019.index[mask_ef]
microdados_2019.drop(mask_ef, axis=0, inplace=True)
microdados_2019 = microdados_2019.reset_index(drop=True)

# Marcação do ano do dataset
microdados_2019['Ano'] = '2019'

# Salva o banco de dados ajustado para uso futuro
microdados_2019.to_parquet('databases/microdados_2019.parquet')


###
# MICRODADOS DE 2021
###
# Carregamento do banco de dados
microdados_2021 = pd.read_csv('databases/microdados_2021.csv', sep=';')

# Filtro das features a se manter
manter = ['CODESC', 'SERIE_ANO', 'nivel_profic_lp', 'nivel_profic_mat']
microdados_2021 = microdados_2021[manter]

# Exclusão das observações do 2º, 3º e 7º Ano EF
mask_ef = microdados_2021.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF', '7º Ano EF'])
mask_ef = microdados_2021.index[mask_ef]
microdados_2021.drop(mask_ef, axis=0, inplace=True)
microdados_2021 = microdados_2021.reset_index(drop=True)

# Marcação do ano do dataset
microdados_2021['Ano'] = '2021'

# Salva o banco de dados ajustado para uso futuro
microdados_2021.to_parquet('databases/microdados_2021.parquet')


###
# MICRODADOS DE 2022
###
# Carregamento do banco de dados
microdados_2022 = pd.read_csv('databases/microdados_2022.csv', sep=';')

# Filtro das features a se manter
manter = ['CODESC', 'SERIE_ANO', 'nivel_profic_lp', 'nivel_profic_mat']
microdados_2022 = microdados_2022[manter]

# Exclusão das observações do 2º, 3º e 7º Ano EF
mask_ef = microdados_2022.SERIE_ANO.isin(['3º Ano EF', '2º Ano EF', '7º Ano EF'])
mask_ef = microdados_2022.index[mask_ef]
microdados_2022.drop(mask_ef, axis=0, inplace=True)
microdados_2022 = microdados_2022.reset_index(drop=True)

# Marcação do ano do dataset
microdados_2022['Ano'] = '2022'

# Salva o banco de dados ajustado para uso futuro
microdados_2022.to_parquet('databases/microdados_2022.parquet')


###
# União dos datasets todos em um único arquivo
###
microdados = pd.concat([microdados_2017, microdados_2018, microdados_2019,
                        microdados_2021, microdados_2022], ignore_index=True)

# Salvando o banco de dados agrupado para utilização futura
microdados.to_parquet('databases/microdados_completo.parquet')


################################################################################
#                   BANCOS DE DADOS PRONTOS, CÁLCULO DO IDESP                  #
#                                                                              #
#                      Referência: 'Nota tecnica_2019.pdf'                     #
# Disponível em                                                                #
# https://dados.educacao.sp.gov.br/sites/default/files/Nota%20tecnica_2019.pdf #
# Acesso em 05/09/2024.                                                        #
################################################################################

# Se necessário, carrega os bancos de dados já trabalhados anteriormente:
alunos = pd.read_parquet('databases/alunos_completo.parquet')
saresp = pd.read_parquet('databases/saresp_completo.parquet')
microdados = pd.read_parquet('databases/microdados_completo.parquet')

# Se os datasets forem carregados novamente é necessário refazer
# o ajuste dos dtypes. Ano, município, código da escola e código
# de tipo identificador são categorias:
int_fluxo = ['Ano', 'CD_ESCOLA', 'CD_TP_IDENTIFICADOR']
categ_fluxo = ['Ano', 'NM_MUNICIPIO', 'CD_ESCOLA', 'CD_TP_IDENTIFICADOR']
fluxo[int_fluxo] = fluxo[int_fluxo].astype('int')  # certifica que todos os códigos estão registrados como inteiros
fluxo[categ_fluxo] = fluxo[categ_fluxo].astype('category')  # após a verificação acima classifica como 'category'

# O dataset dos alunos todas as features são categorias
alunos = alunos.astype('category')

##################################################
#  CÁLCULO DO INDICADOR DE FLUXO DE CADA ESCOLA  #
##################################################

# Tratamento através de for-loops apenas por questões de showcase,
# processo muito demorado para uso em produção neste caso.
# O processo realizado através de funções groupby (como realizado
# no processamento das defasagens no bloco abaixo) são muito
# mais eficientes.

# Registro exclusivo das séries a se considerar no cálculo
series_utilizadas = [12, 9, 5]

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos.SERIE.isin(series_utilizadas) == False
mask_serie = alunos.loc[mask_serie].index
alunos = alunos.drop(mask_serie)

# Ajuste da nomenclatura
alunos['SERIE'] = alunos['SERIE'].astype('string')
alunos = alunos.replace({'SERIE': {'12': 'EM-3ª série',
                                   '9': '9º Ano EF',
                                   '5': '5º Ano EF'}},
                        regex=True)
alunos = alunos.rename(columns={'SERIE': 'SERIE_ANO', 'CD_ESCOLA': 'CODESC'})

# Ajuste do índice para eficiência do cálculo
temp = alunos.set_index(['Ano', 'CODESC', 'SERIE_ANO'])\
        .sort_values(by=['Ano', 'CODESC'])

# Computação do total de alunos e da quantidade de alunos aprovados
alunos_totais = temp.groupby(['Ano', 'CODESC', 'SERIE_ANO'])['RENDIMENTO']\
        .apply(lambda x: len(x))
alunos_totais.name = 'total'
alunos_aprovados = temp.groupby(['Ano', 'CODESC', 'SERIE_ANO'])['RENDIMENTO']\
        .apply(lambda x: (x == 1).sum())
alunos_aprovados.name = 'aprovado'


# União dos objetos acima para cálculo
relacao_alunos = pd.merge(alunos_aprovados,
                          alunos_totais,
                          how='left',
                          on=['Ano', 'CODESC', 'SERIE_ANO'])

# Cálculo efetivo do indicador de fluxo
relacao_alunos['indicador_fluxo'] = relacao_alunos['aprovado'] / relacao_alunos['total']

# Ajustando nomenclatura do objeto
indicador_fluxo = relacao_alunos.copy()

# Salvando o objeto em arquivo para posterior utilização
indicador_fluxo.to_parquet('databases/indicador_fluxo.parquet')


##########################################################
#  CÁLCULO DAS DEFASAGENS POR DISCIPLINA EM CADA ESCOLA  #
##########################################################

# Separa os níveis de proficiência de português e matemática
temp = microdados.set_index(['Ano', 'CODESC', 'SERIE_ANO']).sort_values(by=['Ano', 'CODESC'])

portugues = temp.groupby(level=[0, 1, 2])['nivel_profic_lp'].value_counts(normalize=True, dropna=False).rename('proporcao').reset_index()
matematica = temp.groupby(level=[0, 1, 2])['nivel_profic_mat'].value_counts(normalize=True, dropna=False).rename('proporcao').reset_index()


# Calcula a proporção de cada um dos níveis...
# ...para português...
abaixo_basico_port = portugues[portugues['nivel_profic_lp'] == 'Abaixo do Básico']\
        .groupby(['Ano', 'CODESC', 'SERIE_ANO'])\
        .apply(lambda x: x['proporcao'])\
        .rename('abaixo_basico')
basico_port = portugues[portugues['nivel_profic_lp'] == 'Básico']\
        .groupby(['Ano', 'CODESC', 'SERIE_ANO'])\
        .apply(lambda x: x['proporcao'])\
        .rename('basico')
adequado_port = portugues[portugues['nivel_profic_lp'] == 'Adequado']\
        .groupby(['Ano', 'CODESC', 'SERIE_ANO'])\
        .apply(lambda x: x['proporcao'])\
        .rename('adequado')

# ...e para matemática.
abaixo_basico_mat = matematica[matematica['nivel_profic_mat'] == 'Abaixo do Básico']\
        .groupby(['Ano', 'CODESC', 'SERIE_ANO'])\
        .apply(lambda x: x['proporcao'])\
        .rename('abaixo_basico')
basico_mat = matematica[matematica['nivel_profic_mat'] == 'Básico']\
        .groupby(['Ano', 'CODESC', 'SERIE_ANO'])\
        .apply(lambda x: x['proporcao'])\
        .rename('basico')
adequado_mat = matematica[matematica['nivel_profic_mat'] == 'Adequado']\
        .groupby(['Ano', 'CODESC', 'SERIE_ANO'])\
        .apply(lambda x: x['proporcao'])\
        .rename('adequado')

# União dos objetos para cálculo
prop_portugues = pd.merge(abaixo_basico_port,
                          basico_port,
                          how='right',
                          on=['Ano', 'CODESC', 'SERIE_ANO'])
prop_portugues = pd.merge(prop_portugues,
                          adequado_port,
                          how='left',
                          on=['Ano', 'CODESC', 'SERIE_ANO'])
prop_matematica = pd.merge(abaixo_basico_mat,
                           basico_mat,
                           how='right',
                           on=['Ano', 'CODESC', 'SERIE_ANO'])
prop_matematica = pd.merge(prop_matematica,
                           adequado_mat,
                           how='left',
                           on=['Ano', 'CODESC', 'SERIE_ANO'])


# Observações NaN substituídas por zero
prop_portugues = prop_portugues.fillna(0)
prop_matematica = prop_matematica.fillna(0)

# Cálculo das defasagens, de acordo com a página 4 da
# 'Nota Técnica_2019.pdf', dado por:
# defasagem = (3 * abaixo do básico) + (2 * básico) + (1 * adequado)
defasagem_portugues = (3*prop_portugues['abaixo_basico'] +
                       2*prop_portugues['basico'] +
                       prop_portugues['adequado'])
defasagem_portugues.name = 'portugues'

defasagem_matematica = (3*prop_matematica['abaixo_basico'] +
                        2*prop_matematica['basico'] +
                        prop_matematica['adequado'])
defasagem_matematica.name = 'matematica'

# União das defasagens, para posterior armazenamento em arquivo
defasagens = pd.merge(defasagem_portugues,
                      defasagem_matematica,
                      how='left',
                      on=['Ano', 'CODESC', 'SERIE_ANO'])

# O salvamento em disco propriamente dito
defasagens.to_parquet('databases/defasagens.parquet')

##########################################################
#            CÁLCULO DO INDICADOR DE DESEMPENHO          #
##########################################################
# O cálculo dos indicadores de desempenho é dado por
# indicador_desempenho = (1 - defasagem/3)*10
# (página 5 da 'Nota Técnica_2019.pdf')

# O Indicador de Desempenho (ID) da escola é dado pela média simples
# entre os desempenhos de português e matemática. Por facilidade,
# será realizado em um único dataframe.

# Criação do dataframe vazio
indicador_desempenho = pd.DataFrame()

# Indicador de Matemática
indicador_desempenho_mat = (1 - defasagem_matematica/3)*10
indicador_desempenho_mat.name = 'matematica'

# Indicador de Português
indicador_desempenho_port = (1 - defasagem_portugues/3)*10
indicador_desempenho_port.name = 'portugues'

# O ID da escola é dado pela média simples entre os dois desempenhos,
# que por facilidade será realizado em um único dataframe, unindo-se
# ambos indicadores
indicador_desempenho = pd.merge(indicador_desempenho_port,
                                indicador_desempenho_mat,
                                how='right',
                                on=['Ano', 'CODESC', 'SERIE_ANO'])


indicador_desempenho['ID_escola'] = (indicador_desempenho['portugues'] +
                                     indicador_desempenho['matematica'])/2

# Salvando sempre o banco de dados em arquivo
indicador_desempenho.to_parquet('databases/indicador_desempenho.parquet')

#############################################
#           CÁLCULO FINAL DO IDESP          #
#############################################
# Em conformidade com o contido na 'Nota Técnica_2019.pdf', o IDESP
# é calculado por idesp = indicador_desempenho * indicador_fluxo

# Criação do dataframe com as variáveis necessárias
calculo_idesp = pd.merge(indicador_desempenho,
                         indicador_fluxo,
                         how='right',
                         on=['Ano', 'CODESC', 'SERIE_ANO'])

# Dropando observações com NaN (impossibilita o cálculo) e também
# dropando variáveis já utilizadas
calculo_idesp = calculo_idesp.dropna().drop(['portugues',
                                             'matematica',
                                             'aprovado',
                                             'total'],
                                            axis=1)

calculo_idesp['idesp'] = calculo_idesp['ID_escola'] * calculo_idesp['indicador_fluxo']

idesp = calculo_idesp['idesp'].unstack()


###############################################
#  SEÇÃO PARA ANÁLISE DOS INDICADORES ANUAIS  #
###############################################

anos = ['2017', '2018', '2019', '2021', '2022']
for i in anos:
    print(f'\n\nInformações gerais sobre o ano de {i}:')
    print(idesp.loc[(i)].describe())

minimas = []
for i in anos:
    minimas.append([i, round(idesp.loc[(i)].min(), 2)])
#
medias = []
for i in anos:
    medias.append([i, round(idesp.loc[(i)].mean(), 2)])
#
maximas = []
for i in anos:
    maximas.append([i, round(idesp.loc[(i)].max(), 2)])
#
medianas = []
for i in anos:
    medianas.append([i, round(idesp.loc[(i)].median(), 2)])


########################################
#           MODELO PREDITIVO           #
########################################

# Ajusta os dados para modelagem
modelagem = idesp.unstack().T.dropna()

# Gera o modelo e já faz o fit, imprimindo os resultados no final
modelo = AutoReg(modelagem['2022'],
                 1,
                 trend='c',
                 exog=modelagem.drop('2022', axis=1))
resultado = modelo.fit()
print(resultado.summary())
saida = resultado.summary()

# Realiza o registro da saída do summary em texto
texto = open('modeloAR.txt', 'a')
texto.write(str(saida))
texto.close()

# Realização da previsão de um período adicional
previsao = modelo.predict(resultado.params,
                          exog=modelagem.drop('2017', axis=1))
previsao.name = 'previsto'

# Ajuste dos índices para posterior concatenação
previsao.index = modelagem.index

# Criação do objeto contendo os nomes das colunas
colunas = modelagem.columns.values
colunas = np.append(colunas, previsao.name)

# Concatenação dos dois datasets (base e previsto), com ajuste
# das colunas
completo = pd.concat([modelagem, previsao],
                     ignore_index=True,
                     axis=1,
                     levels=2)
completo.columns = colunas

print(completo)
