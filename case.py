# Importação dos pacotes necessários
import time  # apenas para que eu possa mensurar eficiência do processamento
import numpy as np
import pandas as pd
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
serie_possivel = [14, 13, 12, 11, 10, 9, 8, 7, 6, 5]
rendimento_possivel = [1, 2, 3, 4, 5, 6, 7, 8]
situacao_possivel = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                     11, 12, 13, 14, 15, 16, 17, 18, 19,
                     20, 21, 22, 31, 99, 105, 118]
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
(alunos_2017.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # NÃO possui registros inválidos

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2017.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2017.loc[mask_rend].index
alunos_2017 = alunos_2017.drop(mask_rend)

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2017.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2017.loc[mask_serie].index
alunos_2017 = alunos_2017.drop(mask_serie)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2017['Ano'] = '2017'
alunos_2017 = alunos_2017.reset_index(drop=True)

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
(alunos_2018.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registro inválido, mas já cai fora na limpeza anterior

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2018.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2018.loc[mask_serie].index
alunos_2018 = alunos_2018.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2018.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2018.loc[mask_rend].index
alunos_2018 = alunos_2018.drop(mask_rend)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2018['Ano'] = '2018'
alunos_2018 = alunos_2018.reset_index(drop=True)

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
(alunos_2020.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos, mas já cai fora nas limpezas das outras features

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2020.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2020.loc[mask_serie].index
alunos_2020 = alunos_2020.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2020.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2020.loc[mask_rend].index
alunos_2020 = alunos_2020.drop(mask_rend)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2020['Ano'] = '2020'
alunos_2020 = alunos_2020.reset_index(drop=True)

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
(alunos_2021.FLAG_SIT_ALUNO.isin(situacao_possivel) == False).sum()  # possui registros inválidos, mas já cai fora nas limpezas das outras features

# Limpeza das observações ainda inválidas em 'SERIE'
mask_serie = alunos_2021.SERIE.isin(serie_possivel) == False
mask_serie = alunos_2021.loc[mask_serie].index
alunos_2021 = alunos_2021.drop(mask_serie)

# Limpeza das observações inválidas em 'RENDIMENTO'
mask_rend = alunos_2021.RENDIMENTO.isin(rendimento_possivel) == False
mask_rend = alunos_2021.loc[mask_rend].index
alunos_2021 = alunos_2021.drop(mask_rend)

# Marcação do Ano do dataset e refazimento do índice após a limpeza
alunos_2021['Ano'] = '2021'
alunos_2021 = alunos_2021.reset_index(drop=True)

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


##############################################
#  MANIPULAÇÃO DOS BANCOS DE DADOS DE FLUXO  #
##############################################
# CD_TP_IDENTIFICADOR: 8 é escola estadual

###
# Fluxo de 2017
###
# Carrega o dataset
fluxo_2017 = pd.read_csv('databases/fluxo_2017.csv')

# Define as features a serem mantidas
colunas_nao = ['NM_DIRETORIA', 'CD_REDE_ENSINO', 'NM_COMPLETO_ESCOLA']
colunas = list(fluxo_2017.columns[fluxo_2017.columns.isin(colunas_nao) == False])

# Filtra para o df final e verifica valores NaN
fluxo_2017 = fluxo_2017[colunas]
fluxo_2017.isnull().sum()  # 0, ótimo: sem precisar mexer mais
fluxo_2017.to_parquet('databases/fluxo_2017.parquet')  # salva o dataset em arquivo


###
# Fluxo de 2018
###
# Carrega o dataset
fluxo_2018 = pd.read_csv('databases/fluxo_2018.csv', sep=';')

# Define as features a serem mantidas
colunas_nao = ['NM_DIRETORIA', 'CD_REDE_ENSINO', 'NM_COMPLETO_ESCOLA']
colunas = list(fluxo_2018.columns[fluxo_2018.columns.isin(colunas_nao) == False])

# Filtra para o df final e verifica valores NaN
fluxo_2018 = fluxo_2018[colunas]
fluxo_2018.isnull().sum()  # 0, ótimo: sem precisar mexer mais
fluxo_2018 = fluxo_2018.rename(columns={'ANO': 'Ano'})  # transforma o nome da feature 'ANO' igual ao padrão de 2017 ('Ano')
fluxo_2018.to_parquet('databases/fluxo_2018.parquet')  # salva o dataset em arquivo


###
# Fluxo de 2019
###
# Carrega o dataset
fluxo_2019 = pd.read_csv('databases/fluxo_2019.csv')

# Define as features a serem mantidas
colunas_nao = ['NM_DIRETORIA', 'CD_REDE_ENSINO', 'NM_COMPLETO_ESCOLA']
colunas = list(fluxo_2019.columns[fluxo_2019.columns.isin(colunas_nao) == False])

# Filtra para o df final e verifica valores NaN
fluxo_2019 = fluxo_2019[colunas]
fluxo_2019.isnull().sum()  # 0, ótimo: sem precisar mexer mais
fluxo_2019 = fluxo_2019.rename(columns={'ANO': 'Ano'})  # transforma o nome da feature 'ANO' igual ao padrão de 2017 ('Ano')
fluxo_2019.to_parquet('databases/fluxo_2019.parquet')  # salva o dataset em arquivo


###
# Fluxo de 2020
###
# Carrega o dataset
fluxo_2020 = pd.read_csv('databases/fluxo_2020.csv')

# Define as features a serem mantidas
colunas_nao = ['NM_DIRETORIA', 'CD_REDE_ENSINO', 'NM_COMPLETO_ESCOLA']
colunas = list(fluxo_2020.columns[fluxo_2020.columns.isin(colunas_nao) == False])

# Filtra para o df final e verifica valores NaN
fluxo_2020 = fluxo_2020[colunas]
fluxo_2020.isnull().sum()  # 0, ótimo: sem precisar mexer mais
fluxo_2020 = fluxo_2020.rename(columns={'ANO_LETIVO': 'Ano'})  # transforma o nome da feature 'ANO_LETIVO' igual ao padrão de 2017 ('Ano')
fluxo_2020.to_parquet('databases/fluxo_2020.parquet')  # salva o dataset em arquivo


###
# Fluxo de 2021
###
# Carrega o dataset. Obs.: o arquivo original do download
# resulta em erro na hora de carregar. Abre o arquivo com aplicativo
# de planilhas (Excel, Libreoffice Calc, etc.) e salva novamente como CSV
# sem precisar alterar nada, só abrir e salvar: ajusta o erro automaticamente.
fluxo_2021 = pd.read_csv('databases/fluxo_2021.csv')

# O arquivo de 2021 possui alguns registros incongruentes com os demais
# datasets, o código abaixo limpa essas features indevidas
fluxo_2021 = fluxo_2021[fluxo_2021.columns.drop(list(fluxo_2021.filter(regex='Unnamed')))]

# Define as features a serem mantidas
colunas_nao = ['NM_DIRETORIA', 'CD_REDE_ENSINO', 'NM_COMPLETO_ESCOLA']
colunas = list(fluxo_2021.columns[fluxo_2021.columns.isin(colunas_nao) == False])

# O banco de dados de 2021 possui uma ocorrência de código identificador
# errado, dropando aqui:
indevido = fluxo_2021[fluxo_2021['CD_TP_IDENTIFICADOR'] == ' PROF'].index
fluxo_2021 = fluxo_2021.drop(indevido)

# Filtra para o df final e verifica valores NaN
fluxo_2021 = fluxo_2021[colunas]
fluxo_2021.isnull().sum()  # 0, ótimo: sem precisar mexer mais
fluxo_2021 = fluxo_2021.rename(columns={'ANO_LETIVO': 'Ano'})  # transforma o nome da feature 'ANO_LETIVO' igual ao padrão de 2017 ('Ano')
fluxo_2021.to_parquet('databases/fluxo_2021.parquet')  # salva o dataset em arquivo


###
# Fluxo de 2022
###
# Carrega o dataset. Obs.: o arquivo original do download
# resulta em erro na hora de carregar. Abre o arquivo com aplicativo
# de planilhas (Excel, Libreoffice Calc, etc.) e salva novamente como CSV
# sem precisar alterar nada, só abrir e salvar: ajusta o erro automaticamente.
fluxo_2022 = pd.read_csv('databases/fluxo_2022.csv')

# O arquivo de 2022 também possui alguns registros incongruentes com os
# bancos de dados, utilizando o mesmo código para limpeza
fluxo_2022 = fluxo_2022[fluxo_2022.columns.drop(list(fluxo_2022.filter(regex='Unnamed')))]

# Define as features a serem mantidas
colunas_nao = ['NM_DIRETORIA', 'CD_REDE_ENSINO', 'NM_COMPLETO_ESCOLA', 'CD_DIRETORIA']
colunas = list(fluxo_2022.columns[fluxo_2022.columns.isin(colunas_nao) == False])

# O banco de dados de 2022 possui duas ocorrências de código identificador
# errado, dropando aqui:
indevido = fluxo_2022[(fluxo_2022['CD_TP_IDENTIFICADOR'] == ' PROFESSOR') | (fluxo_2022['CD_TP_IDENTIFICADOR'] == ' PROFESSORA')].index
fluxo_2022 = fluxo_2022.drop(indevido)

# Filtra para o df final e verifica valores NaN
fluxo_2022 = fluxo_2022[colunas]
fluxo_2022.isnull().sum()  # 0, ótimo: sem precisar mexer mais
fluxo_2022 = fluxo_2022.rename(columns={'ANO_LETIVO': 'Ano'})  # transforma o nome da feature 'ANO_LETIVO' igual ao padrão de 2017 ('Ano')
fluxo_2022.to_parquet('databases/fluxo_2022.parquet')  # salva o dataset em arquivo


###
# União dos datasets de fluxo todos em um único arquivo
###
fluxo = pd.concat([fluxo_2017, fluxo_2018, fluxo_2019,
                   fluxo_2020, fluxo_2021, fluxo_2022],
                  ignore_index=True)

# Ajuste dos dtypes das features do dataset
# Ano, município, código da escola e código de tipo identificador são categorias
inteiros = ['Ano', 'CD_ESCOLA', 'CD_TP_IDENTIFICADOR']
categorias = ['Ano', 'NM_MUNICIPIO', 'CD_ESCOLA', 'CD_TP_IDENTIFICADOR']
fluxo[inteiros] = fluxo[inteiros].astype('int')  # certifica que todos os códigos estão registrados como inteiros
fluxo[categorias] = fluxo[categorias].astype('category')  # após a verificação acima classifica como 'category'

# As demais features são todas float, necessário alterar ',' por '.'
# Uso como filtro as features de categorias utilizadas acima
colunas_float = list(fluxo_2022.columns[fluxo_2022.columns.isin(categorias) == False])
for col in fluxo[colunas_float].columns:
    fluxo = fluxo.replace({col: {',': '.'}}, regex=True)
    fluxo[col] = fluxo[col].astype(float)

# Salvando o banco de dados agrupado para utilização futura
fluxo.to_parquet('databases/fluxo_completo.parquet')


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
fluxo = pd.read_parquet('databases/fluxo_completo.parquet')
saresp = pd.read_parquet('databases/saresp_completo.parquet')

alunos = pd.read_parquet('databases/alunos_2017.parquet')
fluxo = pd.read_parquet('databases/fluxo_2017.parquet')
saresp = pd.read_parquet('databases/saresp_completo.parquet')

# Se os datasets forem carregados novamente é necessário refazer
# o ajuste dos dtypes. Ano, município, código da escola e código
# de tipo identificador são categorias:
int_fluxo = ['Ano', 'CD_ESCOLA', 'CD_TP_IDENTIFICADOR']
categ_fluxo = ['Ano', 'NM_MUNICIPIO', 'CD_ESCOLA', 'CD_TP_IDENTIFICADOR']
fluxo[int_fluxo] = fluxo[int_fluxo].astype('int')  # certifica que todos os códigos estão registrados como inteiros
fluxo[categ_fluxo] = fluxo[categ_fluxo].astype('category')  # após a verificação acima classifica como 'category'

# O dataset dos alunos todas as features são categorias
alunos = alunos.astype('category')

# [1] Cálculo do IDESP de cada série (5o EF, 9o EF, 3o EM) dá-se por...
idesp = indicador_desempenho * indicador_fluxo

# [2] ...onde...
indicador_desempenho = (1 - defasagem/3)*10

# [3] ...e tendo...
defasagem = 3*abaixo_basico + 2*basico + adequado

# [4] ...sendo que cada definição (básico, abaixo, adequado) é calculada por
abaixo_basico = alunos_abaixo_basico/total_alunos
basico = alunos_basico/total_alunos
adequado = alunos_adequado/total_alunos

# [5] Para o cálculo do IDESP utiliza-se o ID da escola em cada etapa da
# escolarização, sendo
id_escola = (nota_portugues + nota_matemática)/2

# [6] Finalmente, o indicador de fluxo é tido como
indicador_fluxo = alunos_aprovados / alunos_matriculados


##################################################
#  CÁLCULO DO INDICADOR DE FLUXO DE CADA ESCOLA  #
##################################################

# Ano em consideração
periodo = ['2017', '2018', '2019', '2020', '2021', '2022']
inicio_full = time.time()
indicador_fluxo = []
for i in periodo:
    alunos = pd.read_parquet(f'databases/alunos_{i}.parquet')
    ano_temp = i
    escolas = list(alunos['CD_ESCOLA'].unique())
    inicio = time.time()
    for esc in escolas:
        mask_ap = (alunos['CD_ESCOLA'] == esc) & (alunos['Ano'] == ano_temp) & (alunos['RENDIMENTO'] == 1)
        mask_tot = (alunos['CD_ESCOLA'] == esc) & (alunos['Ano'] == ano_temp)
        ind_fluxo_temp = len(alunos[mask_ap])/len(alunos[mask_tot])
        indicador_fluxo.append([ano_temp, esc, ind_fluxo_temp])
        print(f'Processada escola {esc} no ano {ano_temp}, com fluxo {ind_fluxo_temp}')
    fim = time.time()
    print(f'Tempo para o cálculo do ano de {ano_temp}: {round((fim - inicio)/60, 2)} minutos')
fim_full = time.time()
print(f'Tempo para o cálculo de todos os anos: {round((fim_full - inicio_full)/60, 2)} minutos')


# Ver se a variável alunos consegue ser enxugada ainda mais,
# até acho que sim, se manter apenas CD_ESCOLA, Ano e RENDIMENTO
alunos.columns

if_df = pd.DataFrame(indicador_fluxo, columns={'Ano': 0,
                                               'CD_ESCOLA': 1,
                                               'indicador_fluxo': 2})

escolas[0:5]

esc = escolas[0]

alunos_matriculados = alunos[alunos[esc]].sum()
alunos_aprovados = (alunos[alunos[esc] == escolas[0]].RENDIMENTO == 1).sum()


len(alunos[mask_ap])
len(alunos[mask_tot])


escola
aluno


for yr in ano:
    print(yr)

escolas
escolas_temp
alunos[(alunos.CD_ESCOLA == escolas_2017[0]) & (alunos.Ano == ano)]

escolas_temp = list(alunos[alunos.Ano == ano]['CD_ESCOLA'].unique())
escolas_2017


alunos_matriculados = alunos[alunos.CD_ESCOLA == escolas[0]].sum()
alunos_aprovados = (alunos[alunos.CD_ESCOLA == escolas[0]].RENDIMENTO == 1).sum()

for escola in escolas:
    print(f'Média escola código {escola}: {saresp[saresp[saresp.periodo == 'GERAL'][escola]].medprof.mean()}')

mask = saresp[saresp.periodo == 'GERAL'].index
print(f'Média escola código {escolas[0]}: {saresp[saresp[saresp.periodo == 'GERAL'][escolas[0]]].medprof.mean()}')

saresp[saresp[saresp.periodo == 'GERAL'].CODESC == escolas[0]]

saresp.loc[mask][saresp['CODESC'] == escolas[15]].groupby('SERIE_ANO')['medprof'].mean()

saresp.loc[mask][saresp['CODESC'] == escolas[5]][['Ano', 'CODESC', 'SERIE_ANO', 'ds_comp']]

saresp.loc[mask]

id_escola = ()


fluxo[fluxo.CD_ESCOLA == 12]
fluxo


saresp[saresp.periodo == 'GERAL']

saresp.head()
alunos.head()
fluxo.head()

#
# Gráfico do histórico das proficiências de cada cidade ao longo do tempo
# Registrar nível de desempenho
for ano in alunos.Ano.unique():
    mask = alunos.Ano == ano
