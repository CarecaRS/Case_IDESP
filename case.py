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

# Eliminando coluna que não aparece em nenhum outro Ano
saresp_2018.drop('Unnamed: 12', axis=1, inplace=True)

# Algumas colunas possuem nomes divergentes (maiúsculo/minúsculo),
# deixando todas no mesmo padrão
saresp_2019.columns = saresp_2018.columns
saresp_2021.columns = saresp_2018.columns
saresp_2022.columns = saresp_2018.columns
saresp_2023.columns = saresp_2018.columns

# Registro dos Anos de cada dataset para posterior agrupamento
saresp_2017['Ano'] = '2017'
saresp_2018['Ano'] = '2018'
saresp_2019['Ano'] = '2019'
saresp_2021['Ano'] = '2021'
saresp_2022['Ano'] = '2022'
saresp_2023['Ano'] = '2023'

# Agrupando todos os Anos em um único dataset
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


fluxo = pd.read_csv('databases/fluxo_completo.csv')
saresp = pd.read_csv('databases/saresp_completo.csv')
"""

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


###############################################
#  BANCOS DE DADOS PRONTOS, CÁLCULO DO IDESP  #
###############################################

# Se necessário, carrega os bancos de dados já trabalhados anteriormente:
alunos = pd.read_parquet('databases/alunos_completo.parquet')
fluxo = pd.read_parquet('databases/fluxo_completo.parquet')


# Gráfico do histórico das proficiências de cada cidade ao longo do tempo
# Registrar nível de desempenho
