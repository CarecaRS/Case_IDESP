# Sesi / Senai

## Case para análise - Cientista de Dados

Anotações diversas conforme eu vou trabalhando no banco de dados, depois eu estruturo isso bem bonitinho:

Os dados referente à situação dos alunos foram obtidos através das Matrículas por aluno, disponível em https://dados.educacao.sp.gov.br/dataset/matr%C3%ADculas-por-aluno. Acesso em 05/09/2024. Obs.: todos os bancos de dados retornam erro quando da importação simples utilizando-se o pacote Pandas, por este motivo foi usada a flag "on_bad_lines='skip'" no momento da importação para manuseio.

As manutenções das observações dos bancos de dados dos alunos (quem fica/quem dropa) são feitas a partir do "Dicionário de dados: Matrículas por Alunos", disponível no site https://dados.educacao.sp.gov.br/dicionario-de-dados-matriculas-por-alunos. Acesso em 05/09/2024.

Assim sendo, são utilizados os seguintes parâmetros de filtro em cada feature:
- Observações com valores `NaN`: dropadas imediatamente.
- Todas as features consideradas (`CD_ALUNO`, `CD_ESCOLA`, `RENDIMENTO`, `SERIE`, `FLAG_SIT_ALUNO`) transformadas para números inteiros.
- `RENDIMENTO`: valores possíveis são números inteiros, sequenciais, de 1 a 8. Qualquer informação diferente é dispensada.
- `SERIE`: *sem definição* no Dicionário. Parte-se do princípio que o maior valor informado (14) seja referente ao último ano/grau possível. Logo tem-se que "14" refere-se ao 3º ano do Ensino Médio; "11" refere-se ao 9º ano do Ensino Fundamental; e "7" refere-se ao 5º ano do Ensino Fundamental. Utilizadas duas defasagens de cada `SERIE` em vista ao modelo preditivo.
- `FLAG_SIT_ALUNO`: valores possíveis são números inteiros, sequenciais de 1 a 22, também números 31, 99, 105 e 118. Quaisquer outros valores são dispensados.

IMPORTANTE: As informações dos alunos disponíveis do ano de 2023 possuem a feature `RENDIMENTO` praticamente completa de valores NaN (apenas uma observação possui valor real). Assim sendo, desconsidero o ano de 2023 tanto em respeito às informações dos *alunos* (impossibilidade de calcular-se a variável 'defasagem', por exemplo, necessária para o cálculo do Indicador de Desempenho da escola e, por extensão, cálculo do IDESP) quanto às informações de *fluxo*.
