# Case para análise - IDESP 2017-2022


## BACKGROUND
O Índice de Desenvolvimento da Educação do Estado de São Paulo (IDESP) é o indicador que avalia a qualidade das escolas, considerando o rendimento dos alunos em suas respectivas séries ao final do ano letivo.

O IDESP calculado utilizando-se dois critérios principais: o desempenho dos alunos nos exames de proficiência do SARESP (que tem por objetivo mensruar o quanto o aluno aprendeu) e o fluxo escolar (relação de aprovação dos alunos em determinada série/escola em relação aos matriculados na mesma série/escola). Estes dois critérios se complementam na avaliação da qualidade de ensino oferecido pela escola e permitem o acompanhamento da efetividade do ensino escolar ao longo do tempo.

Para seu cálculo, são utilizadas métricas de qualidade do ensino nas séries iniciais (5o ano do Ensino Fundamental), nas séries finais (9o ano do Ensino fundamental) e no Ensino Médio (tendo como base o 3o ano), juntamente com um Indicador de Desempenho, que avalia o nível de proficiência nas disciplinas de Língua Portuguesa e Matemática das respectivas turmas/escolas.

O Indicador de Desempenho leva em consideração um cálculo de defasagem da escola em relação às expectativas de aprendizagem de cada componente curricular, quantificadas através de proporções dos números dos alunos com nível de desempenho abaixo do básico, básico ou adequado. Dentro do cálculo de defasagem, a Nota Técnica emitida em 2019 (disponível em https://dados.educacao.sp.gov.br/sites/default/files/Nota%20tecnica_2019.pdf) prevê a utilização também da métrica relativa ao nível de desempenho tido como 'avançado' porém multiplica esse item por zero, tornando-o ignorado na equação.

Note-se que a métrica da variável 'defasagem' quanto menor, melhor, e que em momento algum os desempenhos tidos como 'avançado' sejam desconsiderados da análise, é exatamente o contrário: o desempenho avançado é o expoente ao qual o empenho deve ser empreendido. Em um mundo utópico onde todos os desempenhos sejam classificados como 'avançado' a defasagem resulta em zero, maximizando o cálculo do Indicador de Desempenho. Os desempenhos mais baixos (como 'abaixo do básico', por exemplo) possuem acertadamente pesos maiores na contabilização da defasagem, exigindo maior atenção de modo a que a métrica de defasagem melhore (fique mais baixa), impactando diretamente o Indicador de Desempenho da instituição. Maiores informações sobre os procedimentos de cálculo definidos pelo Estado de São Paulo podem ser verificados na Nota Técnica informada anteriormente.

Com o cálculo do IDESP o Estado consegue estabelecer metas de qualidade para as escolas de forma eficaz, que consideram as peculiaridades de cada instituição escolar e que podem ser utilizadas como referência para a melhoria da qualidade do ensino de acordo com o que a escola consegue atingir e do empenho necessário para realizar. As questões relacionadas ao estabelecimento de metas, índice de cumprimento destas e demais questões administrativas não são consideradas no escopo deste trabalho.

## CONSIDERAÇÕES INICIAIS

Os dados referente à situação dos alunos foram obtidos através das Matrículas por aluno, disponível em https://dados.educacao.sp.gov.br/dataset/matr%C3%ADculas-por-aluno, com acesso em 05/09/2024. Observação importante: todos os bancos de dados retornam erro quando da importação simples utilizando-se o pacote Pandas no Python, por este motivo foi usada a flag "on_bad_lines='skip'" no momento da importação para manipulação dos dados.

Nos bancos de dados em referência às Matriculas por aluno considerei apenas as classificações 0 (ativo), 2 (abandonou), 5 (não comparecimento - julguei como semelhante a abandono), 16 e 17 (reversões de abandono), 18 e 20 (não comparecimento - mesmo raciocínio do status 5), 21 (concluinte), 105 e 108 (não comparecimento - mesmo raciocínio do status 5)

Motivos das desconsiderações das demais classificações:
    • 1 (transferido): será contabilizado pela outra Escola
    • 3 (reclassificado): falta de informações sobre esse enquadramento
    • 4 (falecido): não foi desistência voluntária
    • 6, 7, 8, 9 (cessões diversas): contabilizado pelo cessionário
    • 10 (remanejamento): contabilizado pela Escola à qual foi remanejado; se porventura 'remanejado' refere-se a migrar de uma turma para outra então é equívoco deste desenvolvedor do código e o estudo necessita ser revisitado
    • 11, 12, 13, 14, 15 (cessões diversas): contabilizado pelo cessionário
    • 19 (transferido): será contabilizado pela outra instituição
    • 22 (classificação): falta de informações sobre esse enquadramento
    • 31 (transferência): contabilizado pela instituição recebedora
    • 99 (exclusão de matrícula): não necessariamente configura abandono, pode ser erro de operador de cadastros/registros

As manutenções das observações dos bancos de dados dos alunos acima (observações que ficam/que caem) são feitas a partir do "Dicionário de dados: Matrículas por Alunos", disponível no site https://dados.educacao.sp.gov.br/dicionario-de-dados-matriculas-por-alunos. Acesso em 05/09/2024.

Assim sendo, são utilizados os seguintes parâmetros de filtro em cada feature do banco de dados das matrículas dos alunos:
    • Observações com valores `NaN`: dropadas imediatamente.
    • Todas as features consideradas (`CD_ALUNO`, `CD_ESCOLA`, `RENDIMENTO`, `SERIE`, `FLAG_SIT_ALUNO`) transformadas para números inteiros.
    • `RENDIMENTO`: valores possíveis são números inteiros, sequenciais, de 1 a 8. Qualquer informação diferente é dispensada.
    • `SERIE`: *sem definição* no Dicionário. Parte-se do princípio que o maior valor informado COM QUANTIDADES DE OBSERVAÇÕES RELEVANTES seja referente ao último ano/grau possível. Logo tem-se que "12" refere-se ao 3º ano do Ensino Médio; "9" refere-se ao 9º ano do Ensino Fundamental; e "5" refere-se ao 5º ano do Ensino Fundamental. Demais informações são desconsideradas no âmbito deste trabalho.
    • `FLAG_SIT_ALUNO`: valores possíveis são números inteiros, sequenciais de 1 a 22, também números 31, 99, 105 e 118. Quaisquer outros valores são dispensados.

Os bancos de dados referentes aos dados de Proficiência no SARESP possuem erro de importação se for utilizado diretamente o arquivo .CSV de download do Portal Dados Abertos da Educação de São Paulo. Por facilidade de ajuste, os arquivos foram abertos utilizando-se programa de planilha (Excel/Libreoffice Calc/etc) com codificação adequada (UTF-8 não conseguiu interpretar alguns caracteres) e foi salvo novamente com codificação UTF-8, também como .CSV para posterior utilização no Python, sem manipulação externa.

Meu procedimento de cálculo do IDESP leva em consideração o período que o SARESP classifica como `GERAL`.

### Considerações sobre os dados de 2020
Muito embora foram realizados as limpezas e ajustes dos bancos de dados disponíveis, os Microdados de alunos - SARESP para o ano de 2020 são inexistentes, impossibilitando o devido cálculo do IDESP para o ano em questão.

### Considerações sobre os dados de 2023
As informações dos alunos disponíveis do ano de 2023 possuem a feature `RENDIMENTO` praticamente completa de valores NaN (apenas uma observação possui valor real). Assim sendo, desconsidero o ano de 2023 tanto em respeito às informações dos *alunos* (impossibilidade de calcular-se a variável 'defasagem', por exemplo, necessária para o cálculo do Indicador de Desempenho da escola e, por extensão, cálculo do IDESP) quanto às informações de *fluxo*.

### Comparação dos resultados reais IDESP com os dados calculados neste projeto
Os índices fornecidos através do site oficial do IDESP possuem divergências com os dados calculados aqui, algumas informações possuem diferenças ínfimas enquanto outras informações possuem diferenças mais significativas.

Dadas as restrições encontradas durante o desenvolver do projeto, e tendo em vista ter sido um projeto desenvolvido isoladamente (sem supervisão institucional), é esperado que algumas divergências de cálculo venham a aparecer durante o processo. Diferenças essas que podem ser prontamente ajustadas em se alterando as premissas consideradas neste projeto (por exemplo, a utilização das classificações consideradas nas Matrículas dos alunos podem ter sido equivocadas).

## Análise dos resultados obtidos
A quantidade de observações analisadas neste estudo oscilam ao longo do tempo, podendo denotar redução do número de instituições informantes (podendo se tratar de instituição que encerrou suas atividades ou simplesmente não emitiu relatório), falha na geração de arquivo público ou filtro demasiadamente incisivo por parte deste estudo no tocante às informações obtidas através dos dados do Governo de São Paulo.

Dentro do período analisado, em 2017 temos o maior número de informações consideradas em relação às séries iniciais do Ensino Fundamental (1482 observações), quantidade que vai paulatinamente se reduzindo até o ano de 2022 (com apenas 1363 observações). Já em relação às séries finais do Ensino Fundamental o maior número de observações encontra-se em 2018 (3650 observações), que reduz ao longo do tempo mas acaba retomando ao mesmo patamar em 2022. As informações do Ensino Médio são mais enxutas, com pouquíssimas observações em 2017 (apenas 62) e que atinge seu máximo em 2019 (313 observações), mas acaba reduzindo um pouco até 2022, para 246 observações.

Isto posto, é notável a melhoria da média das pontuações no IDESP ao longo do tempo independente da série escolar analisada até atingir o pico em 2019, com consequente baixa na pontuação até 2022. Necessário também notar que após 2019 passamos por um processo de pandemia mundial, o que prejudicou a extrema maioria das atividades sociais (educação incluída aqui) durante todo o período pandêmico e tendo reflexo nos anos subsequentes.

No período inicial deste estudo a média dos índices encontrava-se em 5.7 para as séries iniciais do Ensino Fundamental, 3.97 para as séries finais do Ensino Fundamental e 2.64 para o Ensino Médio. Em 2019 esses valores subiram, respecitvamente, para 5.97, 4.19 e 3.00 (com a nota do Ensino Médio atingindo sua máxima em 2021, com pontuação 3.13). Após 2019 é perceptível a redução das notas médias dos índices, atingindo suas menores pontuações em 2022 (respectivamente, 4.97, 3.90 e 2.50).

Mais importante é o comportamento da mediana das observações (mediana sendo um ponto central entre os dois extremos das observações reais). 2018 foi um ano melhor do que 2017, mas mesmo assim a mediana do IDESP ao longo dos anos não se comporta na mesma tendência da média, tendo suas notas mais altas em 2019 para as séries iniciais do Ensino Fundamental (5.91) e em 2021 para as séries finais do Ensino Fundamental (4.22) bem como também para o Ensino Mèdio (3.13).

Tão ululante quanto a melhoria da média dos índices ao longo do tempo e seu posterior declínio também é o comportamento do IDESP ao longo da vida escolar do aluno, como observado nos gráficos anteriores. Praticamente sem exceção, as notas nas séries iniciais do Ensino Fundamental são maiores do que as séries finais, que por sua vez também são maiores do que as notas dos índices ao final do Ensino Médio. Conforme a criança se desenvolve e entra em sua fase adolescente, aumentam os desafios impostos aos professores em saber lidar com essas transformações do corpo discente e tentar buscar a atração contínua e constante dos alunos em direção ao aprendizado. Quem, por consequência dessa evolução corporal e psicológica (ressalvas quanto a esta última), infelizmente nem sempre acaba tendo como o estudo sua prioridade.

As notas máximas no IDESP ao longo do período estudado não sofrem a mesma oscliação temporal em termos proporcionais, muito embora seja também perceptível a queda do Índice no decorrer dos anos letivos. Para as séries iniciais do Ensino Fundamental, temos a máxima anual mais alta em 2018 (9.43) e sua máxima mais baixa em 2022 (8.24). Nas séries finais do Ensino Fundamental, a pontuação mais alta apresenta-se também em 2018 (8.49), com a pontuação mais baixa também em 2022 (6.85). Já para a mensuração do Ensino Médio, a máxima mais alta ocorre no ano de 2021 (7.60) e a mais baixa em 2017 (5.60).

O comportamento das notas mínimas observadas neste estudo ao longo dos anos considerados segue em consonância com o comportamento das notas médias, com picos em 2019 e retração nos anos seguintes, vindo a se recuperar no último período analisado.

## Modelagem Preditiva
A presente modelagem se dá através de regressão MQO (mínimos quadrados ordinários) para um vetor autoregressivo, estimando os valores futuros do IDESP tendo como parâmetros preditores os valores passados. Parte-se da premissa que os professores sejam sempre os mesmos, que a metodologia de ensino seja mantida constante, que as metas e métricas das escolas não oscilem. Em resumo, *ceteris paribus*.

Abaixo segue o summary do modelo autoregressivo:

                            AutoReg Model Results                             
==============================================================================
Dep. Variable:                   2022   No. Observations:                 4667
Model:                   AutoReg-X(1)   Log Likelihood               -4104.501
Method:               Conditional MLE   S.D. of innovations              0.583
Date:                Sat, 07 Sep 2024   AIC                           8223.002
Time:                        16:01:42   BIC                           8268.139
Sample:                             1   HQIC                          8238.878
                                 4667                                         
==============================================================================
#		 coef    std err          z      P>|z|      [0.025      0.975]
------------------------------------------------------------------------------
const          0.7983      0.053     14.937      0.000       0.694       0.903
2022.L1        0.0398      0.011      3.528      0.000       0.018       0.062
2017           0.1230      0.013      9.139      0.000       0.097       0.149
2018           0.1004      0.014      7.153      0.000       0.073       0.128
2019           0.2410      0.014     17.685      0.000       0.214       0.268
2021           0.2399      0.013     18.459      0.000       0.214       0.265
                                    Roots                                    
=============================================================================
#		  Real          Imaginary           Modulus         Frequency
-----------------------------------------------------------------------------
AR.1           25.1141           +0.0000j           25.1141            0.0000
-----------------------------------------------------------------------------

Observa-se nesse modelo os p-values de cada um dos estimadores (os anos anteriores) sendo estatisticamente significantes para utilização. Os critérios de análise AIC (Akaike Information Critorion), BIC (Bayesian Information Criterion) e HQIC (Hannah-Quinn Information Criterion) são utilizados para comparação entre modelos, não tendo valor de referência em análise isolada. Via de regra, em dois modelos distintos, aquele com menores valores desses critérios é preferível.
	A previsão obtida com o modelo segue em consonância estatística com as demais observações levantadas durante o período observado, verificada no gráfico abaixo:



## Questionário Contextual do SARESP

Anualmente os alunos e seus pais recebem um formulário contendo um questionário contendo perguntas que permitem inferir opiniões sobre a escola como um todo (infraestrutura, posição dos professores, relações entre alunos e professores / alunos e alunos), hábitos de estudo dos alunos, presença dos pais na vida estudantil dos filhos, dados que também permitem estimar nível sócio-econômico das famílias e condições de estudo dos alunos em casa (inclusive comportamento durante a pandemia). Os últimos dados divulgados são do ano de 2021, disponíveis em https://dados.educacao.sp.gov.br/dataset/question%C3%A1rios-saresp (acesso em 05/09/2024).

Algumas realidades mais duras podem fugir da influência da escola em proporcionar um aproveitamento ótimo do conhecimento por parte dos seus alunos, porém há tempos existem estudos que indicam a manutenção da criança na escola como facilitador ao aprendizado. Elaboração de atividades extra-classe, como escolinhas de esportes (futebol, basquete, vôlei...), manutenção das dependências das bibliotecas (se existentes), criação de grupos de estudo de reforço, são exemplos de atividades que podem ser desempenhadas pelas instituições de modo a reter os alunos em escola, promovendo melhorias no aprendizado do aluno ao mesmo tempo que evita que a criança se exponha a atividades não-educacionais ásperas (drogas, bebidas, crime, etc.).

O questionário entregue aos alunos peca em não mensurar níveis de dificuldade que os alunos possam estar enfrentando nas disciplinas, apenas contempla informações abrangentes sobre hábitos de estudo e sentimentos em relação às matérias ("gosta de estudar?", "faz a lição de casa?"). A inserção de um único questionário likert por disciplina avaliando níveis subjetivos de dificuldade enfrentada pelos alunos já pode dar um norte sobre o contexto geral da sala de aula: se a ocorrência de dificuldades é pequena, o corpo doscente pode envidar esforços na recuparação desses estudantes específicos; se a ocorrência de dificuldades é generalizada a instituição deve contactar o professor responsável questionando metodologia de ensino, nível de dificuldade dos conteúdos, empenho do profissional ao dar aula e demais pontos no que tange ministrar as aulas. Em extremo, se período após período o mesmo professor recebe ocorrências generalizadas de dificuldades por parte dos alunos a instituição pode pensar em remanejar o profissional em busca de outro que se adapte melhor à realidade da turma.

No mesmo sentido, um questionário ao aluno que contemple pontos como, por exemplo, 'o que eu mais gosto / menos gosto aula x? jeito do professor ensinar / facilidade de aprendizagem / professor solícito quanto às dúvidas da turma / etc' poderia auxiliar a instituição em como orientar seus profissionais visando a melhor transmissão de conhecimento para seus alunos.
