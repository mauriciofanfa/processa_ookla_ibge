# Processamento Ookla/IBGE

Este repositório contém o script `processa_ookla_ibge.py`, desenvolvido para processar dados abertos da Ookla Global Performance Maps e agregá-los por recortes territoriais brasileiros a partir da malha regional do IBGE.

O produto principal é uma coleção de séries históricas em CSV com a velocidade média de download, ponderada pelo número de dispositivos observados, para municípios, regiões geográficas imediatas, regiões geográficas intermediárias, grandes regiões, regionalização Quatro Brasis de Milton Santos e Maria Laura Silveira e Brasil.

## Apresentação

Os dados apresentam uma série histórica das médias de velocidade de download agregadas por municípios e regiões brasileiras. Podem ser utilizados em estudos de história e geografia da internet brasileira dedicados a compreender mudanças e características do período.

O presente repositório deriva de estudos elaborados e analisados inicialmente como parte da tese de doutorado apresentada ao Programa de Pós-Graduação em Comunicação da Universidade Federal de Santa Maria (UFSM, RS), intitulada *Degradês de conectividade: infraestrutura de internet e desigualdades territoriais da midiatização*, defendida por Mauricio de Souza Fanfa, em 27 de março de 2023, sob orientação da Profª. Drª. Ada Cristina Machado Silveira. Disponível em: https://repositorio.ufsm.br/handle/1/28856. Testes do presente formato de apresentação dos dados foram realizados no ano de 2022, durante a elaboração da tese, e a versão atual foi atualizada em junho de 2026.

## Licenciamento

Salvo indicação em contrário, o código-fonte, a documentação, os arquivos de apoio, os metadados e os demais produtos autorais deste repositório são licenciados sob a licença Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (`CC BY-NC-SA 4.0`). Ver `LICENSE`.

Os dados derivados produzidos pelo pipeline também devem preservar a licença `CC BY-NC-SA 4.0`, pois incorporam dados da Ookla Global Performance Maps disponibilizados sob essa licença. Ver `LICENSE-DATA.md`.

Licenças Creative Commons não são a escolha usual para software. Neste projeto, a adoção de `CC BY-NC-SA 4.0` para o código-fonte e demais materiais autorais é uma opção deliberada para manter coerência com as condições aplicáveis aos dados derivados.

## Objetivo

O script automatiza as seguintes etapas:

1. Localização ou download da malha regional do IBGE.
2. Localização ou download dos shapefiles trimestrais da Ookla.
3. Verificação básica da integridade dos shapefiles em cache.
4. Higienização das métricas numéricas da Ookla.
5. Recorte preliminar dos tiles para a extensão territorial do Brasil.
6. Conversão dos polígonos da Ookla em centroides calculados em CRS métrico.
7. Associação espacial dos centroides aos municípios da malha IBGE.
8. Cálculo de médias ponderadas por dispositivos.
9. Agregação das séries por recortes territoriais.
10. Exportação dos resultados em formatos `long` e `wide`.
11. Registro de log e manifesto com parâmetros da execução.

## Fontes e recortes territoriais

O pipeline utiliza duas fontes principais:

- Ookla Global Performance Maps: tiles trimestrais de desempenho de conexão, por tipo de conexão (`fixed` ou `mobile`).
- IBGE: malha de regiões geográficas de 2017, com municípios, regiões geográficas imediatas e regiões geográficas intermediárias.

Os dados da Ookla são mantidos em `dados/ookla`. A malha do IBGE é mantida em `dados/ibge`. Quando os arquivos necessários não são encontrados localmente, o script tenta baixá-los automaticamente.

Além dos recortes derivados diretamente da malha IBGE, o script inclui dois agrupamentos adicionais:

- Grandes regiões do IBGE, atribuídas por Unidade da Federação.
- Quatro Brasis, regionalização desenvolvida por Milton Santos e Maria Laura Silveira, operacionalizada por Unidade da Federação.

No recorte Quatro Brasis, a classificação adotada no script é:

- Região Amazônica: AC, AM, AP, PA, RO e RR.
- Região Centro-Oeste: DF, GO, MT, MS e TO.
- Região Nordeste: AL, BA, CE, MA, PB, PE, PI, RN e SE.
- Região Concentrada: ES, MG, RJ, SP, PR, SC e RS.

Antes de publicar ou redistribuir dados processados, recomenda-se verificar as condições de uso e licenciamento das fontes originais. Os dados da Ookla Global Performance Maps são disponibilizados sob a licença Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (`CC BY-NC-SA 4.0`). Por incorporarem agregações derivadas dessa fonte, os dados produzidos por este pipeline devem preservar as condições de atribuição, uso não comercial e compartilhamento pela mesma licença. Ver também `LICENSE-DATA.md`.

## Dados da Ookla

Os dados da Ookla Global Performance Maps são disponibilizados como agregações trimestrais em tiles globais. A documentação da fonte informa que os tiles são definidos no nível de zoom 16, com dimensão aproximada de 610,8 metros por 610,8 metros no Equador.

Cada tile contém métricas agregadas de testes Speedtest, incluindo:

- `avg_d_kbps`: velocidade média de download, em Kbps.
- `avg_u_kbps`: velocidade média de upload, em Kbps.
- `avg_lat_ms`: latência média, em milissegundos.
- `tests`: número de testes no tile.
- `devices`: número de dispositivos únicos.
- `quadkey`: identificador do tile.

Esta versão do pipeline utiliza `avg_d_kbps` e `devices` para calcular a velocidade média de download ponderada por dispositivos. As demais métricas permanecem nos shapefiles de origem, mas não são exportadas nos CSVs finais.

Os recortes `fixed` e `mobile` são fornecidos separadamente pela Ookla. Na documentação da fonte, `mobile` corresponde a testes com conexão celular e localização de qualidade GPS; `fixed` corresponde a testes com conexão não celular, como Wi-Fi ou Ethernet, também com localização de qualidade GPS.

A Ookla informa que os dados podem ser reagregados ao longo do tempo, inclusive por razões associadas a solicitações de privacidade e conformidade regulatória. Portanto, execuções realizadas em datas diferentes podem apresentar diferenças em contagens de tiles, testes, dispositivos e métricas agregadas.

## Regionalização do IBGE

A malha regional utilizada corresponde à Divisão Regional do Brasil em Regiões Geográficas Imediatas e Regiões Geográficas Intermediárias, publicada pelo IBGE em 2017. Essa divisão substitui o quadro anterior de mesorregiões e microrregiões geográficas e busca refletir transformações econômicas, demográficas, políticas e territoriais observadas nas décadas anteriores.

No pipeline, essa regionalização é usada como base estática de agregação. O cruzamento espacial associa cada tile da Ookla a um município, e os demais recortes territoriais são derivados dos atributos regionais presentes na malha do IBGE.

## Requisitos

Recomenda-se Python 3.10 ou superior.

Dependências principais:

```text
requests
pandas
geopandas
shapely
tqdm
```

## Uso

Execução recomendada por linha de comando:

```bash
python processa_ookla_ibge.py --tipo fixed --inicio 2019Q1 --fim 2025Q4 --workers 2
```

Parâmetros:

- `--tipo`: tipo de conexão. Valores aceitos: `fixed` ou `mobile`.
- `--inicio`: trimestre inicial, no formato `YYYYQ#`. Exemplo: `2019Q1`.
- `--fim`: trimestre final, no formato `YYYYQ#`. Exemplo: `2025Q4`.
- `--workers`: número de processos paralelos. O padrão é `1`.

O script valida o formato dos trimestres, a ordem do intervalo e a tentativa de processar trimestres futuros. Em terminais interativos, se algum dos parâmetros principais não for informado, o script solicita os valores ao usuário. Em execuções não interativas, como agendadores e pipelines automatizados, `--tipo`, `--inicio` e `--fim` são obrigatórios.

## Estrutura de saída

Cada execução cria uma pasta no formato:

```text
processa_ookla_ibge_output_YYYYMMDD_HHMM/
```

Dentro dela, os arquivos são separados por tipo de conexão:

```text
processa_ookla_ibge_output_YYYYMMDD_HHMM/
`-- fixed/
    |-- manifesto_execucao.json
    |-- serie_velocidade_brasil_consolidado_long.csv
    |-- serie_velocidade_brasil_consolidado_wide.csv
    |-- serie_velocidade_grandes_regioes_long.csv
    |-- serie_velocidade_grandes_regioes_wide.csv
    |-- serie_velocidade_municipios_long.csv
    |-- serie_velocidade_municipios_wide.csv
    |-- serie_velocidade_quatro_brasis_long.csv
    |-- serie_velocidade_quatro_brasis_wide.csv
    |-- serie_velocidade_regioes_imediatas_long.csv
    |-- serie_velocidade_regioes_imediatas_wide.csv
    |-- serie_velocidade_regioes_intermediarias_long.csv
    `-- serie_velocidade_regioes_intermediarias_wide.csv
```

Além disso, é criado um arquivo de log na raiz do projeto:

```text
processa_ookla_ibge_log_YYYYMMDD_HHMM.log
```

## Formatos dos arquivos

Todos os arquivos são gravados em UTF-8.

Cada recorte territorial é exportado em dois formatos.

### Formato long

O formato `long` contém uma linha por localidade e trimestre. Ele é mais adequado para análise estatística, bancos relacionais e ferramentas de visualização.

Exemplo de estrutura:

```text
ano_trimestre,CD_GEOCODI,NOME,sigla_uf,velocidade_download_mbps,total_dispositivos
2019Q1,1100015,Alta Floresta D'Oeste,RO,32.1,80
```

Nos arquivos `long`, o separador de campos é vírgula e o separador decimal é ponto.

### Formato wide

O formato `wide` contém uma linha por localidade e uma coluna para cada trimestre. Ele é útil para inspeção e comparação temporal direta em aplicativos como Excel ou LibreOffice Calc.

Exemplo de estrutura:

```text
CD_GEOCODI;NOME;sigla_uf;2019Q1;2019Q2;2019Q3
1100015;Alta Floresta D'Oeste;RO;32,1;35,4;38,9
```

Nos arquivos `wide`, o separador de campos é ponto e vírgula e o separador decimal é vírgula, formato conveniente para inspeção em planilhas no formato brasileiro.

## Metodologia de cálculo

Os tiles da Ookla informam, entre outras variáveis, a velocidade média de download em Kbps (`avg_d_kbps`) e o número de dispositivos (`devices`). Para cada município, o script calcula uma média ponderada:

```text
velocidade_download_mbps =
    soma(avg_d_kbps * devices) / soma(devices) / 1000
```

Após o cálculo municipal, os demais recortes territoriais são obtidos pela mesma lógica de média ponderada, preservando o peso dos dispositivos observados.

O cruzamento espacial é feito a partir dos centroides dos tiles da Ookla. Esses centroides são calculados em `EPSG:5880` e em seguida reprojetados para o CRS da malha do IBGE. A associação territorial é feita por `spatial join`, usando o predicado `within`.

O script utiliza `inner join` na consolidação final com a malha do IBGE. Assim, municípios sem dados reais da Ookla no período processado não recebem linhas artificiais com velocidade zero.

## Controle de qualidade

Ao fim da execução, o arquivo `manifesto_execucao.json` registra:

- parâmetros da execução;
- versões de Python, pandas, GeoPandas e Shapely;
- trimestres solicitados e trimestres efetivamente processados;
- número de municípios da malha IBGE;
- número de municípios com dados;
- quantidade de tiles lidos, válidos, recortados para o Brasil e associados a municípios;
- lista de arquivos CSV gerados.

Recomenda-se verificar especialmente os campos:

- `trimestres_solicitados`;
- `trimestres_processados`;
- `resumo`;
- `por_trimestre`;
- `arquivos_csv`.

Na execução de referência avaliada neste repositório (`processa_ookla_ibge_output_20260629_2124/fixed`), foram processados 28 de 28 trimestres solicitados, de `2019Q1` a `2025Q4`. O manifesto registrou:

- 5.570 municípios na malha IBGE;
- 5.569 municípios com dados em ao menos um trimestre;
- 152.290 registros municipais no formato `long`;
- 164.606.670 tiles lidos;
- 9.505.050 tiles no recorte preliminar do Brasil;
- 8.010.569 tiles associados a municípios.

## Cache e integridade dos arquivos

O script verifica se os shapefiles locais possuem os componentes básicos esperados:

```text
.shp
.dbf
.shx
.prj
```

Caso o cache da Ookla esteja incompleto, a pasta do trimestre é descartada e o download é tentado novamente. A extração de arquivos ZIP é feita com verificação de segurança para impedir escrita fora da pasta de destino.

Os arquivos em `dados/ookla` e `dados/ibge` são insumos ou cache de processamento. Para publicação dos resultados derivados, recomenda-se preservar ao menos os CSVs finais, o `manifesto_execucao.json` e o log da execução correspondente.

## Desenvolvimento

O script foi desenvolvido com foco em reprodutibilidade e processamento local. A estrutura atual é mantida em um único arquivo para facilitar inspeção, execução direta e publicação junto aos dados derivados.

Este código foi desenvolvido com auxílio parcial de ferramentas de inteligência artificial generativa. O uso de IA apoiou revisão, organização, documentação e sugestões de robustez. A curadoria, validação, execução e responsabilidade pelo uso do script e dos dados processados permanecem com os responsáveis pelo projeto.

Versões iniciais do presente trabalho foram realizadas entre 2019 e 2022 a partir de pesquisas realizadas com apoio da Coordenação de Aperfeiçoamento de Pessoal de Nível Superior - Brasil (CAPES) - Código de Financiamento 001.

## Limitações

Algumas limitações devem ser consideradas na interpretação dos resultados:

- A cobertura da Ookla depende da existência de testes e dispositivos observados em cada localidade e trimestre.
- A ausência de uma localidade em determinado trimestre não deve ser interpretada como velocidade zero.
- Registros com poucos dispositivos podem produzir valores instáveis ou extremos.
- O uso de centroides pressupõe que o ponto central do tile representa de forma suficiente sua associação municipal.
- Mudanças metodológicas, de cobertura ou de disponibilidade na fonte original podem afetar comparações temporais.
- O script exporta tabelas CSV; ele não exporta shapefiles, GeoPackages ou GeoJSONs consolidados. As tabelas podem posteriormente ser inseridas em softwares de cartografia, como QGIS, usando a base da malha do IBGE e a funcionalidade de junção por atributos.

## Referências

FANFA, Mauricio de Souza. Degradês de conectividade: infraestrutura de internet e desigualdades territoriais da midiatização. 2023. Tese (Doutorado em Comunicação) – Programa de Pós-Graduação em Comunicação, Centro de Ciências Sociais e Humanas, Universidade Federal de Santa Maria, Santa Maria, 2023. Disponível em: http://repositorio.ufsm.br/handle/1/28856. Acesso em: 29/06/2026.

FANFA, Mauricio de Souza. processa_ookla_ibge. GitHub, 2026. Disponível em: https://github.com/mauriciofanfa/processa_ookla_ibge

FANFA, Mauricio de Souza. Velocidades de conexão e latência de Internet nos municípios e regiões do Brasil: dados localizados do Speedtest by Ookla. Cambridge, MA: Harvard Dataverse, 2026. Dataset. DOI: https://doi.org/10.7910/DVN/NPQDYJ.

IBGE. Divisão regional do Brasil em regiões geográficas imediatas e regiões geográficas intermediárias, 2017. Rio de Janeiro: Instituto Brasileiro de Geografia e Estatística, 2017. Disponível em: https://www.ibge.gov.br/geociencias/organizacao-do-territorio/divisao-regional/15778-divisoes-regionais-do-brasil.html. Acesso em: 29/06/2026.

OOKLA. Speedtest by Ookla Global Fixed and Mobile Network Performance Map Tiles. Ookla, 2026. Disponível em: https://github.com/teamookla/ookla-open-data. Acesso em: 29 jun. 2026.

SANTOS, Milton; SILVEIRA, Maria Laura. O Brasil: território e sociedade no início do século XXI. Rio de Janeiro: Editora Record, 2005.
