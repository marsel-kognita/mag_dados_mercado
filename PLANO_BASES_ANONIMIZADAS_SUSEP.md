# Plano de Coleta e Consolidação - Bases Anonimizadas SUSEP

## 1. Objetivo
Construir um pipeline reprodutível para:
- baixar os arquivos das bases anonimizadas de **Automóvel**, **Compreensivo** e **Rural**;
- armazenar os arquivos em uma estrutura de pastas alinhada aos links oficiais;
- padronizar e unificar os dados em uma estrutura única para análises futuras.

## 2. Fonte e Estrutura de Referência
Página principal:
- https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas

Sublinks de trabalho:
- Automóvel (R_AUTO e S_AUTO): https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas/bases_auto
- Compreensivo (R_COMP e S_COMP): https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas/bases_comp
- Rural (R_RURAL e S_RURAL): https://www.gov.br/susep/pt-br/central-de-conteudos/dados-estatisticos/bases-anonimizadas/bases_rural

Observações relevantes da estrutura publicada:
- cada sublink possui manual próprio em PDF;
- cada sublink possui dois grupos de arquivos: **R_*** e **S_***;
- os arquivos vêm em ZIP, com hash sha256 publicado;
- a periodicidade aparente varia entre ramos (semestral em partes de Automóvel e anual nos demais).

## 3. Estrutura de Pastas (espelhando links)
Sugestão de organização local:

```text
mag_dados_mercado/
  data/
    gov_br/
      susep/
        central-de-conteudos/
          dados-estatisticos/
            bases-anonimizadas/
              bases_auto/
                metadata/
                  source_page.html
                  links_manifest.csv
                  checksums_manifest.csv
                  manual_Dados_AUTO.pdf
                raw/
                  R_AUTO/
                    2006B/
                    2007A/
                    ...
                  S_AUTO/
                    2006B/
                    2007A/
                    ...
                extracted/
                standardized/
              bases_comp/
                metadata/
                  source_page.html
                  links_manifest.csv
                  checksums_manifest.csv
                  manual_Dados_Compreensivo.pdf
                raw/
                  R_COMP/
                    2007/
                    2008/
                    ...
                  S_COMP/
                    2007/
                    2008/
                    ...
                extracted/
                standardized/
              bases_rural/
                metadata/
                  source_page.html
                  links_manifest.csv
                  checksums_manifest.csv
                  manual_Dados_Rural_Animais.pdf
                raw/
                  R_RURAL/
                    2005/
                    2006/
                    ...
                  S_RURAL/
                    2005/
                    2006/
                    ...
                extracted/
                standardized/
    unified/
      parquet/
      csv/
      dictionaries/
      quality_reports/
```

## 4. Modelo de Metadados (manifestos)
Criar um manifesto por ramo (CSV/Parquet) com as colunas:
- ramo: `auto`, `comp`, `rural`
- grupo_arquivo: `R_AUTO`, `S_AUTO`, `R_COMP`, `S_COMP`, `R_RURAL`, `S_RURAL`
- periodo_bruto: exemplo `2006B`, `2020`, `2014`
- ano: inteiro
- semestre: `A`, `B` ou nulo
- url_origem
- nome_arquivo_zip
- sha256_publicado
- sha256_calculado
- data_download_utc
- tamanho_bytes
- status_download
- status_checksum

## 5. Pipeline Proposto (ETL)

### Etapa A - Catalogação
1. Ler cada página de sublink.
2. Extrair:
- URL do manual;
- lista de arquivos ZIP R_* e S_*;
- hash sha256 publicado para cada ZIP.
3. Salvar catálogo em `metadata/links_manifest.csv` e `metadata/checksums_manifest.csv`.

### Etapa B - Download e Integridade
1. Baixar cada ZIP para `raw/<grupo_arquivo>/<periodo>/`.
2. Calcular sha256 local.
3. Comparar com hash publicado.
4. Registrar resultado no manifesto.
5. Repetir download apenas para falhas (com política de retry).

### Etapa C - Extração
1. Descompactar arquivos para `extracted/<grupo_arquivo>/<periodo>/`.
2. Preservar nome original dos arquivos internos.
3. Registrar log de extração (sucesso/erro, quantidade de arquivos).

### Etapa D - Padronização de Esquema
1. Ler layout no manual de cada base.
2. Padronizar tipos:
- datas em ISO (`YYYY-MM-DD` quando aplicável);
- códigos como string (preservar zeros à esquerda);
- valores monetários em decimal.
3. Uniformizar nomes de colunas (snake_case).
4. Adicionar colunas de linhagem:
- `ramo`;
- `grupo_arquivo`;
- `ano`;
- `semestre` (quando houver);
- `arquivo_origem`;
- `hash_arquivo_origem`.
5. Salvar versão limpa em `standardized/` (preferencialmente Parquet).

### Etapa E - Merge em Estrutura Única

> **Contexto obtido na Etapa D**: os Parquets padronizados possuem schemas distintos
> por ramo e por tipo de arquivo (R_ = riscos/prêmios, S_ = sinistros).
> O merge deve preservar essa heterogeneidade sem forçar uma tabela plana única.

#### E.1 Inventário real dos Parquets padronizados (Etapa D)

| Arquivo                       | Linhas        | Cols dados | Tamanho |
|-------------------------------|---------------|------------|---------|
| R_AUTO_2020A_standardized     |  35.628.198   | 47         | 2.395 MB|
| R_AUTO_2020B_standardized     |  35.854.022   | 47         | 2.233 MB|
| S_AUTO_2020A_standardized     |   3.139.975   | 25         |   106 MB|
| S_AUTO_2020B_standardized     |   2.454.613   | 25         |    84 MB|
| R_COMP_2022_standardized      | 144.458.633   | 16         | 2.080 MB|
| S_COMP_2022_standardized      |     911.357   | 12         |    14 MB|
| R_RURAL_2021_standardized     |  12.773.353   | 24         |   405 MB|
| S_RURAL_2021_standardized     |     158.640   | 19         |     6 MB|
| **TOTAL**                     | **235.378.791** | —        | **~7,3 GB** |

Cada Parquet já contém 5 colunas de linhagem: `ramo`, `grupo_arquivo`, `ano`, `semestre`, `arquivo_origem`.

#### E.2 Schemas por grupo de arquivo

**Colunas de R_AUTO (riscos/prêmios — 47 cols de dados):**
`cod_apo`, `cod_endosso`, `data_comp`, `cod_end`, `item`, `tipo_pes`, `modalidade`,
`tipo_prod`, `cobertura`, `cod_modelo`, `ano_modelo`, `cod_tarif`, `regiao`, `cod_cont`,
`tipo_franq`, `val_franq`, `perc_fator`, `tab_ref`, `is_casco`, `is_rcdmat`, `is_rcdc`,
`is_rcdmor`, `is_app_ma`, `is_app_ipa`, `is_app_dmh`, `pre_casco`, `pre_cas_co`,
`pre_rcdmat`, `pre_rcdc`, `pre_rcdmor`, `pre_app_ma`, `pre_app_ia`, `pre_app_dm`,
`pre_outros`, `inicio_vig`, `fim_vig`, `perc_bonus`, `clas_bonus`, `perc_corr`, `sexo`,
`data_nasc`, `tempo_hab`, `utilizacao`, `cep_util`, `cep_per`, `data_emis`, `sinal`

**Colunas de S_AUTO (sinistros — 25 cols de dados):**
`cod_apo`, `cod_endosso`, `data_comp`, `item`, `modalidade`, `tipo_prod`, `cobertura`,
`cod_modelo`, `ano_modelo`, `cod_tarif`, `regiao`, `cod_cont`, `evento`, `indeniz`,
`val_salvad`, `d_salvado`, `val_ress`, `d_ress`, `d_avi`, `d_liq`, `d_ocorr`, `causa`,
`sexo`, `d_nasc`, `cep`

**Colunas de R_COMP (riscos/prêmios — 16 cols de dados):**
`cod_apo`, `cod_endosso`, `tipo`, `classe`, `cod_end`, `item`, `cobertura`, `uf`,
`inicio_vig`, `fim_vig`, `tipo_franq`, `val_franq`, `imp_seg`, `premio`, `corretagem`,
`perc_desc`

**Colunas de S_COMP (sinistros — 12 cols de dados):**
`cod_apo`, `cod_endosso`, `tipo`, `classe`, `item`, `cobertura`, `uf`, `val_franq`,
`indeniz`, `d_aviso`, `d_liq`, `d_ocorr`

**Colunas de R_RURAL (riscos/prêmios — 24 cols de dados):**
`cod_apo`, `cod_endosso`, `cod_item`, `data_comp`, `cod_end`, `cob_fundo`, `cod_mod`,
`id_bem`, `cobertura`, `cultura`, `munic`, `uf`, `inicio_vig`, `fim_vig`, `tipo_franq`,
`val_franq`, `area_seg`, `imp_seg`, `premio`, `premio_sub`, `origem_sub`, `corretagem`,
`perc_carr`, `perc_desc`

**Colunas de S_RURAL (sinistros — 19 cols de dados):**
`cod_apo`, `cod_endosso`, `cod_item`, `data_comp`, `cob_fundo`, `cod_mod`, `id_bem`,
`cobertura`, `cultura`, `munic`, `uf`, `indeniz`, `desp_sin`, `ev_ger`, `val_franq`,
`d_aviso`, `d_liq`, `d_ocorr_in`, `d_ocorr_fi`

#### E.3 Estratégia de merge — duas camadas separadas

Dado que R_ (riscos/prêmios) e S_ (sinistros) possuem granularidades e semânticas distintas,
o merge **não** mistura R_ com S_ em uma única tabela. Em vez disso:

1. **Camada R (riscos/prêmios)**:
   - Unir R_AUTO + R_COMP + R_RURAL via `pa.unify_schemas()` (lazy).
   - Colunas ausentes em um ramo ficam `null` nos demais (ex: `cultura` só existe em RURAL).
   - Resultado: dataset lazy `riscos_unificado` com ~228,7M linhas.

2. **Camada S (sinistros)**:
   - Unir S_AUTO + S_COMP + S_RURAL via `pa.unify_schemas()` (lazy).
   - Resultado: dataset lazy `sinistros_unificado` com ~6,7M linhas.

3. **Chaves de integração R ↔ S** (conforme manuais):
   - AUTO: `cod_apo` + `cod_endosso` + `item` + `regiao`
   - COMP: `cod_apo` + `cod_endosso` + `tipo` + `classe` + `item` + `cobertura` + `uf`
   - RURAL: `cod_apo` + `cod_endosso` + `cod_item` + `cobertura` + `cob_fundo` + `cod_mod` + `cultura` + `munic` + `uf` + `id_bem`

4. **Dicionário de variáveis**: gerar `unified/dictionaries/mapeamento_colunas.json`
   contendo, para cada coluna, os ramos onde ela aparece e o tipo original.

5. **Saída**:
   - `unified/parquet/riscos/` — Parquets particionados (um por fragmento de origem).
   - `unified/parquet/sinistros/` — Parquets particionados.
   - `unified/manifestos/resumo_merge.csv` — inventário de linhas/colunas por arquivo.

#### E.4   Colunas comuns identificadas

| Coluna         | R_AUTO | S_AUTO | R_COMP | S_COMP | R_RURAL | S_RURAL | Semântica                           |
|----------------|:------:|:------:|:------:|:------:|:-------:|:-------:|-------------------------------------|
| cod_apo        |   ✓    |   ✓    |   ✓    |   ✓    |    ✓    |    ✓    | Apólice anonimizada                 |
| cod_endosso    |   ✓    |   ✓    |   ✓    |   ✓    |    ✓    |    ✓    | Endosso anonimizado                 |
| item           |   ✓    |   ✓    |   ✓    |   ✓    |    —    |    —    | Item do risco (AUTO/COMP)           |
| cod_item       |   —    |   —    |   —    |   —    |    ✓    |    ✓    | Item do risco (RURAL, nome diverso) |
| cobertura      |   ✓    |   ✓    |   ✓    |   ✓    |    ✓    |    ✓    | Código de cobertura                 |
| tipo_franq     |   ✓    |   —    |   ✓    |   —    |    ✓    |    —    | Tipo de franquia (só R_)            |
| val_franq      |   ✓    |   —    |   ✓    |   ✓    |    ✓    |    ✓    | Valor/percentual da franquia        |
| imp_seg        |   —    |   —    |   ✓    |   —    |    ✓    |    —    | Importância segurada (COMP/RURAL)   |
| premio         |   —    |   —    |   ✓    |   —    |    ✓    |    —    | Prêmio total (COMP/RURAL campo único)|
| inicio_vig     |   ✓    |   —    |   ✓    |   —    |    ✓    |    —    | Data início vigência (só R_)        |
| fim_vig        |   ✓    |   —    |   ✓    |   —    |    ✓    |    —    | Data fim vigência (só R_)           |
| indeniz        |   —    |   ✓    |   —    |   ✓    |    —    |    ✓    | Valor da indenização (só S_)        |
| uf             |   —    |   —    |   ✓    |   ✓    |    ✓    |    ✓    | UF do local do risco (COMP/RURAL)   |
| corretagem     |   —    |   —    |   ✓    |   —    |    ✓    |    —    | Comissão de corretagem (COMP/RURAL) |
| perc_desc      |   —    |   —    |   ✓    |   —    |    ✓    |    —    | Percentual de desconto (COMP/RURAL) |

> **Nota**: AUTO usa `regiao` (código SUSEP) em vez de `uf`; os prêmios e IS são
> desdobrados em múltiplas colunas por cobertura (is_casco, is_rcdmat, pre_casco, ...).

#### E.5 Implementação técnica

- **Engine**: PyArrow `dataset` (lazy) + `pa.unify_schemas()` para resolver schemas heterogêneos.
- **Nunca materializar o dataset inteiro em RAM** — usar `dataset.to_batches()` para streaming.
- **Não converter tipos nesta etapa** — manter `utf8` para todas as colunas; a conversão
  de tipos numéricos será feita na Etapa F (harmonização) com regras explícitas por campo.

---

### Etapa F - Harmonização de Variáveis

> Objetivo: preparar os datasets unificados para análise, aplicando conversões de tipo,
> padronizando chaves temporais e gerando metadados de rastreabilidade.

#### F.1 Conversão de tipos por campo

Com base nos manuais SUSEP, aplicar as seguintes conversões (a Etapa D salvou tudo como `utf8`):

**Campos inteiros (identidade anonimizada):**
- `cod_apo`, `cod_endosso`, `cod_item` → manter como `utf8` (IDs, não devem ser somados)

**Campos numéricos inteiros (códigos com semântica de lookup):**
- `cod_end`, `cod_mod`, `id_bem`, `tipo_franq`, `cod_tarif`, `tipo`, `classe`, `ev_ger`,
  `modalidade`, `tipo_prod`, `cod_cont`, `tab_ref`, `evento`, `causa`, `utilizacao`
  → manter como `utf8` (são códigos de domínio, usados em joins e filtros, não em cálculos)

**Campos numéricos monetários/percentuais (converter para `float64`):**
- R_AUTO: `val_franq`, `perc_fator`, `is_casco`, `is_rcdmat`, `is_rcdc`, `is_rcdmor`,
  `is_app_ma`, `is_app_ipa`, `is_app_dmh`, `pre_casco`, `pre_cas_co`, `pre_rcdmat`,
  `pre_rcdc`, `pre_rcdmor`, `pre_app_ma`, `pre_app_ia`, `pre_app_dm`, `pre_outros`,
  `perc_bonus`, `perc_corr`
- S_AUTO: `indeniz`, `val_salvad`, `val_ress`
- R_COMP: `val_franq`, `imp_seg`, `premio`, `corretagem`, `perc_desc`
- S_COMP: `val_franq`, `indeniz`
- R_RURAL: `val_franq`, `area_seg`, `imp_seg`, `premio`, `premio_sub`, `corretagem`,
  `perc_carr`, `perc_desc`
- S_RURAL: `indeniz`, `desp_sin`, `val_franq`

**Regra de conversão segura**: usar `pc.cast(col, pa.float64(), safe=False)` com tratamento
de valores sentinela (`.` → `null`). Aplicar `pc.if_else(pc.equal(col, "."), null, col)` antes do cast.

**Campos de data (AAAAMMDD → manter como `utf8` com validação):**
- R_AUTO/R_COMP/R_RURAL: `inicio_vig`, `fim_vig`, `data_emis`
- S_AUTO: `d_salvado`, `d_ress`, `d_avi`, `d_liq`, `d_ocorr`
- S_COMP: `d_aviso`, `d_liq`, `d_ocorr`
- S_RURAL: `d_aviso`, `d_liq`, `d_ocorr_in`, `d_ocorr_fi`
- Sentinela `00000000` = sem data → converter para `null`
- Opcionalmente converter em `pa.date32()` se upstream precisar, mas `utf8` preserva formatos originais.

**Campos de texto/caractere (manter `utf8`):**
- `item`, `tipo_pes`, `clas_bonus`, `sexo`, `cep_util`, `cep_per`, `cep`, `uf`,
  `cob_fundo`, `origem_sub`, `cod_modelo`, `ano_modelo`, `regiao`, `cultura`, `munic`,
  `sinal`, `cobertura` (AUTO é char, COMP/RURAL é numérico — ambos como `utf8`)

#### F.2 Padronização de chaves temporais

- **Periodicidade real** (extraída dos manuais):
  - AUTO: semestral (ex: `2020A`, `2020B`)
  - COMP: anual (ex: `2022`)
  - RURAL: anual (ex: `2021`)
- Criar coluna `periodo_analitico`:
  - Se `semestre` não é null: `YYYYS` (ex: `2020A`)
  - Se `semestre` é null: `YYYY` (ex: `2022`)
- Implementação vetorizada: `pc.binary_join_element_wise(ano_str, semestre, "")` com
  `pc.if_else(pc.is_valid(semestre), com_sem, sem_sem)`

#### F.3 Coluna `fonte_susep_subpagina`

Adicionar coluna derivada do `base_folder` → `auto`, `comp` ou `rural`.
Permite rastreabilidade até a subpágina de origem no portal SUSEP.

#### F.4 Tratamento especial por ramo

**AUTO — prêmios por cobertura:**
O ramo AUTO desdobra IS e prêmios em múltiplas colunas por cobertura específica:
- IS: `is_casco`, `is_rcdmat`, `is_rcdc`, `is_rcdmor`, `is_app_ma`, `is_app_ipa`, `is_app_dmh`
- Prêmios: `pre_casco`, `pre_cas_co`, `pre_rcdmat`, `pre_rcdc`, `pre_rcdmor`, `pre_app_ma`, `pre_app_ia`, `pre_app_dm`, `pre_outros`

Os demais ramos (COMP, RURAL) possuem campos únicos `imp_seg` e `premio`.
Na harmonização, **não pivotar** estas colunas — manter a estrutura wide para AUTO.
Criar coluna calculada `premio_total_auto = pre_casco + pre_rcdmat + pre_rcdc + pre_rcdmor + pre_app_ma + pre_app_ia + pre_app_dm + pre_outros` para comparabilidade com `premio` dos outros ramos.

**RURAL — campos exclusivos:**
- `cultura` (código da cultura agrícola, tabela com 43+ culturas)
- `cob_fundo` (cobertura FESR: S/N)
- `premio_sub` / `origem_sub` (subvenção governamental)
- `area_seg` (área segurada em hectares)
- `desp_sin` (despesas com sinistro, separada da indenização)
- `d_ocorr_ini` / `d_ocorr_fi` (período de ocorrência, ao invés de data única)

**COMP — tipo/classe:**
- `tipo` (1=Residencial, 2=Condominial, 3=Empresarial)
- `classe` (subclassificação: 01-04/99 por tipo)
- Competências excluídas pela SUSEP: 2005, 2006, 2010, 2012, 2013, 2023
  (documentar no dicionário de variáveis)

#### F.5 Dicionário de variáveis

Gerar `unified/dictionaries/variaveis_por_ramo.json`:
```json
{
  "auto": {
    "num_registros_R": 71482220,
    "num_registros_S": 5594588,
    "colunas_monetarias": ["val_franq", "is_casco", "pre_casco", ...],
    "colunas_codigo": ["cod_end", "modalidade", "cobertura", ...],
    "colunas_data": ["inicio_vig", "fim_vig", "data_emis"],
    "periodicidade": "semestral",
    "chaves_integracao_R_S": ["cod_apo", "cod_endosso", "item", "regiao"]
  },
  ...
}
```

Gerar `unified/dictionaries/mapeamento_colunas.json`:
```json
{
  "colunas_comuns": ["ramo", "grupo_arquivo", "ano", "semestre", "arquivo_origem",
                     "fonte_susep_subpagina", "periodo_analitico"],
  "colunas_especificas_por_ramo": {
    "auto": ["cod_modelo", "ano_modelo", "cod_tarif", "regiao", ...],
    "comp": ["tipo", "classe"],
    "rural": ["cod_item", "cob_fundo", "cultura", "munic", ...]
  }
}
```

#### F.6 Saída

- `unified/parquet/harmonizado/` — Parquets por fragmento com tipos convertidos e colunas adicionais.
- `unified/dictionaries/variaveis_por_ramo.json`
- `unified/dictionaries/mapeamento_colunas.json`
- Reabrir como `ds.dataset()` com `pa.unify_schemas()` para etapas seguintes.

---

### Etapa G - Controle de Qualidade

> Objetivo: validar a integridade, consistência e completude dos dados harmonizados
> via scanning em batches (single-pass, sem materializar em RAM).

#### G.1 Validação de completude do pipeline

| Verificação                              | Critério      |
|------------------------------------------|---------------|
| ZIPs esperados presentes                 | 100%          |
| Checksums SHA256 validados               | 100%          |
| Parquets padronizados gerados (Etapa D)  | 8 arquivos    |
| Parquets harmonizados gerados (Etapa F)  | 8 fragmentos  |
| Colunas de linhagem presentes            | 5 obrigatórias|
| Colunas adicionais de harmonização       | `fonte_susep_subpagina`, `periodo_analitico` |

#### G.2 Contagem de linhas antes/depois

Comparar `meta.num_rows` de cada Parquet padronizado (Etapa D) com a soma
correspondente no dataset harmonizado. A contagem deve ser idêntica — qualquer
diferença indica perda ou duplicação de dados.

| Ramo   | Grupo    | Esperado (Etapa D) | Efetivo (Etapa F) | Status |
|--------|----------|--------------------|--------------------|--------|
| auto   | R_AUTO   | 71.482.220         | verificar          |        |
| auto   | S_AUTO   | 5.594.588          | verificar          |        |
| comp   | R_COMP   | 144.458.633        | verificar          |        |
| comp   | S_COMP   | 911.357            | verificar          |        |
| rural  | R_RURAL  | 12.773.353         | verificar          |        |
| rural  | S_RURAL  | 158.640            | verificar          |        |

#### G.3 Validações específicas por campo (regras dos manuais)

**Domínios de código (valores permitidos):**

| Campo           | Ramo(s)      | Valores válidos                                             |
|-----------------|-------------|-------------------------------------------------------------|
| cod_end         | AUTO        | 0, 1, 2, 3, 4                                               |
| cod_end         | COMP        | 0, 1, 2, 3, 4                                               |
| cod_end         | RURAL       | 0, 1, 2, 3, 4                                               |
| modalidade      | AUTO        | 1, 2, 3, 4                                                  |
| tipo            | COMP        | 1, 2, 3                                                     |
| classe          | COMP        | 01-07, 99                                                    |
| cobertura       | AUTO        | 1, 2, 3, 4, 5, 9                                            |
| cobertura       | RURAL       | 10-340, 999 (tabela VI do manual rural)                      |
| tipo_franq      | AUTO        | 1, 2, 3, 4, 9                                               |
| tipo_franq      | RURAL       | 1, 2, 3, 4, 9                                               |
| cob_fundo       | RURAL       | S, N                                                        |
| cod_mod         | RURAL       | 10, 20, 30, 40, 50, 60, 70, 80, 90, 64                      |
| ev_ger          | RURAL       | 01-39, 99 (tabela X do manual rural)                         |
| evento          | AUTO        | 1-8 (tabela XI do manual auto)                               |
| causa           | AUTO        | 1-7, 9                                                      |
| utilizacao      | AUTO        | 0, 1, 2, 3                                                  |
| sexo            | AUTO        | M, F, 0                                                      |
| tipo_pes        | AUTO        | F, J                                                        |

**Datas (formato AAAAMMDD):**
- Verificar comprimento = 8 ou valor sentinela `00000000`.
- Validar mês (01-12), dia (01-31) quando ≠ sentinela.
- Campos: `inicio_vig`, `fim_vig`, `data_emis`, `data_nasc`, `d_avi`, `d_liq`, `d_ocorr`,
  `d_salvado`, `d_ress`, `d_aviso`, `d_ocorr_in`, `d_ocorr_fi`.

**Monetários:**
- Valores negativos são válidos em endossos de cancelamento (restituição).
- `pre_cas_co` ≤ `pre_casco` (cosseguro ≤ prêmio emitido).
- `premio_sub` ≤ `premio` em RURAL (subvenção ≤ prêmio).

#### G.4 Análise de nulos

Gerar tabela com % de nulos por coluna. Colunas provenientes de outro ramo devem ter
~100% null (ex: `cultura` será null em ramos AUTO e COMP). Colunas do próprio ramo
com null indica dado faltante na fonte.

Classificação:
- **Estrutural** (esperado): coluna exclusiva de outro ramo → 100% null no ramo ausente.
- **Sentinela**: valores como `.`, `00000000`, `999999-9`, `99999999` devem ser reportados
  separadamente como "valor sentinela" e não como null verdadeiro.
- **Faltante** (inesperado): null em coluna obrigatória do ramo → gerar alerta.

#### G.5 Estatísticas descritivas

Para colunas convertidas a `float64`:
- `min`, `max`, `mean`, `count_non_null`, `count_null`, `count_negative`
- Detectar possíveis outliers (>99.9 percentil ou <0.1 percentil).

#### G.6 Relatório de qualidade

Gerar relatório em Markdown:
- `data/unified/quality_reports/run_YYYYMMDD_HHMM.md`

Conteúdo:
1. Resumo executivo (total linhas, colunas, ramos, engine)
2. Resultado de cada validação (G.1 a G.5) com status ✓/✗
3. Tabela de nulos por coluna
4. Estatísticas monetárias
5. Lista de alertas (domínios violados, nulos inesperados)
6. Lista de arquivos gerados

---

### Etapa H - Exportação Final e Versionamento

#### H.1 Saídas Parquet

1. **Diretório harmonizado** (fonte primária):
   `unified/parquet/harmonizado/` — Parquets por fragmento, schema unificado.

2. **Cópia versionada por data**:
   `unified/parquet/base_susep_anonimizada_YYYYMMDD_HHMM/`
   — cópia completa do diretório harmonizado, para rollback.

3. **Link "latest"**:
   `unified/parquet/base_susep_anonimizada_latest/`
   — sempre aponta para a versão mais recente.

#### H.2 CSV resumo

Gerar `unified/csv/base_susep_anonimizada_resumo_YYYYMMDD_HHMM.csv` com:

| ramo | periodo_analitico | grupo_arquivo | num_registros |
|------|-------------------|---------------|---------------|

Serve como inventário rápido sem precisar ler os Parquets.

#### H.3 Metadados e dicionários

| Arquivo                                          | Descrição                            |
|--------------------------------------------------|--------------------------------------|
| `unified/manifestos/links_checksums_manifest.csv` | Catálogo de downloads + SHA256       |
| `unified/manifestos/resumo_merge.csv`             | Inventário de linhas por Parquet     |
| `unified/dictionaries/variaveis_por_ramo.json`    | Dicionário de variáveis por ramo     |
| `unified/dictionaries/mapeamento_colunas.json`    | Mapeamento de colunas entre ramos    |
| `unified/quality_reports/run_YYYYMMDD_HHMM.md`    | Relatório de qualidade da execução   |

#### H.4 Decisão técnica — por que não um único Parquet monolítico

Com ~235M linhas e schemas heterogêneos (até 54 colunas no schema unificado), um único
arquivo Parquet seria ineficiente:
- Colunas específicas de outros ramos ficariam 100% null (~70% das células do schema unificado).
- Leitura parcial seria sempre mais lenta (metadata overhead de colunas desnecessárias).
- A abordagem de **diretório de Parquets** com `pyarrow.dataset` permite:
  - Leitura lazy (só carrega colunas/fragmentos solicitados)
  - Filtro pushdown por ramo/período
  - Escrita incremental sem reprocessar todo o dataset

---

## 6. Regras de Harmonização (consolidação das Etapas E-H)

1. **Não descartar colunas exclusivas de um ramo**: manter com `null` nos demais.
2. **Manter granularidade original**: cada registro = apólice/endosso/item/cobertura (R_)
   ou sinistro individual (S_). Agregar somente em camadas analíticas separadas.
3. **Padronizar chaves temporais**:
   - Anual: `periodo_analitico = YYYY`
   - Semestral: `periodo_analitico = YYYYS` (ex: `2020A`, `2020B`)
4. **Incluir rastreabilidade**: `fonte_susep_subpagina` = `auto` | `comp` | `rural`.
5. **Não pivotar AUTO**: manter IS × cobertura em formato wide; criar campo
   `premio_total_auto` calculado para comparabilidade cross-ramo.
6. **Tratar sentinelas como null ou flag**:
   - Data `00000000` → `null`
   - Código `999999-9` (modelo FIPE inexistente) → manter como string
   - `.` em campo numérico → `null`
7. **Chaves de integração R↔S por ramo**: documentadas no dicionário de variáveis
   (seção E.3) para uso em análises de sinistralidade.

## 7. Controle de Qualidade — Checklist Mínimo

Checklist para cada execução do pipeline:
- [ ] 100% dos ZIPs esperados presentes
- [ ] 100% dos hashes SHA256 validados
- [ ] Contagem de linhas antes/depois da padronização idêntica
- [ ] 8 Parquets padronizados gerados com schemas corretos
- [ ] Colunas obrigatórias de linhagem presentes em todos os registros
- [ ] Domínios de códigos dentro dos valores esperados (tabelas dos manuais)
- [ ] Datas no formato AAAAMMDD ou sentinela 00000000
- [ ] Nulos estruturais classificados (exclusivos de outro ramo vs. faltantes)
- [ ] Estatísticas monetárias sem anomalias extremas
- [ ] Relatório de qualidade gerado em `quality_reports/`

## 8. Estratégia de Atualização Incremental

1. Manter `links_checksums_manifest.csv` como tabela de controle.
2. A cada execução, re-raspar as subpáginas SUSEP e comparar com o manifesto.
3. Novos ZIPs detectados → baixar, validar checksum, extrair e padronizar apenas os novos.
4. Re-executar Etapas E-H sobre o conjunto completo (padronizados antigos + novos).
5. Versionar saída com timestamp. Manter versões anteriores para auditoria.
6. **Periodicidade esperada de novos dados**:
   - AUTO: semestral (A = jan-jun, B = jul-dez)
   - COMP: anual (envio até 31/mar do ano seguinte)
   - RURAL: anual (envio até último dia útil de outubro)

## 9. Entregáveis

- Estrutura de pastas criada e documentada conforme seção 3.
- Manifestos de links e checksums por ramo.
- 8 Parquets padronizados por ramo/grupo/período em `standardized/`.
- Parquets harmonizados em `unified/parquet/harmonizado/` com schema unificado.
- Cópia versionada + "latest" em `unified/parquet/`.
- CSV resumo por ramo/período/grupo.
- Dicionários de variáveis e mapeamento de colunas em `unified/dictionaries/`.
- Relatório de qualidade por execução em `unified/quality_reports/`.

## 10. Decisões Técnicas — Resumo

| Decisão                                     | Justificativa                                                    |
|---------------------------------------------|------------------------------------------------------------------|
| PyArrow nativo (não Pandas)                 | 235M linhas; Pandas causa OOM com `apply(axis=1)` e `concat`    |
| Todas as colunas como `utf8` na Etapa D     | Dados SUSEP contêm sentinelas (`.`), zeros à esquerda (`000001`)|
| CSVs lidos com `delimiter=';'`              | Padrão SUSEP documentado nos manuais                             |
| Diretório de Parquets (não monolítico)      | Schemas heterogêneos; leitura lazy com filtro pushdown           |
| R_ e S_ em camadas separadas               | Granularidades e semânticas diferentes; chaves de join por ramo  |
| Não pivotar IS/prêmios de AUTO              | Estrutura wide é original e performática; campo calculado para cross-ramo |
| Conversão de tipos na Etapa F (não D)       | Separa responsabilidades; Etapa D é "fiel ao CSV", Etapa F é "analítico" |

## 11. Próximo Passo Prático

1. Executar Etapa D (notebook `ETL_Etapas_A_D.ipynb`, célula 11) para regenerar os 8 Parquets padronizados com o delimitador correto.
2. Executar Etapas E→F→G→H em sequência no notebook `ETL_Etapas_E_H.ipynb`.
3. Revisar relatório de qualidade gerado.
4. Validar dicionários de variáveis e mapeamento de colunas para análises downstream.
