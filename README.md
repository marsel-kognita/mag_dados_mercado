# mag_dados_mercado

Pipeline ETL para ingestão, padronização, harmonização e sumarização das **Bases Anonimizadas da SUSEP** (auto, compreensivo e rural).

## Visão geral

O projeto processa ~235 M de linhas de dados de seguros publicados pela SUSEP, passando por 8 etapas (A–H) de ETL e uma etapa final de sumarização geográfica.

## Estrutura de pastas

```
.
├── ETL_Etapas_A_D.ipynb              # Etapas A–D: download, extração, padronização, metadados
├── ETL_Etapas_E_H.ipynb              # Etapas E–H: merge, harmonização, qualidade, exportação
├── Sumarizacao_Municipios.ipynb      # Sumarização geográfica (cep_per / uf / município)
├── PLANO_BASES_ANONIMIZADAS_SUSEP.md # Plano detalhado do pipeline
├── requirements.txt
├── README.md
├── data/
│   ├── gov_br/susep/.../bases-anonimizadas/   # Dados brutos por ramo
│   │   ├── bases_auto/   (raw/ extracted/ standardized/ metadata/)
│   │   ├── bases_comp/
│   │   └── bases_rural/
│   └── unified/                                # Saída consolidada
│       ├── parquet/
│       │   ├── harmonizado_riscos/    # Parquets harmonizados R_AUTO, R_COMP, R_RURAL
│       │   └── harmonizado_sinistros/ # Parquets harmonizados S_AUTO, S_COMP, S_RURAL
│       ├── csv/                       # CSVs de sumarização
│       ├── dictionaries/
│       ├── manifestos/
│       └── quality_reports/
```

## Notebooks — ordem de execução

| # | Notebook | Descrição |
|---|----------|-----------|
| 1 | `ETL_Etapas_A_D.ipynb` | **A** Download dos ZIPs da SUSEP · **B** Extração de CSVs · **C** Padronização (encoding, separador, tipos) · **D** Geração de metadados e manifestos |
| 2 | `ETL_Etapas_E_H.ipynb` | **E** Merge dos CSVs padronizados em Parquet unificado · **F** Harmonização de schemas (tipos, nomes) · **G** Controle de qualidade · **H** Exportação final |
| 3 | `Sumarizacao_Municipios.ipynb` | Merge R↔S por ramo via chaves de integração, agregação geográfica (auto→`cep_per`, comp→`uf`, rural→`munic`) e cálculo de sinistralidade |

## Stack técnica

| Componente | Uso |
|------------|-----|
| **PyArrow** | ETL streaming (~235 M linhas) — Etapas A–H |
| **Polars** (lazy) | Sumarização — `scan_parquet` → `group_by` → `collect` sem materializar tudo em RAM |
| **Pandas** | Exibição de tabelas finais já agregadas (poucos milhares de linhas) |

## Ambiente

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter lab
```

## Saídas principais

- `data/unified/parquet/harmonizado_riscos/*.parquet` — riscos harmonizados por ramo
- `data/unified/parquet/harmonizado_sinistros/*.parquet` — sinistros harmonizados por ramo
- `data/unified/csv/sumarizacao_auto_por_cep_per.csv` — prêmios e sinistros por CEP de pernoite
- `data/unified/csv/sumarizacao_comp_por_uf.csv` — prêmios e sinistros por UF
- `data/unified/csv/sumarizacao_rural_por_munic.csv` — prêmios e sinistros por município
- `data/unified/manifestos/` — checksums e resumos de merge
- `data/unified/quality_reports/` — relatórios de qualidade