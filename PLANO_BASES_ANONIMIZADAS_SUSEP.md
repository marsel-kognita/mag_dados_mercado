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
1. Definir estrutura analítica única em formato longo (recomendado):
- dimensões de tempo, ramo, cobertura/produto (quando disponível), região e demais chaves comuns;
- métricas numéricas harmonizadas.
2. Concatenar os datasets padronizados.
3. Criar um dicionário de mapeamento de variáveis entre ramos em `unified/dictionaries/`.
4. Salvar saída principal em `unified/parquet/base_susep_anonimizada.parquet`.
5. Opcional: exportar recorte em CSV para uso rápido (`unified/csv/`).

## 6. Regras de Harmonização (para evitar problemas no merge)
- não descartar colunas exclusivas de um ramo; manter com nulo para os demais;
- manter granularidade original e só agregar em camadas analíticas separadas;
- padronizar chaves temporais:
- anual: `periodo_analitico = YYYY`;
- semestral: `periodo_analitico = YYYYA` ou `YYYYB`.
- incluir coluna `fonte_susep_subpagina` para rastreabilidade (`bases_auto`, `bases_comp`, `bases_rural`).

## 7. Controle de Qualidade
Checklist mínimo por execução:
- 100% dos ZIPs esperados presentes;
- 100% dos hashes validados;
- contagem de linhas por arquivo antes/depois da padronização;
- verificação de colunas obrigatórias sem perda;
- relatório de nulos e outliers por métrica.

Gerar relatório em:
- `data/unified/quality_reports/run_YYYYMMDD_HHMM.md`

## 8. Estratégia de Atualização Incremental
- manter tabela de controle com última data de varredura por sublink;
- detectar novos arquivos comparando o manifesto atual com o histórico;
- processar apenas novos períodos;
- versionar saída final por data (`base_susep_anonimizada_YYYYMMDD.parquet`).

## 9. Entregáveis
- estrutura de pastas criada e documentada;
- manifestos de links e checksums por ramo;
- dados padronizados por ramo e por grupo (R_* / S_*);
- base unificada final (Parquet + opcional CSV);
- relatório de qualidade por execução.

## 10. Próximo Passo Prático
Implementar um script inicial para:
1. raspar os links e hashes das três subpáginas;
2. gerar automaticamente os manifestos;
3. baixar e validar checksums;
4. preparar a base para a fase de padronização e merge.
