# mag_dados_mercado

Notebook-first project for manual file ingestion and sequential processing.

## New workflow from scratch

1. Manually copy source files into `raw/data`.
2. Run notebooks in sequence from 1 to 5.
3. Collect outputs in `outputs` and intermediate files in `staging/data` and `processed/data`.

## Folder structure

- `raw/data`: manual input files (ZIP, CSV, TXT)
- `staging/data`: extracted content from ZIP files
- `processed/data`: standardized parquet/table outputs
- `outputs`: reports and final consolidated CSVs
- `notebooks`: executable sequence

## Notebook sequence

1. `notebooks/01_setup_workspace.ipynb`
  - Initializes folders and writes base config file.
2. `notebooks/02_inventory_raw_files.ipynb`
  - Inventories all files present in `raw/data`.
3. `notebooks/03_prepare_staging.ipynb`
  - Extracts ZIP files into `staging/data` and logs extraction status.
4. `notebooks/04_build_standardized_tables.ipynb`
  - Loads staged files and builds a standardized dataset.
5. `notebooks/05_consolidate_latest_periods.ipynb`
  - Selects latest 2 periods by branch and exports consolidated file.

## Environment

Use your conda environment:

```bash
conda activate mag_dados_mercado
jupyter lab
```

## Main generated files

- `outputs/pipeline_config.json`
- `outputs/raw_inventory.csv`
- `outputs/staging_extraction_report.csv`
- `processed/data/standardized_raw.parquet`
- `outputs/standardized_summary.csv`
- `outputs/latest_periods_by_branch.csv`
- `outputs/consolidated_latest_2_periods.csv`

## Notes

- This reset removed the previous Python CLI/package pipeline.
- The notebooks are now the main and only execution path.
- Business-field mapping can be refined inside notebooks 04 and 05 after validating your real source layouts.