# Pilot outputs and run-scoped logs

Use the existing repository-root `logs/` directory. Each full run creates
`logs/run_YYYYMMDD_HHMMSS/` with stage logs, `pipeline_execution.csv`,
`schema_inventory.csv`, `dataset_outputs.csv`, and `run_config.csv`.
Logs are excluded from Git; keep patient-level exceptions outside logs.

## Pilot run (RDA only; no network copies)

```r
Sys.setenv(EHR_OUTPUT_MODE = "pilot", EHR_NETWORK_SYNC = "false")
source("code/run/99_run_all.R")
```

## Default release behavior (RDA and CSV)

```r
Sys.unsetenv(c("EHR_OUTPUT_MODE", "EHR_NETWORK_SYNC"))
source("code/run/99_run_all.R")
```

## Export pilot RDAs after reviewing QC

```r
Sys.setenv(EHR_VALIDATED_RUN = "YYYYMMDD_HHMMSS")
source("code/run/98_export_csv_release.R")
```

The export-only script verifies saved RDA checksums and refuses to overwrite an
existing CSV. It does not rerun transforms or synchronize to the network.
Approval is represented by the explicit run ID; it is not automated QC approval.

## Notes
- `write_dataset()` uses `EHR_OUTPUT_MODE`; direct callers default to legacy release behavior.
- Schema inventory is captured at `write_dataset()` for outputs created with that helper. Raw-input and intermediate schema auditing remains future work.
- Other writers (such as `write_combined_rda()`) are not automatically covered by dataset output manifests.
- The existing release report runner must be present and compatible; this change does not repair missing report scripts.
- Pilot mode does not delete CSVs from earlier runs. Use the run manifest to identify outputs from the current run.
- Do not commit source paths, clinical text, IDs, or record-level exceptions.
