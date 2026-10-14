# Test results

- Run start (UTC): 2026-10-14T19:53:05Z
- Duration: 23.8 s
- Commit: `5a96903c7bb1f24fed62a87a83af5f5ad9262e11 (dirty tree)`
- Result: **PASS** (pytest exit code 0)
- Totals: 56 tests, 56 passed, 0 failed, 0 errors, 0 skipped

| Test | Outcome | Time (s) |
|---|---|---|
| tests.test_config::test_salt_unset_uses_default | passed | 0.00 |
| tests.test_config::test_salt_from_env | passed | 0.00 |
| tests.test_config::test_blank_or_padded_salt_fails_naming_variable_not_value[] | passed | 0.00 |
| tests.test_config::test_blank_or_padded_salt_fails_naming_variable_not_value[   ] | passed | 0.00 |
| tests.test_config::test_blank_or_padded_salt_fails_naming_variable_not_value[synthetic-salt-value\\n] | passed | 0.00 |
| tests.test_config::test_blank_or_padded_salt_fails_naming_variable_not_value[ synthetic-salt-value] | passed | 0.00 |
| tests.test_config::test_blank_or_padded_salt_fails_naming_variable_not_value[synthetic-salt-value\\t] | passed | 0.00 |
| tests.test_config::test_missing_data_root_fails_naming_variable[None] | passed | 0.00 |
| tests.test_config::test_missing_data_root_fails_naming_variable[] | passed | 0.00 |
| tests.test_config::test_missing_data_root_fails_naming_variable[   ] | passed | 0.00 |
| tests.test_config::test_repr_masks_salt | passed | 0.00 |
| tests.test_config::test_conf_is_frozen | passed | 0.00 |
| tests.test_config::test_root_stored_without_trailing_slash | passed | 0.00 |
| tests.test_config::test_cli_fails_fast_without_spark[None] | passed | 0.00 |
| tests.test_config::test_cli_fails_fast_without_spark[] | passed | 0.00 |
| tests.test_config::test_cli_fails_fast_without_spark[  ] | passed | 0.00 |
| tests.test_config::test_cli_has_no_emr_target_in_story_1 | passed | 0.00 |
| tests.test_config::test_cli_success_path_runs_silver_then_gold | passed | 0.00 |
| tests.test_config::test_job_entry_fails_fast_without_data_root[silver] | passed | 0.05 |
| tests.test_config::test_job_entry_fails_fast_without_data_root[gold] | passed | 0.05 |
| tests.test_config::test_no_bronze_job_module | passed | 0.00 |
| tests.test_end_to_end::test_local_run_end_to_end | passed | 6.84 |
| tests.test_end_to_end::test_rerun_is_idempotent | passed | 2.70 |
| tests.test_end_to_end::test_gold_file_is_sorted | passed | 1.36 |
| tests.test_end_to_end::test_reordered_bronze_header_is_rejected | passed | 0.07 |
| tests.test_gold::test_gold_from_expected_silver | passed | 1.14 |
| tests.test_gold::test_duration_not_multiplied_by_labs | passed | 1.02 |
| tests.test_gold::test_null_patient_counts_encounter_not_patient | passed | 0.92 |
| tests.test_gold::test_only_cancelled_day_has_no_row_and_null_status_counts | passed | 0.94 |
| tests.test_gold::test_lab_counted_on_encounter_day_and_customer_scoped | passed | 0.93 |
| tests.test_gold::test_order_gold_sorts_by_keys_nulls_last | passed | 0.44 |
| tests.test_hashing::test_normalize_id_trims_unicode_lowercases_keeps_zeros | passed | 0.09 |
| tests.test_hashing::test_hash_is_hmac_of_customer_colon_patient | passed | 0.17 |
| tests.test_hashing::test_hash_is_deterministic_across_formatting | passed | 0.13 |
| tests.test_hashing::test_hash_is_customer_scoped | passed | 0.13 |
| tests.test_hashing::test_different_salt_gives_different_hash | passed | 0.26 |
| tests.test_hashing::test_null_or_blank_input_gives_null | passed | 0.13 |
| tests.test_report::test_escape_cell | passed | 0.00 |
| tests.test_report::test_md_table | passed | 0.00 |
| tests.test_report::test_parse_junit_outcomes | passed | 0.00 |
| tests.test_report::test_parse_count_lines_ignores_other_output | passed | 0.00 |
| tests.test_report::test_render_test_results_header_totals_and_rows | passed | 0.00 |
| tests.test_report::test_render_test_results_pass | passed | 0.00 |
| tests.test_report::test_render_pipeline_output_counts_tables_and_cap | passed | 0.00 |
| tests.test_report::test_render_pipeline_output_not_run | passed | 0.00 |
| tests.test_report::test_pipeline_never_imports_report | passed | 0.00 |
| tests.test_report_main::test_failure_is_reported_then_exit_1[pytest] | passed | 0.00 |
| tests.test_report_main::test_failure_is_reported_then_exit_1[pipeline] | passed | 0.00 |
| tests.test_report_main::test_missing_data_root_writes_not_run_and_exits_2 | passed | 0.00 |
| tests.test_silver::test_clean_encounters | passed | 0.94 |
| tests.test_silver::test_clean_labs | passed | 0.62 |
| tests.test_silver::test_clean_providers | passed | 0.58 |
| tests.test_silver::test_dropped_rows_are_exactly_duplicates_and_missing_required | passed | 0.71 |
| tests.test_silver::test_latest_updated_at_compared_as_timestamp | passed | 0.49 |
| tests.test_silver::test_default_run_ts_is_now_in_whole_seconds | passed | 0.27 |
| tests.test_silver::test_silver_main_writes_three_checked_files | passed | 2.65 |
