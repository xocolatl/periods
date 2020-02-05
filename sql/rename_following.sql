SELECT setting::integer < 90600 AS pre_96
FROM pg_settings WHERE name = 'server_version_num';

/*
 * If anything we store as "name" is renamed, we need to update our catalogs or
 * throw an error.
 */

/* periods */
CREATE TABLE rename_test(col1 text, col2 bigint, col3 time, s integer, e integer);
SELECT periods.add_period('rename_test', 'p', 's', 'e');
TABLE periods.periods;
ALTER TABLE rename_test RENAME s TO start;
ALTER TABLE rename_test RENAME e TO "end";
TABLE periods.periods;
ALTER TABLE rename_test RENAME start TO "s < e";
TABLE periods.periods;
ALTER TABLE rename_test RENAME "end" TO "embedded "" symbols";
TABLE periods.periods;
ALTER TABLE rename_test RENAME CONSTRAINT rename_test_p_check TO start_before_end;
TABLE periods.periods;

/* system_time_periods */
SELECT periods.add_system_time_period('rename_test', excluded_column_names => ARRAY['col3']);
TABLE periods.system_time_periods;
ALTER TABLE rename_test RENAME col3 TO "COLUMN3";
ALTER TABLE rename_test RENAME CONSTRAINT rename_test_system_time_end_infinity_check TO inf_check;
ALTER TRIGGER rename_test_system_time_generated_always ON rename_test RENAME TO generated_always;
ALTER TRIGGER rename_test_system_time_write_history ON rename_test RENAME TO write_history;
ALTER TRIGGER rename_test_truncate ON rename_test RENAME TO trunc;
TABLE periods.system_time_periods;

/* for_portion_views */
ALTER TABLE rename_test ADD COLUMN id integer PRIMARY KEY;
SELECT periods.add_for_portion_view('rename_test', 'p');
TABLE periods.for_portion_views;
ALTER TRIGGER for_portion_of_p ON rename_test__for_portion_of_p RENAME TO portion_trigger;
TABLE periods.for_portion_views;
SELECT periods.drop_for_portion_view('rename_test', 'p');
ALTER TABLE rename_test DROP COLUMN id;

/* unique_keys */
SELECT periods.add_unique_key('rename_test', ARRAY['col2', 'col1', 'col3'], 'p');
TABLE periods.unique_keys;
ALTER TABLE rename_test RENAME COLUMN col1 TO "COLUMN1";
ALTER TABLE rename_test RENAME CONSTRAINT "rename_test_col2_col1_col3_s < e_embedded "" symbols_key" TO unconst;
ALTER TABLE rename_test RENAME CONSTRAINT rename_test_col2_col1_col3_int4range_excl TO exconst;
TABLE periods.unique_keys;

/* foreign_keys */
CREATE TABLE rename_test_ref (LIKE rename_test);
SELECT periods.add_period('rename_test_ref', 'q', 's < e', 'embedded " symbols');
SELECT periods.add_foreign_key('rename_test_ref', ARRAY['col2', 'COLUMN1', 'col3'], 'q', 'rename_test_col2_col1_col3_p');
TABLE periods.foreign_keys;
ALTER TABLE rename_test_ref RENAME COLUMN "COLUMN1" TO col1; -- fails
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_fk_insert" ON rename_test_ref RENAME TO fk_insert;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_fk_update" ON rename_test_ref RENAME TO fk_update;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_uk_update" ON rename_test RENAME TO uk_update;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_uk_delete" ON rename_test RENAME TO uk_delete;
TABLE periods.foreign_keys;
DROP TABLE rename_test_ref;

/* system_versioning */
-- Nothing to do here

DROP TABLE rename_test;
