SELECT setting::integer < 90600 AS pre_96
FROM pg_settings WHERE name = 'server_version_num';

/* Run tests as unprivileged user */
SET ROLE TO periods_unprivileged_user;

CREATE TABLE excl (
    value text NOT NULL,
    null_value integer,
    flap text NOT NULL
);
SELECT periods.add_system_time_period('excl', excluded_column_names => ARRAY['xmin']); -- fails
SELECT periods.add_system_time_period('excl', excluded_column_names => ARRAY['none']); -- fails
SELECT periods.add_system_time_period('excl', excluded_column_names => ARRAY['flap']); -- passes
SELECT periods.add_system_versioning('excl');

TABLE periods.periods;
TABLE periods.system_time_periods;
TABLE periods.system_versioning;

BEGIN;
SELECT CURRENT_TIMESTAMP AS now \gset
INSERT INTO excl (value, flap) VALUES ('hello world', 'off');
COMMIT;
SELECT value, null_value, flap, system_time_start <> :'now' AS changed FROM excl;

UPDATE excl SET flap = 'off';
UPDATE excl SET flap = 'on';
UPDATE excl SET flap = 'off';
UPDATE excl SET flap = 'on';
SELECT value, null_value, flap, system_time_start <> :'now' AS changed FROM excl;

BEGIN;
SELECT CURRENT_TIMESTAMP AS now2 \gset
UPDATE excl SET value = 'howdy folks!';
COMMIT;
SELECT value, null_value, flap, system_time_start <> :'now' AS changed FROM excl;

UPDATE excl SET null_value = 0;
SELECT value, null_value, flap, system_time_start <> :'now2' AS changed FROM excl;

/* Test directly setting the excluded columns */
SELECT periods.drop_system_versioning('excl');
ALTER TABLE excl ADD COLUMN flop text;
ALTER TABLE excl_history ADD COLUMN flop text;
SELECT periods.add_system_versioning('excl');

SELECT periods.set_system_time_period_excluded_columns('excl', ARRAY['flap', 'flop']);
TABLE periods.system_time_periods;
UPDATE excl SET flop = 'flop';
SELECT value, null_value, flap, flop FROM excl;
SELECT value, null_value, flap, flop FROM excl_history ORDER BY system_time_start;

SELECT periods.drop_system_versioning('excl', drop_behavior => 'CASCADE', purge => true);
DROP TABLE excl;
