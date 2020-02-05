SELECT setting::integer < 130000 AS pre_13
FROM pg_settings WHERE name = 'server_version_num';

/* Basic period definitions with dates */
CREATE TABLE basic (val text, s date, e date);
TABLE periods.periods;
SELECT periods.add_period('basic', 'bp', 's', 'e');
TABLE periods.periods;
SELECT periods.drop_period('basic', 'bp');
TABLE periods.periods;
SELECT periods.add_period('basic', 'bp', 's', 'e', bounds_check_constraint => 'c');
TABLE periods.periods;
SELECT periods.drop_period('basic', 'bp', purge => true);
TABLE periods.periods;
SELECT periods.add_period('basic', 'bp', 's', 'e');
TABLE periods.periods;
/* Test constraints */
INSERT INTO basic (val, s, e) VALUES ('x', null, null); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '3000-01-01', null); --fail
INSERT INTO basic (val, s, e) VALUES ('x', null, '1000-01-01'); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '3000-01-01', '1000-01-01'); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '1000-01-01', '3000-01-01'); --success
TABLE basic;
/* Test dropping the whole thing */
DROP TABLE basic;
TABLE periods.periods;

