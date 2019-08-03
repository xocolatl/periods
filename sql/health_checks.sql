SELECT setting::integer < 90600 AS pre_96
FROM pg_settings WHERE name = 'server_version_num';

/* Ensure tables with periods are persistent */
CREATE UNLOGGED TABLE log (id bigint, s date, e date);
SELECT periods.add_period('log', 'p', 's', 'e'); -- fails
SELECT periods.add_system_time_period('log'); -- fails
ALTER TABLE log SET LOGGED;
SELECT periods.add_period('log', 'p', 's', 'e'); -- passes
SELECT periods.add_system_time_period('log'); -- passes
ALTER TABLE log SET UNLOGGED; -- fails
SELECT periods.add_system_versioning('log');
ALTER TABLE log_history SET UNLOGGED; -- fails
SELECT periods.drop_system_versioning('log', purge => true);
DROP TABLE log;
