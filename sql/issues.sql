SELECT setting::integer < 100000 AS pre_10
FROM pg_settings WHERE name = 'server_version_num';

/* https://github.com/xocolatl/periods/issues/5 */

CREATE TABLE issue5 (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    value VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS issue5 (
    id serial PRIMARY KEY,
    value VARCHAR NOT NULL
);

ALTER TABLE issue5
    DROP COLUMN value;

ALTER TABLE issue5
    ADD COLUMN value2 varchar NOT NULL;

INSERT INTO issue5 (value2)
    VALUES ('hello'), ('world');

SELECT periods.add_system_time_period ('issue5');
SELECT periods.add_system_versioning ('issue5');

BEGIN;

SELECT now() AS ts \gset

UPDATE issue5
SET value2 = 'goodbye'
WHERE id = 2;

SELECT id, value2, system_time_start, system_time_end
FROM issue5_with_history
EXCEPT ALL
VALUES (1::integer, 'hello'::varchar, '-infinity'::timestamptz, 'infinity'::timestamptz),
       (2, 'goodbye', :'ts', 'infinity'),
       (2, 'world', '-infinity', :'ts');

COMMIT;

SELECT periods.drop_system_versioning('issue5', drop_behavior => 'CASCADE', purge => true);
DROP TABLE issue5;
