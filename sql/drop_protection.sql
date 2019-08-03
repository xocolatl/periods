SELECT setting::integer < 90600 AS pre_96
FROM pg_settings WHERE name = 'server_version_num';

/* Make sure nobody drops the objects we keep track of in our catalogs. */

CREATE TYPE integerrange AS RANGE (SUBTYPE = integer);
CREATE TABLE dp (
    id bigint,
    s integer,
    e integer
);

/* periods */
SELECT periods.add_period('dp', 'p', 's', 'e', 'integerrange');
DROP TYPE integerrange;

/* system_time_periods */
SELECT periods.add_system_time_period('dp');
ALTER TABLE dp DROP CONSTRAINT dp_system_time_end_infinity_check; -- fails
DROP TRIGGER dp_system_time_generated_always ON dp; -- fails
DROP TRIGGER dp_system_time_write_history ON dp; -- fails
DROP TRIGGER dp_truncate ON dp; -- fails

/* for_portion_views */
SELECT periods.add_for_portion_view('dp', 'p');
DROP VIEW dp__for_portion_of_p;
DROP TRIGGER for_portion_of_p ON dp__for_portion_of_p;
SELECT periods.drop_for_portion_view('dp', 'p');

/* unique_keys */
ALTER TABLE dp
    ADD CONSTRAINT u UNIQUE (id, s, e),
    ADD CONSTRAINT x EXCLUDE USING gist (id WITH =, integerrange(s, e, '[)') WITH &&);
SELECT periods.add_unique_key('dp', ARRAY['id'], 'p', 'k', 'u', 'x');
ALTER TABLE dp DROP CONSTRAINT u; -- fails
ALTER TABLE dp DROP CONSTRAINT x; -- fails
ALTER TABLE dp DROP CONSTRAINT dp_p_check; -- fails

/* foreign_keys */
CREATE TABLE dp_ref (LIKE dp);
SELECT periods.add_period('dp_ref', 'p', 's', 'e', 'integerrange');
SELECT periods.add_foreign_key('dp_ref', ARRAY['id'], 'p', 'k', key_name => 'f');
DROP TRIGGER f_fk_insert ON dp_ref; -- fails
DROP TRIGGER f_fk_update ON dp_ref; -- fails
DROP TRIGGER f_uk_update ON dp; -- fails
DROP TRIGGER f_uk_delete ON dp; -- fails
SELECT periods.drop_foreign_key('dp_ref', 'f');
DROP TABLE dp_ref;

/* system_versioning */
SELECT periods.add_system_versioning('dp');
-- Note: The history table is protected by the history view and the history
-- view is protected by the temporal functions.
DROP TABLE dp_history CASCADE;
DROP VIEW dp_with_history CASCADE;
DROP FUNCTION dp__as_of(timestamp with time zone);
DROP FUNCTION dp__between(timestamp with time zone,timestamp with time zone);
DROP FUNCTION dp__between_symmetric(timestamp with time zone,timestamp with time zone);
DROP FUNCTION dp__from_to(timestamp with time zone,timestamp with time zone);
SELECT periods.drop_system_versioning('dp', purge => true);

DROP TABLE dp;
DROP TYPE integerrange;
