SELECT setting::integer < 90600 AS pre_96
FROM pg_settings WHERE name = 'server_version_num';

-- Unique keys are already pretty much guaranteed by the underlying features of
-- PostgreSQL, but test them anyway.
CREATE TABLE uk (id integer, s integer, e integer, CONSTRAINT uk_pkey PRIMARY KEY (id, s, e));
SELECT periods.add_period('uk', 'p', 's', 'e');
SELECT periods.add_unique_key('uk', ARRAY['id'], 'p', key_name => 'uk_id_p', unique_constraint => 'uk_pkey');
INSERT INTO uk (id, s, e) VALUES (100, 1, 3), (100, 3, 4), (100, 4, 10); -- success
INSERT INTO uk (id, s, e) VALUES (200, 1, 3), (200, 3, 4), (200, 5, 10); -- success
INSERT INTO uk (id, s, e) VALUES (300, 1, 3), (300, 3, 5), (300, 4, 10); -- fail

CREATE TABLE fk (id integer, uk_id integer, s integer, e integer, PRIMARY KEY (id));
SELECT periods.add_period('fk', 'q', 's', 'e');
SELECT periods.add_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p', key_name => 'fk_uk_id_q');
-- INSERT
INSERT INTO fk VALUES (0, 100, 0, 1); -- fail
INSERT INTO fk VALUES (0, 100, 0, 10); -- fail
INSERT INTO fk VALUES (0, 100, 1, 11); -- fail
INSERT INTO fk VALUES (1, 100, 1, 3); -- success
INSERT INTO fk VALUES (2, 100, 1, 10); -- success
-- UPDATE
UPDATE fk SET e = 20 WHERE id = 1; -- fail
UPDATE fk SET e = 6 WHERE id = 1; -- success
UPDATE uk SET s = 2 WHERE (id, s, e) = (100, 1, 3); -- fail
UPDATE uk SET s = 0 WHERE (id, s, e) = (100, 1, 3); -- success
-- DELETE
DELETE FROM uk WHERE (id, s, e) = (100, 3, 4); -- fail
DELETE FROM uk WHERE (id, s, e) = (200, 3, 5); -- success

DROP TABLE fk;
DROP TABLE uk;
