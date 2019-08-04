SELECT setting::integer < 100000 AS pre_10,
       setting::integer < 120000 AS pre_12
FROM pg_settings WHERE name = 'server_version_num';

CREATE TABLE pricing (id1 bigserial,
                      id2 bigint GENERATED ALWAYS AS IDENTITY,
                      id3 bigint GENERATED ALWAYS AS (id1 + id2) STORED,
                      product text, min_quantity integer, max_quantity integer, price numeric);
CREATE TABLE pricing (id1 bigserial,
                      id2 bigint GENERATED ALWAYS AS IDENTITY,
                      product text, min_quantity integer, max_quantity integer, price numeric);
CREATE TABLE pricing (id1 bigserial,
                      product text, min_quantity integer, max_quantity integer, price numeric);
SELECT periods.add_period('pricing', 'quantities', 'min_quantity', 'max_quantity');
SELECT periods.add_for_portion_view('pricing', 'quantities');
TABLE periods.for_portion_views;
/* Test UPDATE FOR PORTION */
INSERT INTO pricing (product, min_quantity, max_quantity, price) VALUES ('Trinket', 1, 20, 200);
TABLE pricing ORDER BY min_quantity;
-- UPDATE fully preceding
UPDATE pricing__for_portion_of_quantities SET min_quantity = 0, max_quantity = 1, price = 0;
TABLE pricing ORDER BY min_quantity;
-- UPDATE fully succeeding
UPDATE pricing__for_portion_of_quantities SET min_quantity = 30, max_quantity = 50, price = 0;
TABLE pricing ORDER BY min_quantity;
-- UPDATE fully surrounding
UPDATE pricing__for_portion_of_quantities SET min_quantity = 0, max_quantity = 100, price = 100;
TABLE pricing ORDER BY min_quantity;
-- UPDATE portion
UPDATE pricing__for_portion_of_quantities SET min_quantity = 10, max_quantity = 20, price = 80;
TABLE pricing ORDER BY min_quantity;
-- UPDATE portion of multiple rows
UPDATE pricing__for_portion_of_quantities SET min_quantity = 5, max_quantity = 15, price = 90;
TABLE pricing ORDER BY min_quantity;
-- If we drop the period (without CASCADE) then the FOR PORTION views should be
-- dropped, too.
SELECT periods.drop_period('pricing', 'quantities');
TABLE periods.for_portion_views;
-- Add it back to test the drop_for_portion_view function
SELECT periods.add_period('pricing', 'quantities', 'min_quantity', 'max_quantity');
SELECT periods.add_for_portion_view('pricing', 'quantities');
-- We can't drop the the table without first dropping the FOR PORTION views
-- because Postgres will complain about dependant objects (our views) before we
-- get a chance to clean them up.
DROP TABLE pricing;
SELECT periods.drop_for_portion_view('pricing', NULL);
TABLE periods.for_portion_views;
DROP TABLE pricing;

/* Make sure we handle nulls correctly */
CREATE TABLE portions (col1 text, col2 text, col3 text, s integer, e integer);
SELECT periods.add_period('portions', 'p', 's', 'e');
SELECT periods.add_for_portion_view('portions', 'p');

INSERT INTO portions VALUES
    ('a', 'b', 'c', 100, 200),
    ('a', null, 'c', 100, 200),
    (null, null, 'c', 100, 200);

TABLE portions ORDER BY col1, col2, s, e;
UPDATE portions__for_portion_of_p SET s = 125, e = 175;
TABLE portions ORDER BY col1, col2, s, e;
UPDATE portions__for_portion_of_p SET col3 = 'd', s = 125, e = 175;
TABLE portions ORDER BY col1, col2, s, e;

SELECT periods.drop_for_portion_view('portions', NULL);
DROP TABLE portions;
