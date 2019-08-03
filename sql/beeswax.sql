/*
 * Test creating a table, dropping a column, and then dropping the whole thing;
 * without any periods.  This is to make sure the health checks don't try to do
 * anything.
 */
CREATE TABLE beeswax (col1 text, col2 date);
ALTER TABLE beeswax DROP COLUMN col1;
DROP TABLE beeswax;
