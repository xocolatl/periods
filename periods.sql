\set ON_ERROR_STOP 1
SET check_function_bodies TO false;

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

DROP SCHEMA IF EXISTS periods CASCADE;
CREATE SCHEMA periods;

\ir periods-catalogs.sql
\ir periods-generate_name.sql

\ir periods-add_period.sql
\ir periods-drop_period.sql

\ir periods-add_system_time_period.sql
\ir periods-drop_system_time_period.sql
\ir periods-triggers-generated_always_as_row_start_end.sql
\ir periods-triggers-write_history.sql

--\ir periods-add_portion_views.sql
--\ir periods-drop_portion_views.sql
--\ir periods-triggers-update_portion_of.sql

\ir periods-add_unique_key.sql
\ir periods-drop_unique_key.sql
\ir periods-triggers-uk_update_check.sql
\ir periods-triggers-uk_delete_check.sql

\ir periods-add_foreign_key.sql
\ir periods-drop_foreign_key.sql
\ir periods-triggers-fk_insert_check.sql
\ir periods-triggers-fk_update_check.sql
\ir periods-validate_foreign_key.sql

\ir periods-add_system_versioning.sql
\ir periods-drop_system_versioning.sql

\ir periods-health_check.sql
