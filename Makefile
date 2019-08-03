MODULES = periods
EXTENSION = periods
DATA = periods--0.04.sql
DOCS = README.periods

REGRESS = install \
		  periods \
		  system_time_periods \
		  system_versioning \
		  unique_foreign \
		  for_portion_of \
		  predicates \
		  drop_protection \
		  rename_following \
		  health_checks \
		  beeswax \
		  uninstall

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
