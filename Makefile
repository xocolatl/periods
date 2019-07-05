MODULES = periods
EXTENSION = periods
DATA = periods--0.02.sql
REGRESS = periods
DOCS = README.periods

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
