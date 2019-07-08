MODULES = periods
EXTENSION = periods
DATA = periods--0.03.sql
REGRESS = periods
DOCS = README.periods

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
