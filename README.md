# Periods and `SYSTEM VERSIONING` for PostgreSQL

[![License](https://img.shields.io/badge/license-PostgreSQL-blue)](https://www.postgresql.org/about/licence/)
[![Code of Conduct](https://img.shields.io/badge/code%20of%20conduct-PostgreSQL-blueviolet)](https://www.postgresql.org/about/policies/coc/)

[![Travis Build Status](https://api.travis-ci.com/xocolatl/periods.svg?branch=master)](https://travis-ci.com/xocolatl/periods)
[![Appveyor Build Status](https://ci.appveyor.com/api/projects/status/github/xocolatl/periods?branch=master&svg=true)](https://ci.appveyor.com/project/xocolatl/periods)
[![codecov](https://codecov.io/gh/xocolatl/periods/branch/master/graph/badge.svg)](https://codecov.io/gh/xocolatl/periods)

*compatible 9.5–12*

This extension recreates the behavior defined in
[SQL:2016](https://www.iso.org/standard/63556.html) (originally in
SQL:2011) around periods and tables with `SYSTEM VERSIONING`. The idea
is to figure out all the rules that PostgreSQL would like to adopt
(there are some details missing in the standard) and to allow earlier
versions of PostgreSQL to simulate the behavior once the feature is
finally integrated.

# What is a period?

A period is a definition on a table which specifies a name and two
columns. The period’s name cannot be the same as any column name of the
table.

``` sql
-- Standard SQL

CREATE TABLE example (
    id bigint,
    start_date date,
    end_date date,
    PERIOD FOR validity (start_date, end_date)
);
```

Defining a period constrains the two columns such that the start
column’s value must be strictly inferior to the end column’s value,
and that both columns be non-null. The period’s value includes the start
value but excludes the end value. A period is therefore very similar to
PostgreSQL’s range types, but a bit more restricted.

Since extensions cannot modify PostgreSQL’s grammar, we use functions,
views, and triggers to get as close to the same thing as possible.

``` sql
CREATE TABLE example (
    id bigint,
    start_date date,
    end_date date
);
SELECT periods.add_period('example', 'validity', 'start_date', 'end_date');
```

## Unique constraints

Periods may be part of `PRIMARY KEY`s and `UNIQUE` constraints.

``` sql
-- Standard SQL

CREATE TABLE example (
    id bigint,
    start_date date,
    end_date date,
    PERIOD FOR validity (start_date, end_date),
    UNIQUE (id, validity WITHOUT OVERLAPS)
);
```

``` sql
CREATE TABLE example (
    id bigint,
    start_date date,
    end_date date
);
SELECT periods.add_period('example', 'validity', 'start_date', 'end_date');
SELECT periods.add_unique_key('example', ARRAY['id'], 'validity');
```

The extension will create a unique constraint over all of the columns
specified and the two columns of the period given. It will also create
an exclusion constraint using gist to implement the `WITHOUT OVERLAPS`
part of the constraint. The function also takes optional parameters if
you already have such a constraint that you would like to use.

``` sql
-- Standard SQL

CREATE TABLE example (
    id bigint,
    start_date date,
    end_date date,
    PERIOD FOR validity (start_date, end_date),
    CONSTRAINT example_pkey PRIMARY KEY (id, validity WITHOUT OVERLAPS)
);
```

``` sql
CREATE TABLE example (
    id bigint,
    start_date date,
    end_date date,
    CONSTRAINT example_pkey PRIMARY KEY (id, start_date, end_date)
);
SELECT periods.add_period('example', 'validity', 'start_date', 'end_date');
SELECT periods.add_unique_key('example', ARRAY['id'], 'validity', unique_constraint => 'example_pkey');
```

Unique constraints may only contain one period.

## Foreign keys

If you can have unique keys with periods, you can also have foreign keys
pointing at
them.

``` sql
SELECT periods.add_foreign_key('example2', 'ARRAY[ex_id]', 'validity', 'example_id_validity');
```

In this example, we give the name of the unique key instead of listing
out the referenced columns as you would in normal SQL.

## Portions

The SQL standard allows syntax for updating or deleting just a portion
of a period. Rows are inserted as needed for the portions not being
updated or deleted. Yes, that means a simple `DELETE` statement can
actually `INSERT` rows\!

``` sql
-- Standard SQL

UPDATE example
FOR PORTION OF validity FROM '...' TO '...'
SET ...
WHERE ...;

DELETE FROM example
FOR PORTION OF validity FROM '...' TO '...'
WHERE ...;
```

When updating a portion of a period, it is illegal to modify either of
the two columns contained in the period. This extension uses a view with
an `INSTEAD OF` trigger to figure out what portion of the period you
would like to modify, and issue the correct DML on the underlying table
to do the job.

``` sql
UPDATE example__for_portion_of_validity
SET ...,
    start_date = ...,
    end_date = ...
WHERE ...;
```

We see no way to simulate deleting portions of periods, alas.

## Predicates

The SQL standard provides for several predicates on periods. We have
implemented them as inlined functions for the sake of completeness but
they require specifying the start and end column names instead of the
period name.

``` sql
-- Standard SQL and this extension's equivalent

-- "t" and "u" are tables with respective periods "p" and "q".
-- Both periods have underlying columns "s" and "e".

WHERE t.p CONTAINS 42
WHERE periods.contains(t.s, t.e, 42)

WHERE t.p CONTAINS u.q
WHERE periods.contains(t.s, t.e, u.s, u.e)

WHERE t.p EQUALS u.q
WHERE periods.equals(t.s, t.e, u.s, u.e)

WHERE t.p OVERLAPS u.q
WHERE periods.overlaps(t.s, t.e, u.s, u.e)

WHERE t.p PRECEDES u.q
WHERE periods.precedes(t.s, t.e, u.s, u.e)

WHERE t.p SUCCEEDS u.q
WHERE periods.succeeds(t.s, t.e, u.s, u.e)

WHERE t.p IMMEDIATELY PRECEDES u.q
WHERE periods.immediately_precedes(t.s, t.e, u.s, u.e)

WHERE t.p IMMEDIATELY SUCCEEDS u.q
WHERE periods.immediately_succeeds(t.s, t.e, u.s, u.e)
```

# System-versioned tables

## `SYSTEM_TIME`

If the period is named `SYSTEM_TIME`, then special rules apply. The type
of the columns must be `date`, `timestamp without time zone`, or
`timestamp with time zone`; and they are not modifiable by the user. In
the SQL standard, the start column is `GENERATED ALWAYS AS ROW START`
and the end column is `GENERATED ALWAYS AS ROW END`. This extension uses
triggers to set the start column to `transaction_timestamp()` and the
end column is always `'infinity'`.

***Note:*** It is generally unwise to use anything but `timestamp with
time zone` because changes in the `TimeZone` configuration paramater or
even just Daylight Savings Time changes can distort the history. Even
when only using UTC, we recommend the `timestamp with time zone` type.

``` sql
-- Standard SQL

CREATE TABLE example (
    id bigint PRIMARY KEY,
    value text,
    PERIOD FOR system_time (row_start, row_end)
);
```

``` sql
CREATE TABLE example (
    id bigint PRIMARY KEY,
    value text
);
SELECT periods.add_system_time_period('example', 'row_start', 'row_end');
```

Note that the columns in this special case need not exist. They will be
created both by the SQL standard and by this extension. A special
function is provided as a convenience, but `add_period` can also be
called.

## `WITH SYSTEM VERSIONING`

This special `SYSTEM_TIME` period can be used to keep track of changes
in the table.

``` sql
-- Standard SQL

CREATE TABLE example (
    id bigint PRIMARY KEY,
    value text,
    PERIOD FOR system_time (row_start, row_end)
) WITH SYSTEM VERSIONING;
```

``` sql
CREATE TABLE example (
    id bigint PRIMARY KEY,
    value text
);
SELECT periods.add_system_time_period('example', 'row_start', 'row_end');
SELECT periods.add_system_versioning('example');
```

This instructs the system to keep a record of all changes in the table.
We use a separate history table for this. You can create the history
table yourself and instruct the extension to use it if you want to do
things like add partitioning.

## Temporal querying

The SQL standard extends the `FROM` and `JOIN` clauses to allow
specifying a point in time, or even a range of time (shall we say a
*period* of time?) for which we want the data. This only applies to base
tables and so this extension implements them through inlined functions.

``` sql
-- Standard SQL and this extension's equivalent

SELECT * FROM t FOR system_time AS OF '...';
SELECT * FROM t__as_of('...');

SELECT * FROM t FOR system_time FROM '...' TO '...';
SELECT * FROM t__from_to('...', '...');

SELECT * FROM t FOR system_time BETWEEN '...' AND '...';
SELECT * FROM t__between('...', '...');

SELECT * FROM t FOR system_time BETWEEN SYMMETRIC '...' AND '...';
SELECT * FROM t__between_symmetric('...', '...');
```

# Future

## Completion

This extension is pretty much feature complete, but there are still many
aspects that need to be handled. For example, there is currently no
management of access control.

## Performance

Performance for the temporal queries should be already very similar to
what we can expect from a native implementation in PostgreSQL.

Unique keys should also be as performant as a native implementation,
except that two indexes are needed instead of just one. One of the goals
of this extension is to fork btree to a new access method that handles
the `WITHOUT OVERLAPS` and then patch that back into PostgreSQL when
periods are added.

Foreign key performance should mostly be reasonable, except perhaps when
validating existing data. Some benchmarks would be helpful here.

Performance for the DDL stuff isn’t all that important, but those
functions will likely also be rewritten in C, if only to start being the
patch to present to the PostgreSQL community.

# Contributions

***Contributions are very much welcome\!***

If you would like to help implement the missing features, optimize them,
rewrite them in C, and especially modify btree; please don’t hesitate to
do so.

This project adheres to the [PostgreSQL Community Code of
Conduct](https://www.postgresql.org/about/policies/coc/).

Released under the [PostgreSQL
License](https://www.postgresql.org/about/licence/).

# Acknowledgements

The project would like extend special thanks to:

  - [Christoph Berg](https://github.com/df7cb/) for Debian packaging,
  - [Devrim Gündüz](https://github.com/devrimgunduz) for RPM packaging,
    and
  - [Mikhail Titov](https://github.com/mlt) for Appveyor and Windows
    support.

