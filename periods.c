#include "postgres.h"
#include "fmgr.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "datatype/timestamp.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(generated_always_as_row_start_end);
PG_FUNCTION_INFO_V1(write_history);

/* Define some SQLSTATEs that might not exist */
#if (PG_VERSION_NUM < 100000)
#define ERRCODE_GENERATED_ALWAYS MAKE_SQLSTATE('4','2','8','C','9')
#endif
#define ERRCODE_INVALID_ROW_VERSION MAKE_SQLSTATE('2','2','0','1','H')

void GetPeriodColumnNames(Relation rel, char *period_name, char **start_name, char **end_name);
Oid GetHistoryTable(Relation rel);

void
GetPeriodColumnNames(Relation rel, char *period_name, char **start_name, char **end_name)
{
	int				ret;
	Oid				types[2];
	Datum			values[2];
	SPITupleTable  *tuptable;
	bool			is_null;
	Datum			dat;
	MemoryContext	mcxt = CurrentMemoryContext; /* The context outside of SPI */

	const char *sql =
		"SELECT p.start_column_name, p.end_column_name "
		"FROM periods.periods AS p "
		"WHERE (p.table_name, p.period_name) = ($1, $2)";

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Query the periods table to get the start and end columns.
	 * XXX: Can we cache this?
	 */
	types[0] = OIDOID;
	values[0] = ObjectIdGetDatum(rel->rd_id);
	types[1] = NAMEOID;
	values[1] = CStringGetDatum(period_name);
	ret = SPI_execute_with_args(sql, 2, types, values, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %d", ret);

	/* Make sure we got one */
	if (SPI_processed == 0)
		ereport(ERROR,
				(errmsg("period \"%s\" not found on table \"%s\"",
						period_name,
						RelationGetRelationName(rel))));

	/* There is a unique constraint so there shouldn't be more than 1 row */
	Assert(SPI_processed == 1);

	/*
	 * Get the names from the result tuple.  We copy them into the original
	 * context so they don't get wiped out by SPI_finish().
	 */
	tuptable = SPI_tuptable;

	dat = SPI_getbinval(tuptable->vals[0], tuptable->tupdesc, 1, &is_null);
	*start_name = MemoryContextStrdup(mcxt, NameStr(*(DatumGetName(dat))));

	dat = SPI_getbinval(tuptable->vals[0], tuptable->tupdesc, 2, &is_null);
	*end_name = MemoryContextStrdup(mcxt, NameStr(*(DatumGetName(dat))));

	/* All done with SPI */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * Get the oid of the history table.  If this table does not have a system_time
 * period an error is raised.  If it doesn't have SYSTEM VERSIONING, then
 * InvalidOid is returned.
 */
Oid
GetHistoryTable(Relation rel)
{
	int		ret;
	Oid		types[1];
	Datum	values[1];
	Oid		result;
	SPITupleTable  *tuptable;
	bool			is_null;

	const char *sql =
		"SELECT history_table_name::oid "
		"FROM periods.system_versioning AS sv "
		"WHERE sv.table_name = $1";

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Check existence in system_versioning table.
	 * XXX: Can we cache this?
	 */
	types[0] = OIDOID;
	values[0] = ObjectIdGetDatum(rel->rd_id);
	ret = SPI_execute_with_args(sql, 1, types, values, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %d", ret);

	/* Did we get one? */
	if (SPI_processed == 0)
	{
		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "SPI_finish failed");
		return InvalidOid;
	}

	/* There is a unique constraint so there shouldn't be more than 1 row */
	Assert(SPI_processed == 1);

	/* Get oid from results */
	tuptable = SPI_tuptable;
	result = DatumGetObjectId(SPI_getbinval(tuptable->vals[0], tuptable->tupdesc, 1, &is_null));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return result;
}

Datum
generated_always_as_row_start_end(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = castNode(TriggerData, fcinfo->context);
	const char	   *funcname = "generated_always_as_row_start_end";
	Relation		rel;
	HeapTuple		new_row;
	TupleDesc		new_tupdesc;
	Datum			values[2];
	bool			nulls[2];
	int				columns[2];
	char		   *start_name, *end_name;
	int16			start_num, end_num;

	/*
	 * Make sure this is being called as an BEFORE ROW trigger.  Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired BEFORE ROW",
						funcname)));

	/* Get the new data that was inserted/updated */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		new_row = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		new_row = trigdata->tg_newtuple;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE",
						funcname)));
		new_row = NULL;			/* keep compiler quiet */
	}
	rel = trigdata->tg_relation;
	new_tupdesc = RelationGetDescr(rel);

	GetPeriodColumnNames(rel, "system_time", &start_name, &end_name);

	/* Get the column numbers */
	start_num = SPI_fnumber(new_tupdesc, start_name);
	end_num = SPI_fnumber(new_tupdesc, end_name);

	columns[0] = start_num;
	values[0] = TimestampTzGetDatum(GetCurrentTransactionStartTimestamp());
	nulls[0] = false;
	columns[1] = end_num;
	values[1] = TimestampTzGetDatum(DT_NOEND);
	nulls[1] = false;
#if (PG_VERSION_NUM < 100000)
	new_row = SPI_modifytuple(rel, new_row, 2, columns, values, nulls);
#else
	new_row = heap_modify_tuple_by_cols(new_row, new_tupdesc, 2, columns, values, nulls);
#endif

	return PointerGetDatum(new_row);
}

Datum
write_history(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = castNode(TriggerData, fcinfo->context);
	const char	   *funcname = "write_history";
	Relation		rel;
	HeapTuple		old_row, new_row;
	TupleDesc		tupledesc;
	char		   *start_name, *end_name;
	int16			start_num, end_num;
	TimestampTz		start_val, end_val;
	bool			is_null;
	Oid				history_id;

	/*
	 * Make sure this is being called as an AFTER ROW trigger.  Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER ROW",
						funcname)));

	/* Get the old data that was updated/deleted */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		old_row = NULL;			/* keep compiler quiet */
		new_row = trigdata->tg_trigtuple;
	}
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		old_row = trigdata->tg_trigtuple;
		new_row = trigdata->tg_newtuple;
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		old_row = trigdata->tg_trigtuple;
		new_row = NULL;			/* keep compiler quiet */
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE or DELETE",
						funcname)));
		old_row = NULL;			/* keep compiler quiet */
		new_row = NULL;			/* keep compiler quiet */
	}
	rel = trigdata->tg_relation;
	tupledesc = RelationGetDescr(rel);

	GetPeriodColumnNames(rel, "system_time", &start_name, &end_name);

	/* Get the column numbers */
	start_num = SPI_fnumber(tupledesc, start_name);
	end_num = SPI_fnumber(tupledesc, end_name);

	/*
	 * Validate that the period columns haven't been modified.  This can happen
	 * with a trigger executed after generated_always_as_row_start_end().
	 */
	if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		start_val = DatumGetTimestampTz(SPI_getbinval(new_row, tupledesc, start_num, &is_null));
		end_val = DatumGetTimestampTz(SPI_getbinval(new_row, tupledesc, end_num, &is_null));

		if (start_val != GetCurrentTransactionStartTimestamp())
			ereport(ERROR,
					(errcode(ERRCODE_GENERATED_ALWAYS),
					 errmsg("cannot insert or update column \"%s\"", start_name),
					 errdetail("Column \"%s\" is GENERATED ALWAYS AS ROW START", start_name)));

		if (end_val != DT_NOEND)
			ereport(ERROR,
					(errcode(ERRCODE_GENERATED_ALWAYS),
					 errmsg("cannot insert or update column \"%s\"", end_name),
					 errdetail("Column \"%s\" is GENERATED ALWAYS AS ROW END", end_name)));

		/*
		 * If this is an INSERT, then we're done because there is no history to
		 * write.
		 */
		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
			return PointerGetDatum(NULL);
	}

	/*
	 * Don't do anything more if the start time is still the same.
	 *
	 * DELETE: SQL:2016 13.4 GR 15)a)iii)2)
	 * UPDATE: SQL:2016 15.13 GR 9)a)iii)2)
	 */
	start_val = DatumGetTimestampTz(SPI_getbinval(old_row, tupledesc, start_num, &is_null));
	if (start_val == GetCurrentTransactionStartTimestamp())
		return PointerGetDatum(NULL);

	/*
	 * There is a weird case in READ UNCOMMITTED and READ COMMITTED where a
	 * transaction can UPDATE/DELETE a row created by a transaction that
	 * started later.  In effect, system-versioned tables must be run at the
	 * SERIALIZABLE level and so if we come across such an anomaly, we give an
	 * invalid row version error, per spec.
	 *
	 * DELETE: SQL:2016 13.4 GR 15)a)iii)1)
	 * UPDATE: SQL:2016 15.13 GR 9)a)iii)1)
	 */
	if (start_val > GetCurrentTransactionStartTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ROW_VERSION),
				 errmsg("invalid row version"),
				 errdetail("The row being updated or deleted was created after this transaction started."),
				 errhint("The transaction might succeed if retried.")));

	/*
	 * If this table does not have SYSTEM VERSIONING, there is nothing else to
	 * be done.
	 */
	history_id = GetHistoryTable(rel);
	if (OidIsValid(history_id))
	{
		Relation	history_rel;
		TupleDesc	history_tupdesc;
		HeapTuple	history_tuple;
		Datum	   *values;
		bool	   *nulls;

		/* Open the history table for inserting */
		history_rel = heap_open(history_id, RowExclusiveLock);
		history_tupdesc = RelationGetDescr(history_rel);

		/* Build the new tuple for the history table */
		values = (Datum *) palloc(tupledesc->natts * sizeof(Datum));
		nulls = (bool *) palloc(tupledesc->natts * sizeof(bool));

		heap_deform_tuple(old_row, tupledesc, values, nulls);
		/* Modify the historical ROW END on the fly */
		values[end_num-1] = TimestampTzGetDatum(GetCurrentTransactionStartTimestamp());
		nulls[end_num-1] = false;
		history_tuple = heap_form_tuple(history_tupdesc, values, nulls);

		pfree(values);
		pfree(nulls);

		/* INSERT the row */
		simple_heap_insert(history_rel, history_tuple);

		/* Keep the lock until end of transaction */
		heap_close(history_rel, NoLock);
	}

	return PointerGetDatum(NULL);
}
