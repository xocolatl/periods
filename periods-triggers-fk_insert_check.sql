CREATE FUNCTION periods.fk_insert_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    jnew jsonb;
BEGIN
    /*
     * This function is called when a new row is inserted into a table
     * containing foreign keys with periods.  It checks to verify that the
     * referenced table contains the proper data to satisfy the foreign key
     * constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     */

    /* Use jsonb to look up values by parameterized names */
    jnew := row_to_json(NEW);

    /* Check the constraint */
    PERFORM periods.validate_foreign_key(TG_ARGV[0], jnew);

    RETURN NULL;
END;
$function$;

