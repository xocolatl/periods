CREATE FUNCTION periods.uk_delete_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
BEGIN
    /*
     * This function is called when a table referenced by foreign keys with
     * periods is deleted from.  It checks to verify that the referenced table
     * still contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     *
     * The only difference between NO ACTION and RESTRICT is when the check is
     * done, so this function is used for both.
     *
     * Currently, the entire table is rechecked.  Obviously, this can be improved.
     */

    /* Check the constraint */
    PERFORM periods.validate_foreign_key(TG_ARGV[0]);

    RETURN NULL;
END;
$function$;

