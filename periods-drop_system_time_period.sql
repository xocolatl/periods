CREATE FUNCTION periods.drop_system_time_period(table_name regclass, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE sql
AS
$function$
SELECT periods.drop_period($1, 'system_time', drop_behavior, purge);
$function$;
