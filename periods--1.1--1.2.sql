/* Fix up access controls */

GRANT USAGE ON SCHEMA periods TO PUBLIC;
REVOKE ALL
    ON TABLE periods.periods, periods.system_time_periods,
             periods.for_portion_views, periods.unique_keys,
             periods.foreign_keys, periods.system_versioning
    FROM PUBLIC;
GRANT SELECT
    ON TABLE periods.periods, periods.system_time_periods,
             periods.for_portion_views, periods.unique_keys,
             periods.foreign_keys, periods.system_versioning
    TO PUBLIC;

ALTER FUNCTION periods.add_for_portion_view(regclass,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_foreign_key(regclass,name[],name,name,periods.fk_match_types,periods.fk_actions,periods.fk_actions,name,name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_period(regclass,name,name,name,regtype,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_system_time_period(regclass,name,name,name,name,name,name,name,name[]) SECURITY DEFINER;
ALTER FUNCTION periods.add_system_versioning(regclass,name,name,name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_unique_key(regclass,name[],name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.drop_for_portion_view(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_foreign_key(regclass,name) SECURITY DEFINER;
ALTER FUNCTION periods.drop_period(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_system_versioning(regclass,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_unique_key(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.generated_always_as_row_start_end() SECURITY DEFINER;
ALTER FUNCTION periods.health_checks() SECURITY DEFINER;
ALTER FUNCTION periods.rename_following() SECURITY DEFINER;
ALTER FUNCTION periods.set_system_time_period_excluded_columns(regclass,name[]) SECURITY DEFINER;
ALTER FUNCTION periods.truncate_system_versioning() SECURITY DEFINER;
ALTER FUNCTION periods.write_history() SECURITY DEFINER;
