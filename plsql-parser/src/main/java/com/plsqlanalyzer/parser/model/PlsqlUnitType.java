package com.plsqlanalyzer.parser.model;

public enum PlsqlUnitType {
    PACKAGE_SPEC,
    PACKAGE_BODY,
    PROCEDURE,
    FUNCTION,
    TRIGGER,
    VIEW,
    MATERIALIZED_VIEW,
    /** A scheduled job (Oracle DBMS_SCHEDULER/DBMS_JOB, SQL Agent, pg_cron, MySQL EVENT). */
    JOB
}
