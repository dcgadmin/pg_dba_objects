/*
This SQL script sets up a DDL tracking system in PostgreSQL, mimicking the functionality of Oracle's DBA_OBJECTS view.
It creates a dedicated schema, a tracking table, and event triggers to automatically capture and log DDL operations
(CREATE, ALTER, DROP) on various database objects.

Key components include:
- `dba_objects_pg` schema: Houses all tracking objects.
- `dba_objects_tracker` table: Stores metadata about database objects, their creation, last DDL time, and operation.
- `track_ddl_operations()` function: An event trigger function that records CREATE and ALTER DDL commands.
- `track_ddl_drops()` function: An event trigger function that marks objects as INVALID upon DROP commands.
- `populate_existing_objects()` function: A utility function to initially populate the tracker with existing database objects.
- `dba_objects` view: A user-friendly view over `dba_objects_tracker` for easy querying.
- Event Triggers (`ddl_command_end_trigger`, `sql_drop_trigger`): Automatically invoke tracking functions on DDL events.
*/

DROP SCHEMA IF EXISTS dba_objects_pg CASCADE;

create schema if not exists dba_objects_pg;
set search_path = dba_objects_pg, public;

CREATE TABLE IF NOT EXISTS dba_objects_tracker (
    object_id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    object_type TEXT NOT NULL,
    status TEXT DEFAULT 'VALID',
    created_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_ddl_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ddl_operation TEXT,
    object_oid OID
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dba_objects_tracker_unique
ON dba_objects_tracker(schema_name, object_name, object_type)
NULLS NOT DISTINCT;

--create additional indexes for performance
CREATE INDEX IF NOT EXISTS idx_dba_objects_tracker_schema_name ON dba_objects_tracker(schema_name);
CREATE INDEX IF NOT EXISTS idx_dba_objects_tracker_object_name ON dba_objects_tracker(object_name);
CREATE INDEX IF NOT EXISTS idx_dba_objects_tracker_object_type ON dba_objects_tracker(object_type);



CREATE OR REPLACE FUNCTION dba_objects_pg.track_ddl_operations()
RETURNS event_trigger 
AS $$
DECLARE 
    obj RECORD;
BEGIN
    FOR obj IN SELECT classid, objid, objsubid, command_tag, object_type, schema_name, object_identity
               FROM pg_event_trigger_ddl_commands()
               where not in_extension
    LOOP
        INSERT INTO dba_objects_pg.dba_objects_tracker (
            object_name, object_type, schema_name, status, created_date,
            last_ddl_time, ddl_operation, object_oid
        )
        VALUES (
            case array_length(string_to_array(obj.object_identity, '.'),1) 
                when  1 then obj.object_identity::text
                when  2 then (string_to_array(obj.object_identity, '.'))[2]::text
                else  
                    case 
                        when obj.object_type in ('FUNCTION','PROCEDURE') 
                        then array_to_string((string_to_array(obj.object_identity, '.'))[2:],'.')
                        else obj.object_identity::text 
                    end
                end , upper(obj.object_type), CASE WHEN TG_TAG in ('CREATE SCHEMA','ALTER SCHEMA') then obj.object_identity 
                else  coalesce(nullif(obj.schema_name,''),current_schema) end , 'VALID',
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TG_TAG, obj.objid
        )
        ON CONFLICT (schema_name, object_name, object_type)
        DO UPDATE SET
            last_ddl_time = CURRENT_TIMESTAMP,
            ddl_operation = TG_TAG,
            object_oid = obj.objid,
            status = CASE WHEN TG_TAG ilike 'CREATE%' THEN 'VALID' else dba_objects_tracker.status end,
            created_date = CASE WHEN TG_TAG ilike 'CREATE%' THEN CURRENT_TIMESTAMP else dba_objects_tracker.created_date end;
    END LOOP;
END;
$$ LANGUAGE plpgsql
 SECURITY DEFINER
 set search_path to '$user',dba_objects_pg;

CREATE OR REPLACE FUNCTION dba_objects_pg.track_ddl_drops()
RETURNS event_trigger AS $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN SELECT case array_length(string_to_array(object_identity, '.'),1) 
                when  1 then object_identity::text
                when  2 then (string_to_array(object_identity, '.'))[2]::text
                else  
                    case 
                        when object_type in ('FUNCTION','PROCEDURE') 
                        then array_to_string((string_to_array(object_identity, '.'))[2:],'.')
                        else object_identity::text 
                    end
                end  object_name, schema_name, object_type, object_identity
               FROM pg_event_trigger_dropped_objects()
    LOOP
        UPDATE dba_objects_pg.dba_objects_tracker dba
        SET status = 'INVALID',
            last_ddl_time = CURRENT_TIMESTAMP,
            ddl_operation = TG_TAG
        WHERE dba.schema_name ilike coalesce(nullif(obj.schema_name,current_schema),schema_name) 
          AND dba.object_name ilike coalesce(nullif(obj.object_name,''),object_name)
          AND dba.object_type ilike obj.object_type;
    END LOOP;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
set search_path to '$user',dba_objects_pg;


CREATE OR REPLACE FUNCTION populate_existing_objects(truncatetable boolean)
RETURNS VOID AS $$
BEGIN    
   if truncatetable then 
   truncate table dba_objects_tracker;
   end if;
    INSERT INTO dba_objects_tracker 
    (object_name, object_type, schema_name, status, created_date, last_ddl_time, ddl_operation)
      SELECT  c.relname ,
    CASE WHEN c.relkind = 'r' and not relispartition THEN 'TABLE'
         WHEN c.relkind = 'r' and  relispartition THEN 'PARTITION'
         WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE'
         WHEN c.relkind = 'p' and  relispartition THEN 'PARTITION' 
         WHEN c.relkind = 'v' THEN 'VIEW'
         WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW'
         WHEN c.relkind = 'S' THEN 'SEQUENCE'
         ELSE 'OTHER'
    END,
 n.nspname, 'VALID', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'INITIAL_LOAD'
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','v','m','S','')
      AND n.nspname not in ('pg_catalog','dba_objects_pg')
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
  ON CONFLICT DO NOTHING;  

INSERT INTO dba_objects_tracker (object_name, object_type, schema_name, status, created_date, last_ddl_time, ddl_operation)
    SELECT n.nspname AS "Name",'SCHEMA',n.nspname,'VALID', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'INITIAL_LOAD'
    FROM pg_catalog.pg_namespace n
    WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
    ON CONFLICT DO NOTHING;   
    
     INSERT INTO dba_objects_tracker (object_name, object_type, schema_name, status, created_date, last_ddl_time, ddl_operation)
    SELECT pg_catalog.format_type(t.oid, NULL),
    CASE t.typtype when 'b' then 'TYPE' when 'c' then 'TYPE'
                        when 'd' then 'DOMAIN' when 'e' then 'ENUM'
                        when 'p' then 'PSEUDO-TYPE' when 'r' then 'RANGE' end,
    n.nspname as "Schema",
  'VALID',
  CURRENT_TIMESTAMP, 
  CURRENT_TIMESTAMP, 
  'INITIAL_LOAD'
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname <> 'dba_objects_pg'
  AND pg_catalog.pg_type_is_visible(t.oid)
  ON CONFLICT DO NOTHING;  
  

  INSERT INTO dba_objects_tracker (object_name, object_type, schema_name, status, created_date, last_ddl_time, ddl_operation)
    SELECT p.proname || '(' || pg_catalog.pg_get_function_arguments(p.oid) || ')', case prokind when 'f' then 'FUNCTION' when 'p' then 'PROCEDURE' when 'a' then 'AGGREGATE' when 'w' then 'WINDOW' end, n.nspname, 'VALID', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'INITIAL_LOAD'
    FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname NOT IN ('information_schema', 'pg_catalog','dba_objects_pg')
    ON CONFLICT DO NOTHING;   

    INSERT INTO dba_objects_tracker (object_name, object_type, schema_name, status, created_date, last_ddl_time, ddl_operation)
    SELECT i.relname, 'INDEX', n.nspname, 'VALID', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'INITIAL_LOAD'
    FROM pg_class i JOIN pg_namespace n ON i.relnamespace = n.oid
    WHERE i.relkind IN  ('i','I') AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast','dba_objects_pg')
    ON CONFLICT DO NOTHING;
	
	analyze dba_objects_tracker;
END;
$$ LANGUAGE plpgsql
set search_path = dba_objects_pg, public;

CREATE OR REPLACE VIEW dba_objects AS
SELECT 
    object_id,
    case when object_type in ('FUNCTION','PROCEDURE')  
    then regexp_replace(object_name, '(?:.*\.)?(\w+)\(.*', '\1') 
    else object_name end object_name,
    object_name as complete_functions_name,
    object_type,
    schema_name AS owner,
    status,
    created_date,
    last_ddl_time,
    ddl_operation AS last_ddl_operation,
    object_oid
FROM dba_objects_tracker
ORDER BY schema_name, object_name, object_type;

DROP EVENT TRIGGER  IF EXISTS ddl_command_end_trigger ;

CREATE EVENT TRIGGER ddl_command_end_trigger
ON ddl_command_end
when TAG IN (
    'CREATE TABLE', 'ALTER TABLE','CREATE TABLE AS',
    'CREATE INDEX', 'ALTER INDEX', 
    'CREATE SEQUENCE', 'ALTER SEQUENCE', 
    'CREATE VIEW', 'ALTER VIEW',
    'CREATE MATERIALIZED VIEW', 'ALTER MATERIALIZED VIEW', 
    'CREATE FUNCTION', 'ALTER FUNCTION', 
    'CREATE PROCEDURE', 'ALTER PROCEDURE', 
    'CREATE TRIGGER', 'ALTER TRIGGER', 
    'CREATE SCHEMA', 'ALTER SCHEMA', 
    'CREATE TYPE', 'ALTER TYPE', 
    'CREATE DOMAIN', 'ALTER DOMAIN'
)
EXECUTE FUNCTION track_ddl_operations();

DROP EVENT TRIGGER  IF EXISTS sql_drop_trigger ;

CREATE EVENT TRIGGER sql_drop_trigger
ON sql_drop
when TAG IN (
    'DROP TABLE',
    'DROP INDEX',
    'DROP SEQUENCE',
    'DROP VIEW',
    'DROP MATERIALIZED VIEW',
    'DROP FUNCTION',
    'DROP PROCEDURE',
    'DROP TRIGGER',
    'DROP SCHEMA',
    'DROP TYPE',
    'DROP DOMAIN'
)
EXECUTE FUNCTION track_ddl_drops();


