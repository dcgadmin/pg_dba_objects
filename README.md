# Tracking Database Objects: Bringing Oracle's DBA_OBJECTS to PostgreSQL

If you've worked with Oracle databases, you know how handy `DBA_OBJECTS` is—it's like having a live inventory of everything in your database that tracks every table, index, function, and view. You can see when objects were created, last modified, and whether they're valid or broken.

PostgreSQL doesn't have this out of the box. But what if you're migrating from Oracle or just want that same visibility? That's exactly why we built a PostgreSQL wrapper that mimics `DBA_OBJECTS` for DDL tracking using event triggers features.

**Disclaimer:** This tool is intended for testing and development environments. It should not be used in production without thorough testing and understanding of its implications.

## What Does DBA_OBJECTS Do in Oracle?

Think of `DBA_OBJECTS` as your database's logbook. It automatically tracks:

*   **What exists**: Tables, views, indexes, sequences, functions—everything.
*   **When it changed**: Creation dates and last modification times.
*   **Its health**: Whether objects are valid or have errors that can be caused due to dependencies.

When you create a table, it shows up immediately. Alter it? The timestamp updates. Drop it? It disappears from the view. No setup required—it just works.

## The PostgreSQL Challenge

PostgreSQL has system catalogs like `pg_class` and `pg_namespace`, but they don't track modification history or object status the way Oracle does. You can see what exists now, but you can't easily answer questions like "What changed last week?" or "When was this object created?"

Some of the options available in PostgreSQL to track DDL activities are not as seamless as in databases like Oracle. One common approach is setting `log_statement = 'ddl'` in `postgresql.conf`, which captures DDL statements in server logs. However, this information resides only in text log files, not within the database itself, making it harder to query or analyze changes directly from SQL. While it provides basic visibility into DDL operations, it lacks the convenience of an in-database, queryable audit trail that DBAs often rely on for efficient change tracking and compliance.

## Our Solution: An Event Trigger-Based Tracker

We built a custom tracking system that brings Oracle's `DBA_OBJECTS` experience to PostgreSQL, currently focused on DDL tracking.

### How it works:

1.  **Initial population**: We scan existing objects and load them into a tracking table.
2.  **Event triggers**: These automatically capture DDL operations (CREATE, ALTER, DROP) as they happen.
3.  **Real-time updates**: Every change updates the tracker with timestamps as `last_ddl_time` and operation types.

### What you can track:

*   Tables, views, and materialized views
*   Indexes and sequences
*   Functions and procedures
*   Schemas and custom types
*   An object's creation time, last DDL time, and status

### What you cannot track:

*   Dependencies-based object validity.

## Why This Matters

For Oracle DBAs moving to PostgreSQL, this wrapper feels instantly familiar. You get the same auditing capabilities you're used to, without learning entirely new system catalogs.

Even if you're a PostgreSQL-native team, having centralized object tracking means:

*   **Easier troubleshooting**: Quickly identify when and how objects changed.
*   **Better auditing**: See who created what and when.
*   **No log parsing**: Query structured data instead of `grep`-ing through text files.
*   **Zero application changes**: Works transparently at the database level.

## The Bottom Line

Object tracking is more than a convenience—it’s essential for managing production databases effectively. PostgreSQL already provides powerful hooks like event triggers for tracking schema changes, but setting them up efficiently can take effort.

That’s where we’ve stepped in. We’ve built an enhanced wrapper that brings Oracle-like object tracking simplicity to PostgreSQL. Whether you're migrating from Oracle or looking to streamline object visibility, our solution delivers the enterprise-grade insights every DBA needs.

## Usage

To use this tool, simply run the `dba_object_event_trigger.sql` script in your PostgreSQL database. This will create the necessary schema, tables, functions, and event triggers.

You can run the script using the `psql` command-line tool:
```bash
psql -U your_username -d your_database -f dba_object_event_trigger.sql
```

After installation, you can query the `dba_objects` view to see the tracked objects:

```sql
SELECT * FROM dba_objects_pg.dba_objects;
```

To populate the tracker with existing objects, run the following function:

```sql
SELECT populate_existing_objects(true); -- 'true' truncates the table before populating
```

## Usage Examples

Here are some examples of how the `dba_objects` view tracks DDL changes.

### Table DDL

1.  **Create a new table:**

    ```sql
    CREATE TABLE public.employees (
        id SERIAL PRIMARY KEY,
        name TEXT
    );
    ```

    Now, query the `dba_objects` view:

    ```sql
    SELECT object_name, object_type, owner, last_ddl_operation
    FROM dba_objects_pg.dba_objects
    WHERE object_name = 'employees';
    ```

    **Result:**

| object_name | object_type | owner | last_ddl_operation |
| :--- | :--- | :--- | :--- |
| employees | TABLE | public | CREATE TABLE |

2.  **Alter the table:**

    ```sql
    ALTER TABLE public.employees ADD COLUMN salary NUMERIC;
    ```

    Query the `dba_objects` view again:

    ```sql
    SELECT object_name, object_type, owner, last_ddl_operation, last_ddl_time
    FROM dba_objects_pg.dba_objects
    WHERE object_name = 'employees';
    ```

    The `last_ddl_operation` will now be `ALTER TABLE` and the `last_ddl_time` will be updated.

### Function DDL

1.  **Create a new function:**

    ```sql
    CREATE OR REPLACE FUNCTION public.get_employee_count()
    RETURNS INT AS $$
    BEGIN
        RETURN (SELECT count(*) FROM public.employees);
    END;
    $$ LANGUAGE plpgsql;
    ```

    Query the `dba_objects` view:

    ```sql
    SELECT object_name, object_type, owner, last_ddl_operation
    FROM dba_objects_pg.dba_objects
    WHERE object_name = 'get_employee_count';
    ```

    **Result:**

| object_name | object_type | owner | last_ddl_operation |
| :--- | :--- | :--- | :--- |
| get_employee_count | FUNCTION | public | CREATE FUNCTION |

2.  **Alter the function:**

    ```sql
    CREATE OR REPLACE FUNCTION public.get_employee_count()
    RETURNS INT AS $$
    BEGIN
        -- A small change
        RETURN (SELECT count(*) FROM public.employees WHERE name IS NOT NULL);
    END;
    $$ LANGUAGE plpgsql;
    ```

    Querying the `dba_objects` view again will show the `last_ddl_operation` as `CREATE FUNCTION` (since `CREATE OR REPLACE` is used) and an updated `last_ddl_time`.
