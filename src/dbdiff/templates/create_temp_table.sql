CREATE LOCAL TEMP TABLE {{ table_name }} ON COMMIT PRESERVE ROWS AS ({{ query }})