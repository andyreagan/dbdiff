SELECT COUNT(*)
FROM (
    SELECT 1
    FROM {{ schema_name }}.{{ table_name }}
    GROUP BY {{ join_cols }}
     ) gb