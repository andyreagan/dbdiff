  SELECT column_name,
         COUNT(*)
    FROM {{ schema_name }}.{{ table_name }}
GROUP BY 1
ORDER BY 2 DESC;