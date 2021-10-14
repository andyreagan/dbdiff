-- could pull them out of the original table:
-- SELECT x_{{ column }},
--   y_{{ column }},
--   COUNT(*) AS ct
-- FROM (
-- SELECT x_{{ column }}, y_{{ column }},
--   x_{{ column }} <=> y_{{ column }} AS {{ column }}_eq
-- FROM {{ joined_schema }}.{{ joined_table }}
--   ) AS t1
-- WHERE t1.{{ column }}_eq IS FALSE
-- GROUP BY x_{{ column }}, y_{{ column }}
-- ORDER BY ct DESC
-- but instead use the results we already have
-- some critique of the following: the group by in the subquery isn't
-- necessary based on the design (the insert into this diff table)
SELECT joined.x_{{ column }},
   joined.y_{{ column }},
   COUNT(*) AS ct
FROM {{ joined_schema }}.{{ joined_table }} joined
INNER JOIN (
SELECT {{ group_cols }}
FROM {{ diff_schema }}.{{ diff_table }}
WHERE column_name = '{{ column }}'
GROUP BY {{ group_cols }}
   ) diff
   ON {{ join_cols }}
GROUP BY joined.x_{{ column }}, joined.y_{{ column }}
ORDER BY ct DESC