    SELECT x.*
      FROM {{ schema_name }}.{{ joined_table }} x
INNER JOIN (
    SELECT {{ group_cols }}
      FROM {{ schema_name }}.{{ diff_table }}
  GROUP BY {{ group_cols }}
           ) joined
        ON {{ join_cols }}