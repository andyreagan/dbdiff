        SELECT {{ join_cols|join(", ")  }},
               x_{{ column }},
               y_{{ column }},
               ABS(x_{{ column }} - y_{{ column }}) AS abs_diff
          FROM ({% include "joined_column_raw.sql" %}) q_raw
      ORDER BY ABS(x_{{ column }} - y_{{ column }}) DESC