SELECT MIN(diff) AS min_diff,
       MAX(diff) AS max_diff,
       SUM(ct) AS ct
  FROM (
      SELECT NTILE({{ tiles }}) OVER (ORDER BY ABS(raw.x_{{ column }}-raw.y_{{ column }})) AS n_tile,
            ABS(raw.x_{{ column }}-raw.y_{{ column }}) AS diff,
            raw.ct
            FROM (
                {% include "joined_column.sql" %}
            ) raw
            WHERE raw.x_{{ column }} IS NOT NULL
                  AND raw.y_{{ column }} IS NOT NULL
        ) tiled
GROUP BY n_tile
ORDER BY MIN(diff) ASC, MAX(diff) ASC