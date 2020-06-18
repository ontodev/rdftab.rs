WITH literal(value, escaped) AS (
  SELECT DISTINCT
    value,
    replace(replace(replace(value, '\', '\\'), '"', '\"'), '
', '\n') AS escaped
  FROM statements
)
SELECT
  "@prefix " || prefix || ": <" || base || "> ."
FROM prefix
UNION ALL
SELECT 
   subject
|| " "
|| predicate
|| " "
|| coalesce(
     object,
     """" || escaped || """^^" || datatype,
     """" || escaped || """@" || language,
     """" || escaped || """"
   )
|| " ."
FROM statements LEFT JOIN literal ON statements.value = literal.value;
