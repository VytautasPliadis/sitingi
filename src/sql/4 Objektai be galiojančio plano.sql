SELECT
    o.obj_numeris AS objekto_numeris
FROM
    objektai o
LEFT JOIN
    planai p
ON
    o.obj_id = p.obj_id
    AND (p.pln_galioja_iki IS NULL OR p.pln_galioja_iki >= CURRENT_DATE)
WHERE
    p.pln_id IS NULL;