SELECT
    o.obj_numeris AS objekto_numeris
    , b.busena_nuo AS busena_nuo
    , b.busena_iki AS busena_iki
FROM
    busenos b
JOIN
    objektai o ON b.obj_id = o.obj_id
WHERE
    b.busena = 2
    AND (b.busena_iki IS NULL OR b.busena_iki >= CURRENT_DATE);