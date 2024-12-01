WITH paskutinis_statusas_cte AS (
    SELECT
        obj_id
        , MAX(busena_nuo) AS max_busena_nuo
    FROM
        busenos
    GROUP BY
        obj_id
)
SELECT
    o.obj_numeris AS objekto_numeris
    , bk.busena_tekstas AS busena_tekstas
    , b.busena_nuo AS busena_nuo
FROM
    busenos b
JOIN
    objektai o ON b.obj_id = o.obj_id
JOIN
    busena_kodai bk ON b.busena = bk.busena_kodas
JOIN
    paskutinis_statusas_cte ps ON b.obj_id = ps.obj_id AND b.busena_nuo = ps.max_busena_nuo;