WITH buves_statuas AS (
    SELECT
        bb.obj_id
        , bk.busena_tekstas AS buves_statusas
        , bb.busena_iki AS buvusio_statuso_pabaigos_data
        , ROW_NUMBER() OVER (PARTITION BY bb.obj_id ORDER BY bb.busena_iki DESC) AS rn
    FROM
        busenos bb
    JOIN
        busena_kodai bk ON bb.busena = bk.busena_kodas
    WHERE
        bb.busena_iki IS NOT NULL
),
esamas_statusas AS (
    SELECT
        eb.obj_id
        , bk.busena_tekstas AS esamas_statusas
        , ROW_NUMBER() OVER (PARTITION BY eb.obj_id ORDER BY eb.busena_nuo DESC) AS rn
    FROM
        busenos eb
    JOIN
        busena_kodai bk ON eb.busena = bk.busena_kodas
    WHERE
        eb.busena_iki IS NULL
)
SELECT
    o.obj_numeris AS objekto_numeris
    , bs.buves_statusas AS buvusi_būsena
    , es.esamas_statusas AS esama_būsena
FROM
    objektai o
LEFT JOIN
    (SELECT * FROM buves_statuas WHERE rn = 1) bs ON o.obj_id = bs.obj_id
LEFT JOIN
    (SELECT * FROM esamas_statusas WHERE rn = 1) es ON o.obj_id = es.obj_id;