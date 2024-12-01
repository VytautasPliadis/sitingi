# Techninės užduoties sprendiniai

## 1. Neaktyvūs objektai
Visų **šiuo metu** neaktyvių objektų sąrąšas (objekto nueris, neaktyvus nuo, nekatyvus iki)

```sql
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
```

## 2. Objektai su naujausia būsena
Kiekvieno objekto numeris ir jo naujausios būsenos statusas (tekstas ir data)

```sql
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
```

# 3. Objektai su galiojančia ir prieš tai buvusia būsena
Kiekvieno objekto šiuo metu galiojantis ir prieš tai buvęs statusas (objekto numeris, dabartinė būsena, ankstesnė būsena)
```sql
WITH buves_statuas AS (
    SELECT
        bb.obj_id
        , bk.busena_tekstas AS buves_statusas
        , bb.busena_iki AS buvusio_statuso_pabaigos_data
        , ROW_NUMBER() OVER (PARTITION BY bb.obj_id ORDER BY bb.busena_iki DESC) AS rn
    FROM
        Busenos bb
    JOIN
        Busena_kodai bk ON bb.busena = bk.busena_kodas
    WHERE
        bb.busena_iki IS NOT NULL
),
esamas_statusas AS (
    SELECT
        eb.obj_id
        , bk.busena_tekstas AS esamas_statusas
        , ROW_NUMBER() OVER (PARTITION BY eb.obj_id ORDER BY eb.busena_nuo DESC) AS rn
    FROM
        Busenos eb
    JOIN
        Busena_kodai bk ON eb.busena = bk.busena_kodas
    WHERE
        eb.busena_iki IS NULL
)
SELECT
    o.obj_numeris AS objekto_numeris
    , es.esamas_statusas AS esamas_statusas
    , bs.buves_statusas AS buves_statusas
FROM
    Objektai o
LEFT JOIN
    (SELECT * FROM buves_statuas WHERE rn = 1) bs ON o.obj_id = bs.obj_id
LEFT JOIN
    (SELECT * FROM esamas_statusas WHERE rn = 1) es ON o.obj_id = es.obj_id;
```

## 4. Objektai be galiojančio plano
```sql
SELECT 
    o.obj_numeris
FROM 
    objektai o
LEFT JOIN 
    planai p
ON 
    o.obj_id = p.obj_id 
    AND (p.pln_galioja_iki IS NULL OR p.pln_galioja_iki >= CURRENT_DATE)
WHERE 
    p.pln_id IS NULL;
```