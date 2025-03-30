DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc4
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER ()                 AS objectid
,b.tnmid
,CAST(NULL AS VARCHAR(40))            AS metasourceid
,CAST(NULL AS DATE)                   AS loaddate
,ROUND(a.areasqkm::NUMERIC,4)         AS areasqkm
,ROUND(a.areaacres::NUMERIC,4)        AS areaacres
,b.name
,ARRAY_TO_STRING(a.states_array,',')  AS states
,a.huc4
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc6
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu6_np21_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32655_pk
ON cipsrv_wbd.wbd_hu4_np21_32655(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32655_u01
ON cipsrv_wbd.wbd_hu4_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32655_u02
ON cipsrv_wbd.wbd_hu4_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_32655_f01
ON cipsrv_wbd.wbd_hu4_np21_32655(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_32655_spx
ON cipsrv_wbd.wbd_hu4_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21_32655;

