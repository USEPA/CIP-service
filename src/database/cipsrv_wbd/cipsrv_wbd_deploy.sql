--******************************--
----- materialized views/wbd_hu12_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_5070_pk
ON cipsrv_wbd.wbd_hu12_np21_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_5070_u01
ON cipsrv_wbd.wbd_hu12_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_5070_u02
ON cipsrv_wbd.wbd_hu12_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_5070_f01
ON cipsrv_wbd.wbd_hu12_np21_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_5070_f02
ON cipsrv_wbd.wbd_hu12_np21_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_5070_f03
ON cipsrv_wbd.wbd_hu12_np21_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_5070_f04
ON cipsrv_wbd.wbd_hu12_np21_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_5070_f05
ON cipsrv_wbd.wbd_hu12_np21_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_5070_spx
ON cipsrv_wbd.wbd_hu12_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_np21_5070;

--******************************--
----- materialized views/wbd_hu12_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_3338_pk
ON cipsrv_wbd.wbd_hu12_np21_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_3338_u01
ON cipsrv_wbd.wbd_hu12_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_3338_u02
ON cipsrv_wbd.wbd_hu12_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_3338_f01
ON cipsrv_wbd.wbd_hu12_np21_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_3338_f02
ON cipsrv_wbd.wbd_hu12_np21_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_3338_f03
ON cipsrv_wbd.wbd_hu12_np21_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_3338_f04
ON cipsrv_wbd.wbd_hu12_np21_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_3338_f05
ON cipsrv_wbd.wbd_hu12_np21_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_3338_spx
ON cipsrv_wbd.wbd_hu12_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_np21_3338;

--******************************--
----- materialized views/wbd_hu12_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipsrv_wbd.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipsrv_wbd.wbd_hu12_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_26904_pk
ON cipsrv_wbd.wbd_hu12_np21_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_26904_u01
ON cipsrv_wbd.wbd_hu12_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_26904_u02
ON cipsrv_wbd.wbd_hu12_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_26904_f01
ON cipsrv_wbd.wbd_hu12_np21_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_26904_f02
ON cipsrv_wbd.wbd_hu12_np21_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_26904_f03
ON cipsrv_wbd.wbd_hu12_np21_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_26904_f04
ON cipsrv_wbd.wbd_hu12_np21_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_26904_f05
ON cipsrv_wbd.wbd_hu12_np21_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_26904_spx
ON cipsrv_wbd.wbd_hu12_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_np21_26904;

--******************************--
----- materialized views/wbd_hu12_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32161_pk
ON cipsrv_wbd.wbd_hu12_np21_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32161_u01
ON cipsrv_wbd.wbd_hu12_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32161_u02
ON cipsrv_wbd.wbd_hu12_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32161_f01
ON cipsrv_wbd.wbd_hu12_np21_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32161_f02
ON cipsrv_wbd.wbd_hu12_np21_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32161_f03
ON cipsrv_wbd.wbd_hu12_np21_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32161_f04
ON cipsrv_wbd.wbd_hu12_np21_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32161_f05
ON cipsrv_wbd.wbd_hu12_np21_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32161_spx
ON cipsrv_wbd.wbd_hu12_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_np21_32161;

--******************************--
----- materialized views/wbd_hu12_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32655)      AS shape
FROM
cipsrv_wbd.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_wbd.wbd_hu12_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32655_pk
ON cipsrv_wbd.wbd_hu12_np21_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32655_u01
ON cipsrv_wbd.wbd_hu12_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32655_u02
ON cipsrv_wbd.wbd_hu12_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32655_f01
ON cipsrv_wbd.wbd_hu12_np21_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32655_f02
ON cipsrv_wbd.wbd_hu12_np21_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32655_f03
ON cipsrv_wbd.wbd_hu12_np21_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32655_f04
ON cipsrv_wbd.wbd_hu12_np21_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32655_f05
ON cipsrv_wbd.wbd_hu12_np21_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32655_spx
ON cipsrv_wbd.wbd_hu12_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_np21_32655;

--******************************--
----- materialized views/wbd_hu12_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32702)      AS shape
FROM
cipsrv_wbd.wbd_hu12_np21 a
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_wbd.wbd_hu12_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32702_pk
ON cipsrv_wbd.wbd_hu12_np21_32702(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32702_u01
ON cipsrv_wbd.wbd_hu12_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_np21_32702_u02
ON cipsrv_wbd.wbd_hu12_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32702_f01
ON cipsrv_wbd.wbd_hu12_np21_32702(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32702_f02
ON cipsrv_wbd.wbd_hu12_np21_32702(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32702_f03
ON cipsrv_wbd.wbd_hu12_np21_32702(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32702_f04
ON cipsrv_wbd.wbd_hu12_np21_32702(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32702_f05
ON cipsrv_wbd.wbd_hu12_np21_32702(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_np21_32702_spx
ON cipsrv_wbd.wbd_hu12_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_np21_32702;

--******************************--
----- materialized views/wbd_hu10_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_np21_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_5070_pk
ON cipsrv_wbd.wbd_hu10_np21_5070(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_5070_u01
ON cipsrv_wbd.wbd_hu10_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_5070_u02
ON cipsrv_wbd.wbd_hu10_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_5070_f01
ON cipsrv_wbd.wbd_hu10_np21_5070(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_5070_f02
ON cipsrv_wbd.wbd_hu10_np21_5070(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_5070_f03
ON cipsrv_wbd.wbd_hu10_np21_5070(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_5070_f04
ON cipsrv_wbd.wbd_hu10_np21_5070(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_5070_spx
ON cipsrv_wbd.wbd_hu10_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21_5070;

--******************************--
----- materialized views/wbd_hu10_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_np21_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_3338_pk
ON cipsrv_wbd.wbd_hu10_np21_3338(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_3338_u01
ON cipsrv_wbd.wbd_hu10_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_3338_u02
ON cipsrv_wbd.wbd_hu10_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_3338_f01
ON cipsrv_wbd.wbd_hu10_np21_3338(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_3338_f02
ON cipsrv_wbd.wbd_hu10_np21_3338(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_3338_f03
ON cipsrv_wbd.wbd_hu10_np21_3338(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_3338_f04
ON cipsrv_wbd.wbd_hu10_np21_3338(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_3338_spx
ON cipsrv_wbd.wbd_hu10_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21_3338;

--******************************--
----- materialized views/wbd_hu10_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_np21_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_26904_pk
ON cipsrv_wbd.wbd_hu10_np21_26904(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_26904_u01
ON cipsrv_wbd.wbd_hu10_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_26904_u02
ON cipsrv_wbd.wbd_hu10_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_26904_f01
ON cipsrv_wbd.wbd_hu10_np21_26904(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_26904_f02
ON cipsrv_wbd.wbd_hu10_np21_26904(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_26904_f03
ON cipsrv_wbd.wbd_hu10_np21_26904(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_26904_f04
ON cipsrv_wbd.wbd_hu10_np21_26904(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_26904_spx
ON cipsrv_wbd.wbd_hu10_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21_26904;

--******************************--
----- materialized views/wbd_hu10_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_np21_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32161_pk
ON cipsrv_wbd.wbd_hu10_np21_32161(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32161_u01
ON cipsrv_wbd.wbd_hu10_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32161_u02
ON cipsrv_wbd.wbd_hu10_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32161_f01
ON cipsrv_wbd.wbd_hu10_np21_32161(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32161_f02
ON cipsrv_wbd.wbd_hu10_np21_32161(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32161_f03
ON cipsrv_wbd.wbd_hu10_np21_32161(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32161_f04
ON cipsrv_wbd.wbd_hu10_np21_32161(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32161_spx
ON cipsrv_wbd.wbd_hu10_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21_32161;

--******************************--
----- materialized views/wbd_hu10_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_np21_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32655_pk
ON cipsrv_wbd.wbd_hu10_np21_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32655_u01
ON cipsrv_wbd.wbd_hu10_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32655_u02
ON cipsrv_wbd.wbd_hu10_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32655_f01
ON cipsrv_wbd.wbd_hu10_np21_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32655_f02
ON cipsrv_wbd.wbd_hu10_np21_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32655_f03
ON cipsrv_wbd.wbd_hu10_np21_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32655_f04
ON cipsrv_wbd.wbd_hu10_np21_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32655_spx
ON cipsrv_wbd.wbd_hu10_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21_32655;

--******************************--
----- materialized views/wbd_hu10_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_np21_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32702_pk
ON cipsrv_wbd.wbd_hu10_np21_32702(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32702_u01
ON cipsrv_wbd.wbd_hu10_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_32702_u02
ON cipsrv_wbd.wbd_hu10_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32702_f01
ON cipsrv_wbd.wbd_hu10_np21_32702(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32702_f02
ON cipsrv_wbd.wbd_hu10_np21_32702(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32702_f03
ON cipsrv_wbd.wbd_hu10_np21_32702(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32702_f04
ON cipsrv_wbd.wbd_hu10_np21_32702(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_32702_spx
ON cipsrv_wbd.wbd_hu10_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21_32702;

--******************************--
----- materialized views/wbd_hu8_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_np21_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_5070_pk
ON cipsrv_wbd.wbd_hu8_np21_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_5070_u01
ON cipsrv_wbd.wbd_hu8_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_5070_u02
ON cipsrv_wbd.wbd_hu8_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_5070_f01
ON cipsrv_wbd.wbd_hu8_np21_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_5070_f02
ON cipsrv_wbd.wbd_hu8_np21_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_5070_f03
ON cipsrv_wbd.wbd_hu8_np21_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_5070_spx
ON cipsrv_wbd.wbd_hu8_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21_5070;

--******************************--
----- materialized views/wbd_hu8_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_np21_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_3338_pk
ON cipsrv_wbd.wbd_hu8_np21_3338(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_3338_u01
ON cipsrv_wbd.wbd_hu8_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_3338_u02
ON cipsrv_wbd.wbd_hu8_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_3338_f01
ON cipsrv_wbd.wbd_hu8_np21_3338(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_3338_f02
ON cipsrv_wbd.wbd_hu8_np21_3338(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_3338_f03
ON cipsrv_wbd.wbd_hu8_np21_3338(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_3338_spx
ON cipsrv_wbd.wbd_hu8_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21_3338;

--******************************--
----- materialized views/wbd_hu8_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_np21_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_26904_pk
ON cipsrv_wbd.wbd_hu8_np21_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_26904_u01
ON cipsrv_wbd.wbd_hu8_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_26904_u02
ON cipsrv_wbd.wbd_hu8_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_26904_f01
ON cipsrv_wbd.wbd_hu8_np21_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_26904_f02
ON cipsrv_wbd.wbd_hu8_np21_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_26904_f03
ON cipsrv_wbd.wbd_hu8_np21_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_26904_spx
ON cipsrv_wbd.wbd_hu8_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21_26904;

--******************************--
----- materialized views/wbd_hu8_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_np21_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32161_pk
ON cipsrv_wbd.wbd_hu8_np21_32161(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32161_u01
ON cipsrv_wbd.wbd_hu8_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32161_u02
ON cipsrv_wbd.wbd_hu8_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32161_f01
ON cipsrv_wbd.wbd_hu8_np21_32161(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32161_f02
ON cipsrv_wbd.wbd_hu8_np21_32161(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32161_f03
ON cipsrv_wbd.wbd_hu8_np21_32161(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32161_spx
ON cipsrv_wbd.wbd_hu8_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21_32161;

--******************************--
----- materialized views/wbd_hu8_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_np21_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32655_pk
ON cipsrv_wbd.wbd_hu8_np21_32655(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32655_u01
ON cipsrv_wbd.wbd_hu8_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32655_u02
ON cipsrv_wbd.wbd_hu8_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32655_f01
ON cipsrv_wbd.wbd_hu8_np21_32655(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32655_f02
ON cipsrv_wbd.wbd_hu8_np21_32655(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32655_f03
ON cipsrv_wbd.wbd_hu8_np21_32655(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32655_spx
ON cipsrv_wbd.wbd_hu8_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21_32655;

--******************************--
----- materialized views/wbd_hu8_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_np21_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32702_pk
ON cipsrv_wbd.wbd_hu8_np21_32702(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32702_u01
ON cipsrv_wbd.wbd_hu8_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_32702_u02
ON cipsrv_wbd.wbd_hu8_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32702_f01
ON cipsrv_wbd.wbd_hu8_np21_32702(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32702_f02
ON cipsrv_wbd.wbd_hu8_np21_32702(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32702_f03
ON cipsrv_wbd.wbd_hu8_np21_32702(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_32702_spx
ON cipsrv_wbd.wbd_hu8_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21_32702;

--******************************--
----- materialized views/wbd_hu6_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_np21_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_5070_pk
ON cipsrv_wbd.wbd_hu6_np21_5070(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_5070_u01
ON cipsrv_wbd.wbd_hu6_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_5070_u02
ON cipsrv_wbd.wbd_hu6_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_5070_f01
ON cipsrv_wbd.wbd_hu6_np21_5070(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_5070_f02
ON cipsrv_wbd.wbd_hu6_np21_5070(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_5070_spx
ON cipsrv_wbd.wbd_hu6_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21_5070;

--******************************--
----- materialized views/wbd_hu6_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_np21_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_3338_pk
ON cipsrv_wbd.wbd_hu6_np21_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_3338_u01
ON cipsrv_wbd.wbd_hu6_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_3338_u02
ON cipsrv_wbd.wbd_hu6_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_3338_f01
ON cipsrv_wbd.wbd_hu6_np21_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_3338_f02
ON cipsrv_wbd.wbd_hu6_np21_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_3338_spx
ON cipsrv_wbd.wbd_hu6_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21_3338;

--******************************--
----- materialized views/wbd_hu6_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_np21_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_26904_pk
ON cipsrv_wbd.wbd_hu6_np21_26904(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_26904_u01
ON cipsrv_wbd.wbd_hu6_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_26904_u02
ON cipsrv_wbd.wbd_hu6_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_26904_f01
ON cipsrv_wbd.wbd_hu6_np21_26904(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_26904_f02
ON cipsrv_wbd.wbd_hu6_np21_26904(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_26904_spx
ON cipsrv_wbd.wbd_hu6_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21_26904;

--******************************--
----- materialized views/wbd_hu6_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_np21_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32161_pk
ON cipsrv_wbd.wbd_hu6_np21_32161(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32161_u01
ON cipsrv_wbd.wbd_hu6_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32161_u02
ON cipsrv_wbd.wbd_hu6_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32161_f01
ON cipsrv_wbd.wbd_hu6_np21_32161(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32161_f02
ON cipsrv_wbd.wbd_hu6_np21_32161(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32161_spx
ON cipsrv_wbd.wbd_hu6_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21_32161;

--******************************--
----- materialized views/wbd_hu6_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_np21_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32655_pk
ON cipsrv_wbd.wbd_hu6_np21_32655(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32655_u01
ON cipsrv_wbd.wbd_hu6_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32655_u02
ON cipsrv_wbd.wbd_hu6_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32655_f01
ON cipsrv_wbd.wbd_hu6_np21_32655(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32655_f02
ON cipsrv_wbd.wbd_hu6_np21_32655(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32655_spx
ON cipsrv_wbd.wbd_hu6_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21_32655;

--******************************--
----- materialized views/wbd_hu6_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_np21_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32702_pk
ON cipsrv_wbd.wbd_hu6_np21_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32702_u01
ON cipsrv_wbd.wbd_hu6_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_32702_u02
ON cipsrv_wbd.wbd_hu6_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32702_f01
ON cipsrv_wbd.wbd_hu6_np21_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32702_f02
ON cipsrv_wbd.wbd_hu6_np21_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_32702_spx
ON cipsrv_wbd.wbd_hu6_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21_32702;

--******************************--
----- materialized views/wbd_hu4_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21_5070(
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
         cipsrv_wbd.wbd_hu6_np21_5070 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_5070_pk
ON cipsrv_wbd.wbd_hu4_np21_5070(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_5070_u01
ON cipsrv_wbd.wbd_hu4_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_5070_u02
ON cipsrv_wbd.wbd_hu4_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_5070_f01
ON cipsrv_wbd.wbd_hu4_np21_5070(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_5070_spx
ON cipsrv_wbd.wbd_hu4_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21_5070;

--******************************--
----- materialized views/wbd_hu4_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21_3338(
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
         cipsrv_wbd.wbd_hu6_np21_3338 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_3338_pk
ON cipsrv_wbd.wbd_hu4_np21_3338(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_3338_u01
ON cipsrv_wbd.wbd_hu4_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_3338_u02
ON cipsrv_wbd.wbd_hu4_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_3338_f01
ON cipsrv_wbd.wbd_hu4_np21_3338(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_3338_spx
ON cipsrv_wbd.wbd_hu4_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21_3338;

--******************************--
----- materialized views/wbd_hu4_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21_26904(
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
         cipsrv_wbd.wbd_hu6_np21_26904 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_26904_pk
ON cipsrv_wbd.wbd_hu4_np21_26904(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_26904_u01
ON cipsrv_wbd.wbd_hu4_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_26904_u02
ON cipsrv_wbd.wbd_hu4_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_26904_f01
ON cipsrv_wbd.wbd_hu4_np21_26904(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_26904_spx
ON cipsrv_wbd.wbd_hu4_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21_26904;

--******************************--
----- materialized views/wbd_hu4_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21_32161(
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
         cipsrv_wbd.wbd_hu6_np21_32161 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32161_pk
ON cipsrv_wbd.wbd_hu4_np21_32161(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32161_u01
ON cipsrv_wbd.wbd_hu4_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32161_u02
ON cipsrv_wbd.wbd_hu4_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_32161_f01
ON cipsrv_wbd.wbd_hu4_np21_32161(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_32161_spx
ON cipsrv_wbd.wbd_hu4_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21_32161;

--******************************--
----- materialized views/wbd_hu4_np21_32655.sql 

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

--******************************--
----- materialized views/wbd_hu4_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21_32702(
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
         cipsrv_wbd.wbd_hu6_np21_32702 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32702_pk
ON cipsrv_wbd.wbd_hu4_np21_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32702_u01
ON cipsrv_wbd.wbd_hu4_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_32702_u02
ON cipsrv_wbd.wbd_hu4_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_32702_f01
ON cipsrv_wbd.wbd_hu4_np21_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_32702_spx
ON cipsrv_wbd.wbd_hu4_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21_32702;

--******************************--
----- materialized views/wbd_hu2_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_np21_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_5070_pk
ON cipsrv_wbd.wbd_hu2_np21_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_5070_u01
ON cipsrv_wbd.wbd_hu2_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_5070_u02
ON cipsrv_wbd.wbd_hu2_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_np21_5070_spx
ON cipsrv_wbd.wbd_hu2_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_np21_5070;

--******************************--
----- materialized views/wbd_hu2_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_np21_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_3338_pk
ON cipsrv_wbd.wbd_hu2_np21_3338(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_3338_u01
ON cipsrv_wbd.wbd_hu2_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_3338_u02
ON cipsrv_wbd.wbd_hu2_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_np21_3338_spx
ON cipsrv_wbd.wbd_hu2_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_np21_3338;

--******************************--
----- materialized views/wbd_hu2_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_np21_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_26904_pk
ON cipsrv_wbd.wbd_hu2_np21_26904(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_26904_u01
ON cipsrv_wbd.wbd_hu2_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_26904_u02
ON cipsrv_wbd.wbd_hu2_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_np21_26904_spx
ON cipsrv_wbd.wbd_hu2_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_np21_26904;

--******************************--
----- materialized views/wbd_hu2_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_np21_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_32161_pk
ON cipsrv_wbd.wbd_hu2_np21_32161(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_32161_u01
ON cipsrv_wbd.wbd_hu2_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_32161_u02
ON cipsrv_wbd.wbd_hu2_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_np21_32161_spx
ON cipsrv_wbd.wbd_hu2_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_np21_32161;

--******************************--
----- materialized views/wbd_hu10_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc10
,a.hutype
,a.humod
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc10
   ,aa.hutype
   ,aa.humod
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc10
   ,bb.hutype
   ,bb.humod
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc10
   ,cc.hutype
   ,cc.humod
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc10
   ,dd.hutype
   ,dd.humod
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc10
   ,ee.hutype
   ,ee.humod
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc10
   ,ff.hutype
   ,ff.humod
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_np21_32702 ff
   UNION ALL
   SELECT
    gg.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,gg.areasqkm
   ,gg.areaacres
   ,gg.name
   ,gg.states
   ,gg.huc10
   ,gg.hutype
   ,gg.humod
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       bbb.tnmid
      ,ggg.metasourceid
      ,ggg.loaddate
      ,ggg.areasqkm
      ,ggg.areaacres
      ,bbb.name
      ,ggg.states
      ,SUBSTR(ggg.huc12,1,10) AS huc10
      ,ggg.hutype
      ,ggg.humod
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,ggg.shape
      FROM
      cipsrv_wbd.wbd_hu12_np21 ggg
      LEFT JOIN
      cipsrv_wbd.wbd_names bbb
      ON
      bbb.huc = SUBSTR(ggg.huc12,1,10)
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
   ) gg
) a;

ALTER TABLE cipsrv_wbd.wbd_hu10_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_pk
ON cipsrv_wbd.wbd_hu10_np21(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_u01
ON cipsrv_wbd.wbd_hu10_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_np21_u02
ON cipsrv_wbd.wbd_hu10_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_f01
ON cipsrv_wbd.wbd_hu10_np21(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_f02
ON cipsrv_wbd.wbd_hu10_np21(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_f03
ON cipsrv_wbd.wbd_hu10_np21(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_f04
ON cipsrv_wbd.wbd_hu10_np21(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_np21_spx
ON cipsrv_wbd.wbd_hu10_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_np21;

--******************************--
----- materialized views/wbd_hu8_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_pk
ON cipsrv_wbd.wbd_hu8_np21(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_u01
ON cipsrv_wbd.wbd_hu8_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_np21_u02
ON cipsrv_wbd.wbd_hu8_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_f01
ON cipsrv_wbd.wbd_hu8_np21(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_f02
ON cipsrv_wbd.wbd_hu8_np21(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_f03
ON cipsrv_wbd.wbd_hu8_np21(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_np21_spx
ON cipsrv_wbd.wbd_hu8_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21;

--******************************--
----- materialized views/wbd_hu6_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_pk
ON cipsrv_wbd.wbd_hu6_np21(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_u01
ON cipsrv_wbd.wbd_hu6_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_np21_u02
ON cipsrv_wbd.wbd_hu6_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_f01
ON cipsrv_wbd.wbd_hu6_np21(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_f02
ON cipsrv_wbd.wbd_hu6_np21(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_np21_spx
ON cipsrv_wbd.wbd_hu6_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_np21;


--******************************--
----- materialized views/wbd_hu4_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_np21(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_pk
ON cipsrv_wbd.wbd_hu4_np21(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_u01
ON cipsrv_wbd.wbd_hu4_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_np21_u02
ON cipsrv_wbd.wbd_hu4_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_f01
ON cipsrv_wbd.wbd_hu4_np21(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_np21_spx
ON cipsrv_wbd.wbd_hu4_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_np21;


--******************************--
----- materialized views/wbd_hu2_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_np21_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) AS shape
        FROM (
           SELECT
           ST_TRANSFORM(a2.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4_np21_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4_np21_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12_np21 ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_pk
ON cipsrv_wbd.wbd_hu2_np21(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_u01
ON cipsrv_wbd.wbd_hu2_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_np21_u02
ON cipsrv_wbd.wbd_hu2_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_np21_spx
ON cipsrv_wbd.wbd_hu2_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_np21;


--******************************--
----- materialized views/wbd_hu12_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_pk
ON cipsrv_wbd.wbd_hu12_nphr_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_u01
ON cipsrv_wbd.wbd_hu12_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_u02
ON cipsrv_wbd.wbd_hu12_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_f01
ON cipsrv_wbd.wbd_hu12_nphr_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_f02
ON cipsrv_wbd.wbd_hu12_nphr_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_f03
ON cipsrv_wbd.wbd_hu12_nphr_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_f04
ON cipsrv_wbd.wbd_hu12_nphr_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_f05
ON cipsrv_wbd.wbd_hu12_nphr_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_5070_spx
ON cipsrv_wbd.wbd_hu12_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_nphr_5070;

--******************************--
----- materialized views/wbd_hu12_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_pk
ON cipsrv_wbd.wbd_hu12_nphr_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_u01
ON cipsrv_wbd.wbd_hu12_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_u02
ON cipsrv_wbd.wbd_hu12_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_f01
ON cipsrv_wbd.wbd_hu12_nphr_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_f02
ON cipsrv_wbd.wbd_hu12_nphr_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_f03
ON cipsrv_wbd.wbd_hu12_nphr_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_f04
ON cipsrv_wbd.wbd_hu12_nphr_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_f05
ON cipsrv_wbd.wbd_hu12_nphr_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_3338_spx
ON cipsrv_wbd.wbd_hu12_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_nphr_3338;

--******************************--
----- materialized views/wbd_hu12_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipsrv_wbd.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipsrv_wbd.wbd_hu12_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_pk
ON cipsrv_wbd.wbd_hu12_nphr_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_u01
ON cipsrv_wbd.wbd_hu12_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_u02
ON cipsrv_wbd.wbd_hu12_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_f01
ON cipsrv_wbd.wbd_hu12_nphr_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_f02
ON cipsrv_wbd.wbd_hu12_nphr_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_f03
ON cipsrv_wbd.wbd_hu12_nphr_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_f04
ON cipsrv_wbd.wbd_hu12_nphr_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_f05
ON cipsrv_wbd.wbd_hu12_nphr_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_26904_spx
ON cipsrv_wbd.wbd_hu12_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_nphr_26904;

--******************************--
----- materialized views/wbd_hu12_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_pk
ON cipsrv_wbd.wbd_hu12_nphr_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_u01
ON cipsrv_wbd.wbd_hu12_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_u02
ON cipsrv_wbd.wbd_hu12_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_f01
ON cipsrv_wbd.wbd_hu12_nphr_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_f02
ON cipsrv_wbd.wbd_hu12_nphr_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_f03
ON cipsrv_wbd.wbd_hu12_nphr_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_f04
ON cipsrv_wbd.wbd_hu12_nphr_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_f05
ON cipsrv_wbd.wbd_hu12_nphr_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32161_spx
ON cipsrv_wbd.wbd_hu12_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_nphr_32161;

--******************************--
----- materialized views/wbd_hu12_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32655)      AS shape
FROM
cipsrv_wbd.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_wbd.wbd_hu12_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_pk
ON cipsrv_wbd.wbd_hu12_nphr_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_u01
ON cipsrv_wbd.wbd_hu12_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_u02
ON cipsrv_wbd.wbd_hu12_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_f01
ON cipsrv_wbd.wbd_hu12_nphr_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_f02
ON cipsrv_wbd.wbd_hu12_nphr_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_f03
ON cipsrv_wbd.wbd_hu12_nphr_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_f04
ON cipsrv_wbd.wbd_hu12_nphr_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_f05
ON cipsrv_wbd.wbd_hu12_nphr_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32655_spx
ON cipsrv_wbd.wbd_hu12_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_nphr_32655;

--******************************--
----- materialized views/wbd_hu12_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32702)      AS shape
FROM
cipsrv_wbd.wbd_hu12_nphr a
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_wbd.wbd_hu12_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_pk
ON cipsrv_wbd.wbd_hu12_nphr_32702(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_u01
ON cipsrv_wbd.wbd_hu12_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_u02
ON cipsrv_wbd.wbd_hu12_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_f01
ON cipsrv_wbd.wbd_hu12_nphr_32702(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_f02
ON cipsrv_wbd.wbd_hu12_nphr_32702(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_f03
ON cipsrv_wbd.wbd_hu12_nphr_32702(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_f04
ON cipsrv_wbd.wbd_hu12_nphr_32702(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_f05
ON cipsrv_wbd.wbd_hu12_nphr_32702(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_nphr_32702_spx
ON cipsrv_wbd.wbd_hu12_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_nphr_32702;

--******************************--
----- materialized views/wbd_hu10_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_pk
ON cipsrv_wbd.wbd_hu10_nphr_5070(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_u01
ON cipsrv_wbd.wbd_hu10_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_u02
ON cipsrv_wbd.wbd_hu10_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_f01
ON cipsrv_wbd.wbd_hu10_nphr_5070(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_f02
ON cipsrv_wbd.wbd_hu10_nphr_5070(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_f03
ON cipsrv_wbd.wbd_hu10_nphr_5070(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_f04
ON cipsrv_wbd.wbd_hu10_nphr_5070(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_5070_spx
ON cipsrv_wbd.wbd_hu10_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr_5070;

--******************************--
----- materialized views/wbd_hu10_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_pk
ON cipsrv_wbd.wbd_hu10_nphr_3338(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_u01
ON cipsrv_wbd.wbd_hu10_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_u02
ON cipsrv_wbd.wbd_hu10_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_f01
ON cipsrv_wbd.wbd_hu10_nphr_3338(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_f02
ON cipsrv_wbd.wbd_hu10_nphr_3338(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_f03
ON cipsrv_wbd.wbd_hu10_nphr_3338(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_f04
ON cipsrv_wbd.wbd_hu10_nphr_3338(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_3338_spx
ON cipsrv_wbd.wbd_hu10_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr_3338;

--******************************--
----- materialized views/wbd_hu10_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_pk
ON cipsrv_wbd.wbd_hu10_nphr_26904(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_u01
ON cipsrv_wbd.wbd_hu10_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_u02
ON cipsrv_wbd.wbd_hu10_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_f01
ON cipsrv_wbd.wbd_hu10_nphr_26904(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_f02
ON cipsrv_wbd.wbd_hu10_nphr_26904(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_f03
ON cipsrv_wbd.wbd_hu10_nphr_26904(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_f04
ON cipsrv_wbd.wbd_hu10_nphr_26904(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_26904_spx
ON cipsrv_wbd.wbd_hu10_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr_26904;

--******************************--
----- materialized views/wbd_hu10_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_pk
ON cipsrv_wbd.wbd_hu10_nphr_32161(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_u01
ON cipsrv_wbd.wbd_hu10_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_u02
ON cipsrv_wbd.wbd_hu10_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_f01
ON cipsrv_wbd.wbd_hu10_nphr_32161(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_f02
ON cipsrv_wbd.wbd_hu10_nphr_32161(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_f03
ON cipsrv_wbd.wbd_hu10_nphr_32161(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_f04
ON cipsrv_wbd.wbd_hu10_nphr_32161(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32161_spx
ON cipsrv_wbd.wbd_hu10_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr_32161;

--******************************--
----- materialized views/wbd_hu10_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_pk
ON cipsrv_wbd.wbd_hu10_nphr_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_u01
ON cipsrv_wbd.wbd_hu10_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_u02
ON cipsrv_wbd.wbd_hu10_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_f01
ON cipsrv_wbd.wbd_hu10_nphr_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_f02
ON cipsrv_wbd.wbd_hu10_nphr_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_f03
ON cipsrv_wbd.wbd_hu10_nphr_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_f04
ON cipsrv_wbd.wbd_hu10_nphr_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32655_spx
ON cipsrv_wbd.wbd_hu10_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr_32655;

--******************************--
----- materialized views/wbd_hu10_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_nphr_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_pk
ON cipsrv_wbd.wbd_hu10_nphr_32702(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_u01
ON cipsrv_wbd.wbd_hu10_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_u02
ON cipsrv_wbd.wbd_hu10_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_f01
ON cipsrv_wbd.wbd_hu10_nphr_32702(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_f02
ON cipsrv_wbd.wbd_hu10_nphr_32702(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_f03
ON cipsrv_wbd.wbd_hu10_nphr_32702(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_f04
ON cipsrv_wbd.wbd_hu10_nphr_32702(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_32702_spx
ON cipsrv_wbd.wbd_hu10_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr_32702;

--******************************--
----- materialized views/wbd_hu8_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_nphr_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_pk
ON cipsrv_wbd.wbd_hu8_nphr_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_u01
ON cipsrv_wbd.wbd_hu8_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_u02
ON cipsrv_wbd.wbd_hu8_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_f01
ON cipsrv_wbd.wbd_hu8_nphr_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_f02
ON cipsrv_wbd.wbd_hu8_nphr_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_f03
ON cipsrv_wbd.wbd_hu8_nphr_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_5070_spx
ON cipsrv_wbd.wbd_hu8_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr_5070;

--******************************--
----- materialized views/wbd_hu8_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_nphr_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_pk
ON cipsrv_wbd.wbd_hu8_nphr_3338(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_u01
ON cipsrv_wbd.wbd_hu8_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_u02
ON cipsrv_wbd.wbd_hu8_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_f01
ON cipsrv_wbd.wbd_hu8_nphr_3338(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_f02
ON cipsrv_wbd.wbd_hu8_nphr_3338(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_f03
ON cipsrv_wbd.wbd_hu8_nphr_3338(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_3338_spx
ON cipsrv_wbd.wbd_hu8_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr_3338;

--******************************--
----- materialized views/wbd_hu8_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_nphr_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_pk
ON cipsrv_wbd.wbd_hu8_nphr_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_u01
ON cipsrv_wbd.wbd_hu8_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_u02
ON cipsrv_wbd.wbd_hu8_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_f01
ON cipsrv_wbd.wbd_hu8_nphr_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_f02
ON cipsrv_wbd.wbd_hu8_nphr_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_f03
ON cipsrv_wbd.wbd_hu8_nphr_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_26904_spx
ON cipsrv_wbd.wbd_hu8_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr_26904;

--******************************--
----- materialized views/wbd_hu8_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_nphr_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_pk
ON cipsrv_wbd.wbd_hu8_nphr_32161(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_u01
ON cipsrv_wbd.wbd_hu8_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_u02
ON cipsrv_wbd.wbd_hu8_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_f01
ON cipsrv_wbd.wbd_hu8_nphr_32161(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_f02
ON cipsrv_wbd.wbd_hu8_nphr_32161(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_f03
ON cipsrv_wbd.wbd_hu8_nphr_32161(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32161_spx
ON cipsrv_wbd.wbd_hu8_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr_32161;

--******************************--
----- materialized views/wbd_hu8_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_nphr_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_pk
ON cipsrv_wbd.wbd_hu8_nphr_32655(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_u01
ON cipsrv_wbd.wbd_hu8_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_u02
ON cipsrv_wbd.wbd_hu8_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_f01
ON cipsrv_wbd.wbd_hu8_nphr_32655(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_f02
ON cipsrv_wbd.wbd_hu8_nphr_32655(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_f03
ON cipsrv_wbd.wbd_hu8_nphr_32655(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32655_spx
ON cipsrv_wbd.wbd_hu8_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr_32655;

--******************************--
----- materialized views/wbd_hu8_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_nphr_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_pk
ON cipsrv_wbd.wbd_hu8_nphr_32702(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_u01
ON cipsrv_wbd.wbd_hu8_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_u02
ON cipsrv_wbd.wbd_hu8_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_f01
ON cipsrv_wbd.wbd_hu8_nphr_32702(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_f02
ON cipsrv_wbd.wbd_hu8_nphr_32702(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_f03
ON cipsrv_wbd.wbd_hu8_nphr_32702(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_32702_spx
ON cipsrv_wbd.wbd_hu8_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr_32702;

--******************************--
----- materialized views/wbd_hu6_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_nphr_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_5070_pk
ON cipsrv_wbd.wbd_hu6_nphr_5070(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_5070_u01
ON cipsrv_wbd.wbd_hu6_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_5070_u02
ON cipsrv_wbd.wbd_hu6_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_5070_f01
ON cipsrv_wbd.wbd_hu6_nphr_5070(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_5070_f02
ON cipsrv_wbd.wbd_hu6_nphr_5070(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_5070_spx
ON cipsrv_wbd.wbd_hu6_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr_5070;

--******************************--
----- materialized views/wbd_hu6_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_nphr_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_3338_pk
ON cipsrv_wbd.wbd_hu6_nphr_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_3338_u01
ON cipsrv_wbd.wbd_hu6_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_3338_u02
ON cipsrv_wbd.wbd_hu6_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_3338_f01
ON cipsrv_wbd.wbd_hu6_nphr_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_3338_f02
ON cipsrv_wbd.wbd_hu6_nphr_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_3338_spx
ON cipsrv_wbd.wbd_hu6_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr_3338;

--******************************--
----- materialized views/wbd_hu6_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_nphr_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_26904_pk
ON cipsrv_wbd.wbd_hu6_nphr_26904(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_26904_u01
ON cipsrv_wbd.wbd_hu6_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_26904_u02
ON cipsrv_wbd.wbd_hu6_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_26904_f01
ON cipsrv_wbd.wbd_hu6_nphr_26904(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_26904_f02
ON cipsrv_wbd.wbd_hu6_nphr_26904(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_26904_spx
ON cipsrv_wbd.wbd_hu6_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr_26904;

--******************************--
----- materialized views/wbd_hu6_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_nphr_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32161_pk
ON cipsrv_wbd.wbd_hu6_nphr_32161(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32161_u01
ON cipsrv_wbd.wbd_hu6_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32161_u02
ON cipsrv_wbd.wbd_hu6_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32161_f01
ON cipsrv_wbd.wbd_hu6_nphr_32161(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32161_f02
ON cipsrv_wbd.wbd_hu6_nphr_32161(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32161_spx
ON cipsrv_wbd.wbd_hu6_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr_32161;

--******************************--
----- materialized views/wbd_hu6_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_nphr_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32655_pk
ON cipsrv_wbd.wbd_hu6_nphr_32655(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32655_u01
ON cipsrv_wbd.wbd_hu6_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32655_u02
ON cipsrv_wbd.wbd_hu6_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32655_f01
ON cipsrv_wbd.wbd_hu6_nphr_32655(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32655_f02
ON cipsrv_wbd.wbd_hu6_nphr_32655(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32655_spx
ON cipsrv_wbd.wbd_hu6_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr_32655;

--******************************--
----- materialized views/wbd_hu6_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_nphr_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32702_pk
ON cipsrv_wbd.wbd_hu6_nphr_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32702_u01
ON cipsrv_wbd.wbd_hu6_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_32702_u02
ON cipsrv_wbd.wbd_hu6_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32702_f01
ON cipsrv_wbd.wbd_hu6_nphr_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32702_f02
ON cipsrv_wbd.wbd_hu6_nphr_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_32702_spx
ON cipsrv_wbd.wbd_hu6_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr_32702;

--******************************--
----- materialized views/wbd_hu4_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr_5070(
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
         cipsrv_wbd.wbd_hu6_nphr_5070 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_5070_pk
ON cipsrv_wbd.wbd_hu4_nphr_5070(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_5070_u01
ON cipsrv_wbd.wbd_hu4_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_5070_u02
ON cipsrv_wbd.wbd_hu4_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_5070_f01
ON cipsrv_wbd.wbd_hu4_nphr_5070(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_5070_spx
ON cipsrv_wbd.wbd_hu4_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr_5070;

--******************************--
----- materialized views/wbd_hu4_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr_3338(
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
         cipsrv_wbd.wbd_hu6_nphr_3338 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_3338_pk
ON cipsrv_wbd.wbd_hu4_nphr_3338(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_3338_u01
ON cipsrv_wbd.wbd_hu4_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_3338_u02
ON cipsrv_wbd.wbd_hu4_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_3338_f01
ON cipsrv_wbd.wbd_hu4_nphr_3338(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_3338_spx
ON cipsrv_wbd.wbd_hu4_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr_3338;

--******************************--
----- materialized views/wbd_hu4_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr_26904(
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
         cipsrv_wbd.wbd_hu6_nphr_26904 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_26904_pk
ON cipsrv_wbd.wbd_hu4_nphr_26904(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_26904_u01
ON cipsrv_wbd.wbd_hu4_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_26904_u02
ON cipsrv_wbd.wbd_hu4_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_26904_f01
ON cipsrv_wbd.wbd_hu4_nphr_26904(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_26904_spx
ON cipsrv_wbd.wbd_hu4_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr_26904;

--******************************--
----- materialized views/wbd_hu4_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr_32161(
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
         cipsrv_wbd.wbd_hu6_nphr_32161 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32161_pk
ON cipsrv_wbd.wbd_hu4_nphr_32161(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32161_u01
ON cipsrv_wbd.wbd_hu4_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32161_u02
ON cipsrv_wbd.wbd_hu4_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_32161_f01
ON cipsrv_wbd.wbd_hu4_nphr_32161(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_32161_spx
ON cipsrv_wbd.wbd_hu4_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr_32161;

--******************************--
----- materialized views/wbd_hu4_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr_32655(
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
         cipsrv_wbd.wbd_hu6_nphr_32655 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32655_pk
ON cipsrv_wbd.wbd_hu4_nphr_32655(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32655_u01
ON cipsrv_wbd.wbd_hu4_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32655_u02
ON cipsrv_wbd.wbd_hu4_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_32655_f01
ON cipsrv_wbd.wbd_hu4_nphr_32655(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_32655_spx
ON cipsrv_wbd.wbd_hu4_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr_32655;

--******************************--
----- materialized views/wbd_hu4_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr_32702(
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
         cipsrv_wbd.wbd_hu6_nphr_32702 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32702_pk
ON cipsrv_wbd.wbd_hu4_nphr_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32702_u01
ON cipsrv_wbd.wbd_hu4_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_32702_u02
ON cipsrv_wbd.wbd_hu4_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_32702_f01
ON cipsrv_wbd.wbd_hu4_nphr_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_32702_spx
ON cipsrv_wbd.wbd_hu4_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr_32702;

--******************************--
----- materialized views/wbd_hu2_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_nphr_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_5070_pk
ON cipsrv_wbd.wbd_hu2_nphr_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_5070_u01
ON cipsrv_wbd.wbd_hu2_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_5070_u02
ON cipsrv_wbd.wbd_hu2_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_nphr_5070_spx
ON cipsrv_wbd.wbd_hu2_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_nphr_5070;

--******************************--
----- materialized views/wbd_hu2_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_nphr_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_3338_pk
ON cipsrv_wbd.wbd_hu2_nphr_3338(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_3338_u01
ON cipsrv_wbd.wbd_hu2_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_3338_u02
ON cipsrv_wbd.wbd_hu2_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_nphr_3338_spx
ON cipsrv_wbd.wbd_hu2_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_nphr_3338;

--******************************--
----- materialized views/wbd_hu2_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_nphr_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_26904_pk
ON cipsrv_wbd.wbd_hu2_nphr_26904(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_26904_u01
ON cipsrv_wbd.wbd_hu2_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_26904_u02
ON cipsrv_wbd.wbd_hu2_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_nphr_26904_spx
ON cipsrv_wbd.wbd_hu2_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_nphr_26904;

--******************************--
----- materialized views/wbd_hu2_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_nphr_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_32161_pk
ON cipsrv_wbd.wbd_hu2_nphr_32161(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_32161_u01
ON cipsrv_wbd.wbd_hu2_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_32161_u02
ON cipsrv_wbd.wbd_hu2_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_nphr_32161_spx
ON cipsrv_wbd.wbd_hu2_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_nphr_32161;

--******************************--
----- materialized views/wbd_hu10_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc10
,a.hutype
,a.humod
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc10
   ,aa.hutype
   ,aa.humod
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc10
   ,bb.hutype
   ,bb.humod
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc10
   ,cc.hutype
   ,cc.humod
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc10
   ,dd.hutype
   ,dd.humod
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc10
   ,ee.hutype
   ,ee.humod
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc10
   ,ff.hutype
   ,ff.humod
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu10_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_pk
ON cipsrv_wbd.wbd_hu10_nphr(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_u01
ON cipsrv_wbd.wbd_hu10_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_nphr_u02
ON cipsrv_wbd.wbd_hu10_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_f01
ON cipsrv_wbd.wbd_hu10_nphr(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_f02
ON cipsrv_wbd.wbd_hu10_nphr(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_f03
ON cipsrv_wbd.wbd_hu10_nphr(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_f04
ON cipsrv_wbd.wbd_hu10_nphr(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_nphr_spx
ON cipsrv_wbd.wbd_hu10_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_nphr;

--******************************--
----- materialized views/wbd_hu8_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_pk
ON cipsrv_wbd.wbd_hu8_nphr(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_u01
ON cipsrv_wbd.wbd_hu8_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_nphr_u02
ON cipsrv_wbd.wbd_hu8_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_f01
ON cipsrv_wbd.wbd_hu8_nphr(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_f02
ON cipsrv_wbd.wbd_hu8_nphr(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_f03
ON cipsrv_wbd.wbd_hu8_nphr(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_nphr_spx
ON cipsrv_wbd.wbd_hu8_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr;

--******************************--
----- materialized views/wbd_hu6_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_pk
ON cipsrv_wbd.wbd_hu6_nphr(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_u01
ON cipsrv_wbd.wbd_hu6_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_nphr_u02
ON cipsrv_wbd.wbd_hu6_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_f01
ON cipsrv_wbd.wbd_hu6_nphr(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_f02
ON cipsrv_wbd.wbd_hu6_nphr(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_nphr_spx
ON cipsrv_wbd.wbd_hu6_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_nphr;


--******************************--
----- materialized views/wbd_hu4_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_nphr(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_pk
ON cipsrv_wbd.wbd_hu4_nphr(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_u01
ON cipsrv_wbd.wbd_hu4_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_nphr_u02
ON cipsrv_wbd.wbd_hu4_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_f01
ON cipsrv_wbd.wbd_hu4_nphr(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_nphr_spx
ON cipsrv_wbd.wbd_hu4_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_nphr;


--******************************--
----- materialized views/wbd_hu2_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_nphr_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) 
        FROM (
           SELECT
           ST_TRANSFORM(a2.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4_nphr_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4_nphr_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12_nphr ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_pk
ON cipsrv_wbd.wbd_hu2_nphr(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_u01
ON cipsrv_wbd.wbd_hu2_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_nphr_u02
ON cipsrv_wbd.wbd_hu2_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_nphr_spx
ON cipsrv_wbd.wbd_hu2_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_nphr;


--******************************--
----- materialized views/wbd_hu12_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_5070_pk
ON cipsrv_wbd.wbd_hu12_f3_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_5070_u01
ON cipsrv_wbd.wbd_hu12_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_5070_u02
ON cipsrv_wbd.wbd_hu12_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_5070_f01
ON cipsrv_wbd.wbd_hu12_f3_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_5070_f02
ON cipsrv_wbd.wbd_hu12_f3_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_5070_f03
ON cipsrv_wbd.wbd_hu12_f3_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_5070_f04
ON cipsrv_wbd.wbd_hu12_f3_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_5070_f05
ON cipsrv_wbd.wbd_hu12_f3_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_5070_spx
ON cipsrv_wbd.wbd_hu12_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_5070;

--******************************--
----- materialized views/wbd_hu12_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_3338_pk
ON cipsrv_wbd.wbd_hu12_f3_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_3338_u01
ON cipsrv_wbd.wbd_hu12_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_3338_u02
ON cipsrv_wbd.wbd_hu12_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f01
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f02
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f03
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f04
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_f05
ON cipsrv_wbd.wbd_hu12_f3_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_3338_spx
ON cipsrv_wbd.wbd_hu12_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_3338;

--******************************--
----- materialized views/wbd_hu12_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_26904_pk
ON cipsrv_wbd.wbd_hu12_f3_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_26904_u01
ON cipsrv_wbd.wbd_hu12_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_26904_u02
ON cipsrv_wbd.wbd_hu12_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_26904_f01
ON cipsrv_wbd.wbd_hu12_f3_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_26904_f02
ON cipsrv_wbd.wbd_hu12_f3_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_26904_f03
ON cipsrv_wbd.wbd_hu12_f3_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_26904_f04
ON cipsrv_wbd.wbd_hu12_f3_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_26904_f05
ON cipsrv_wbd.wbd_hu12_f3_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_26904_spx
ON cipsrv_wbd.wbd_hu12_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_26904;

--******************************--
----- materialized views/wbd_hu12_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32161_pk
ON cipsrv_wbd.wbd_hu12_f3_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32161_u01
ON cipsrv_wbd.wbd_hu12_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32161_u02
ON cipsrv_wbd.wbd_hu12_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32161_f01
ON cipsrv_wbd.wbd_hu12_f3_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32161_f02
ON cipsrv_wbd.wbd_hu12_f3_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32161_f03
ON cipsrv_wbd.wbd_hu12_f3_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32161_f04
ON cipsrv_wbd.wbd_hu12_f3_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32161_f05
ON cipsrv_wbd.wbd_hu12_f3_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32161_spx
ON cipsrv_wbd.wbd_hu12_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_32161;

--******************************--
----- materialized views/wbd_hu12_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32655)      AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32655_pk
ON cipsrv_wbd.wbd_hu12_f3_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32655_u01
ON cipsrv_wbd.wbd_hu12_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32655_u02
ON cipsrv_wbd.wbd_hu12_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32655_f01
ON cipsrv_wbd.wbd_hu12_f3_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32655_f02
ON cipsrv_wbd.wbd_hu12_f3_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32655_f03
ON cipsrv_wbd.wbd_hu12_f3_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32655_f04
ON cipsrv_wbd.wbd_hu12_f3_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32655_f05
ON cipsrv_wbd.wbd_hu12_f3_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32655_spx
ON cipsrv_wbd.wbd_hu12_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_32655;

--******************************--
----- materialized views/wbd_hu12_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,a.centermass_x
,a.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32702)      AS shape
FROM
cipsrv_wbd.wbd_hu12_f3 a
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_wbd.wbd_hu12_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32702_pk
ON cipsrv_wbd.wbd_hu12_f3_32702(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32702_u01
ON cipsrv_wbd.wbd_hu12_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12_f3_32702_u02
ON cipsrv_wbd.wbd_hu12_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32702_f01
ON cipsrv_wbd.wbd_hu12_f3_32702(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32702_f02
ON cipsrv_wbd.wbd_hu12_f3_32702(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32702_f03
ON cipsrv_wbd.wbd_hu12_f3_32702(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32702_f04
ON cipsrv_wbd.wbd_hu12_f3_32702(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32702_f05
ON cipsrv_wbd.wbd_hu12_f3_32702(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12_f3_32702_spx
ON cipsrv_wbd.wbd_hu12_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12_f3_32702;

--******************************--
----- materialized views/wbd_hu10_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_f3_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_5070_pk
ON cipsrv_wbd.wbd_hu10_f3_5070(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_5070_u01
ON cipsrv_wbd.wbd_hu10_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_5070_u02
ON cipsrv_wbd.wbd_hu10_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_5070_f01
ON cipsrv_wbd.wbd_hu10_f3_5070(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_5070_f02
ON cipsrv_wbd.wbd_hu10_f3_5070(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_5070_f03
ON cipsrv_wbd.wbd_hu10_f3_5070(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_5070_f04
ON cipsrv_wbd.wbd_hu10_f3_5070(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_5070_spx
ON cipsrv_wbd.wbd_hu10_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3_5070;

--******************************--
----- materialized views/wbd_hu10_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_f3_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_3338_pk
ON cipsrv_wbd.wbd_hu10_f3_3338(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_3338_u01
ON cipsrv_wbd.wbd_hu10_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_3338_u02
ON cipsrv_wbd.wbd_hu10_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_3338_f01
ON cipsrv_wbd.wbd_hu10_f3_3338(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_3338_f02
ON cipsrv_wbd.wbd_hu10_f3_3338(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_3338_f03
ON cipsrv_wbd.wbd_hu10_f3_3338(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_3338_f04
ON cipsrv_wbd.wbd_hu10_f3_3338(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_3338_spx
ON cipsrv_wbd.wbd_hu10_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3_3338;

--******************************--
----- materialized views/wbd_hu10_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_f3_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_26904_pk
ON cipsrv_wbd.wbd_hu10_f3_26904(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_26904_u01
ON cipsrv_wbd.wbd_hu10_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_26904_u02
ON cipsrv_wbd.wbd_hu10_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_26904_f01
ON cipsrv_wbd.wbd_hu10_f3_26904(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_26904_f02
ON cipsrv_wbd.wbd_hu10_f3_26904(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_26904_f03
ON cipsrv_wbd.wbd_hu10_f3_26904(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_26904_f04
ON cipsrv_wbd.wbd_hu10_f3_26904(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_26904_spx
ON cipsrv_wbd.wbd_hu10_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3_26904;

--******************************--
----- materialized views/wbd_hu10_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_f3_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32161_pk
ON cipsrv_wbd.wbd_hu10_f3_32161(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32161_u01
ON cipsrv_wbd.wbd_hu10_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32161_u02
ON cipsrv_wbd.wbd_hu10_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32161_f01
ON cipsrv_wbd.wbd_hu10_f3_32161(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32161_f02
ON cipsrv_wbd.wbd_hu10_f3_32161(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32161_f03
ON cipsrv_wbd.wbd_hu10_f3_32161(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32161_f04
ON cipsrv_wbd.wbd_hu10_f3_32161(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32161_spx
ON cipsrv_wbd.wbd_hu10_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3_32161;

--******************************--
----- materialized views/wbd_hu10_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_f3_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32655_pk
ON cipsrv_wbd.wbd_hu10_f3_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32655_u01
ON cipsrv_wbd.wbd_hu10_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32655_u02
ON cipsrv_wbd.wbd_hu10_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32655_f01
ON cipsrv_wbd.wbd_hu10_f3_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32655_f02
ON cipsrv_wbd.wbd_hu10_f3_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32655_f03
ON cipsrv_wbd.wbd_hu10_f3_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32655_f04
ON cipsrv_wbd.wbd_hu10_f3_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32655_spx
ON cipsrv_wbd.wbd_hu10_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3_32655;

--******************************--
----- materialized views/wbd_hu10_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc12
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu12_f3_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32702_pk
ON cipsrv_wbd.wbd_hu10_f3_32702(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32702_u01
ON cipsrv_wbd.wbd_hu10_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_32702_u02
ON cipsrv_wbd.wbd_hu10_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32702_f01
ON cipsrv_wbd.wbd_hu10_f3_32702(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32702_f02
ON cipsrv_wbd.wbd_hu10_f3_32702(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32702_f03
ON cipsrv_wbd.wbd_hu10_f3_32702(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32702_f04
ON cipsrv_wbd.wbd_hu10_f3_32702(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_32702_spx
ON cipsrv_wbd.wbd_hu10_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3_32702;

--******************************--
----- materialized views/wbd_hu8_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_f3_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_5070_pk
ON cipsrv_wbd.wbd_hu8_f3_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_5070_u01
ON cipsrv_wbd.wbd_hu8_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_5070_u02
ON cipsrv_wbd.wbd_hu8_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_5070_f01
ON cipsrv_wbd.wbd_hu8_f3_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_5070_f02
ON cipsrv_wbd.wbd_hu8_f3_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_5070_f03
ON cipsrv_wbd.wbd_hu8_f3_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_5070_spx
ON cipsrv_wbd.wbd_hu8_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3_5070;

--******************************--
----- materialized views/wbd_hu8_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_f3_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_3338_pk
ON cipsrv_wbd.wbd_hu8_f3_3338(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_3338_u01
ON cipsrv_wbd.wbd_hu8_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_3338_u02
ON cipsrv_wbd.wbd_hu8_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_3338_f01
ON cipsrv_wbd.wbd_hu8_f3_3338(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_3338_f02
ON cipsrv_wbd.wbd_hu8_f3_3338(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_3338_f03
ON cipsrv_wbd.wbd_hu8_f3_3338(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_3338_spx
ON cipsrv_wbd.wbd_hu8_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3_3338;

--******************************--
----- materialized views/wbd_hu8_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_f3_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_26904_pk
ON cipsrv_wbd.wbd_hu8_f3_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_26904_u01
ON cipsrv_wbd.wbd_hu8_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_26904_u02
ON cipsrv_wbd.wbd_hu8_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_26904_f01
ON cipsrv_wbd.wbd_hu8_f3_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_26904_f02
ON cipsrv_wbd.wbd_hu8_f3_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_26904_f03
ON cipsrv_wbd.wbd_hu8_f3_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_26904_spx
ON cipsrv_wbd.wbd_hu8_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3_26904;

--******************************--
----- materialized views/wbd_hu8_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_f3_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32161_pk
ON cipsrv_wbd.wbd_hu8_f3_32161(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32161_u01
ON cipsrv_wbd.wbd_hu8_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32161_u02
ON cipsrv_wbd.wbd_hu8_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32161_f01
ON cipsrv_wbd.wbd_hu8_f3_32161(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32161_f02
ON cipsrv_wbd.wbd_hu8_f3_32161(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32161_f03
ON cipsrv_wbd.wbd_hu8_f3_32161(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32161_spx
ON cipsrv_wbd.wbd_hu8_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3_32161;

--******************************--
----- materialized views/wbd_hu8_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_f3_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32655_pk
ON cipsrv_wbd.wbd_hu8_f3_32655(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32655_u01
ON cipsrv_wbd.wbd_hu8_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32655_u02
ON cipsrv_wbd.wbd_hu8_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32655_f01
ON cipsrv_wbd.wbd_hu8_f3_32655(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32655_f02
ON cipsrv_wbd.wbd_hu8_f3_32655(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32655_f03
ON cipsrv_wbd.wbd_hu8_f3_32655(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32655_spx
ON cipsrv_wbd.wbd_hu8_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3_32655;

--******************************--
----- materialized views/wbd_hu8_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,a.huc8
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc10
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu10_f3_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32702_pk
ON cipsrv_wbd.wbd_hu8_f3_32702(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32702_u01
ON cipsrv_wbd.wbd_hu8_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_32702_u02
ON cipsrv_wbd.wbd_hu8_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32702_f01
ON cipsrv_wbd.wbd_hu8_f3_32702(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32702_f02
ON cipsrv_wbd.wbd_hu8_f3_32702(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32702_f03
ON cipsrv_wbd.wbd_hu8_f3_32702(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_32702_spx
ON cipsrv_wbd.wbd_hu8_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3_32702;

--******************************--
----- materialized views/wbd_hu6_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_f3_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_5070_pk
ON cipsrv_wbd.wbd_hu6_f3_5070(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_5070_u01
ON cipsrv_wbd.wbd_hu6_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_5070_u02
ON cipsrv_wbd.wbd_hu6_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_5070_f01
ON cipsrv_wbd.wbd_hu6_f3_5070(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_5070_f02
ON cipsrv_wbd.wbd_hu6_f3_5070(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_5070_spx
ON cipsrv_wbd.wbd_hu6_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3_5070;

--******************************--
----- materialized views/wbd_hu6_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_f3_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_3338_pk
ON cipsrv_wbd.wbd_hu6_f3_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_3338_u01
ON cipsrv_wbd.wbd_hu6_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_3338_u02
ON cipsrv_wbd.wbd_hu6_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_3338_f01
ON cipsrv_wbd.wbd_hu6_f3_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_3338_f02
ON cipsrv_wbd.wbd_hu6_f3_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_3338_spx
ON cipsrv_wbd.wbd_hu6_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3_3338;

--******************************--
----- materialized views/wbd_hu6_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_f3_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_26904_pk
ON cipsrv_wbd.wbd_hu6_f3_26904(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_26904_u01
ON cipsrv_wbd.wbd_hu6_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_26904_u02
ON cipsrv_wbd.wbd_hu6_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_26904_f01
ON cipsrv_wbd.wbd_hu6_f3_26904(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_26904_f02
ON cipsrv_wbd.wbd_hu6_f3_26904(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_26904_spx
ON cipsrv_wbd.wbd_hu6_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3_26904;

--******************************--
----- materialized views/wbd_hu6_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_f3_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32161_pk
ON cipsrv_wbd.wbd_hu6_f3_32161(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32161_u01
ON cipsrv_wbd.wbd_hu6_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32161_u02
ON cipsrv_wbd.wbd_hu6_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32161_f01
ON cipsrv_wbd.wbd_hu6_f3_32161(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32161_f02
ON cipsrv_wbd.wbd_hu6_f3_32161(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32161_spx
ON cipsrv_wbd.wbd_hu6_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3_32161;

--******************************--
----- materialized views/wbd_hu6_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_f3_32655 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32655_pk
ON cipsrv_wbd.wbd_hu6_f3_32655(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32655_u01
ON cipsrv_wbd.wbd_hu6_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32655_u02
ON cipsrv_wbd.wbd_hu6_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32655_f01
ON cipsrv_wbd.wbd_hu6_f3_32655(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32655_f02
ON cipsrv_wbd.wbd_hu6_f3_32655(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32655_spx
ON cipsrv_wbd.wbd_hu6_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3_32655;

--******************************--
----- materialized views/wbd_hu6_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,a.huc6
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc8
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu8_f3_32702 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32702_pk
ON cipsrv_wbd.wbd_hu6_f3_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32702_u01
ON cipsrv_wbd.wbd_hu6_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_32702_u02
ON cipsrv_wbd.wbd_hu6_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32702_f01
ON cipsrv_wbd.wbd_hu6_f3_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32702_f02
ON cipsrv_wbd.wbd_hu6_f3_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_32702_spx
ON cipsrv_wbd.wbd_hu6_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3_32702;

--******************************--
----- materialized views/wbd_hu4_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3_5070(
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
         cipsrv_wbd.wbd_hu6_f3_5070 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_5070_pk
ON cipsrv_wbd.wbd_hu4_f3_5070(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_5070_u01
ON cipsrv_wbd.wbd_hu4_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_5070_u02
ON cipsrv_wbd.wbd_hu4_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_5070_f01
ON cipsrv_wbd.wbd_hu4_f3_5070(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_5070_spx
ON cipsrv_wbd.wbd_hu4_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3_5070;

--******************************--
----- materialized views/wbd_hu4_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3_3338(
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
         cipsrv_wbd.wbd_hu6_f3_3338 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_3338_pk
ON cipsrv_wbd.wbd_hu4_f3_3338(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_3338_u01
ON cipsrv_wbd.wbd_hu4_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_3338_u02
ON cipsrv_wbd.wbd_hu4_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_3338_f01
ON cipsrv_wbd.wbd_hu4_f3_3338(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_3338_spx
ON cipsrv_wbd.wbd_hu4_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3_3338;

--******************************--
----- materialized views/wbd_hu4_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3_26904(
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
         cipsrv_wbd.wbd_hu6_f3_26904 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_26904_pk
ON cipsrv_wbd.wbd_hu4_f3_26904(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_26904_u01
ON cipsrv_wbd.wbd_hu4_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_26904_u02
ON cipsrv_wbd.wbd_hu4_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_26904_f01
ON cipsrv_wbd.wbd_hu4_f3_26904(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_26904_spx
ON cipsrv_wbd.wbd_hu4_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3_26904;

--******************************--
----- materialized views/wbd_hu4_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3_32161(
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
         cipsrv_wbd.wbd_hu6_f3_32161 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32161_pk
ON cipsrv_wbd.wbd_hu4_f3_32161(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32161_u01
ON cipsrv_wbd.wbd_hu4_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32161_u02
ON cipsrv_wbd.wbd_hu4_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_32161_f01
ON cipsrv_wbd.wbd_hu4_f3_32161(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_32161_spx
ON cipsrv_wbd.wbd_hu4_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3_32161;

--******************************--
----- materialized views/wbd_hu4_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3_32655(
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
         cipsrv_wbd.wbd_hu6_f3_32655 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32655_pk
ON cipsrv_wbd.wbd_hu4_f3_32655(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32655_u01
ON cipsrv_wbd.wbd_hu4_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32655_u02
ON cipsrv_wbd.wbd_hu4_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_32655_f01
ON cipsrv_wbd.wbd_hu4_f3_32655(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_32655_spx
ON cipsrv_wbd.wbd_hu4_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3_32655;

--******************************--
----- materialized views/wbd_hu4_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3_32702(
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
         cipsrv_wbd.wbd_hu6_f3_32702 aaaa
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

ALTER TABLE cipsrv_wbd.wbd_hu4_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32702_pk
ON cipsrv_wbd.wbd_hu4_f3_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32702_u01
ON cipsrv_wbd.wbd_hu4_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_32702_u02
ON cipsrv_wbd.wbd_hu4_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_32702_f01
ON cipsrv_wbd.wbd_hu4_f3_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_32702_spx
ON cipsrv_wbd.wbd_hu4_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3_32702;

--******************************--
----- materialized views/wbd_hu2_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_f3_5070 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_5070_pk
ON cipsrv_wbd.wbd_hu2_f3_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_5070_u01
ON cipsrv_wbd.wbd_hu2_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_5070_u02
ON cipsrv_wbd.wbd_hu2_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_f3_5070_spx
ON cipsrv_wbd.wbd_hu2_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_f3_5070;

--******************************--
----- materialized views/wbd_hu2_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_f3_3338 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_3338_pk
ON cipsrv_wbd.wbd_hu2_f3_3338(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_3338_u01
ON cipsrv_wbd.wbd_hu2_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_3338_u02
ON cipsrv_wbd.wbd_hu2_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_f3_3338_spx
ON cipsrv_wbd.wbd_hu2_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_f3_3338;

--******************************--
----- materialized views/wbd_hu2_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_f3_26904 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_26904_pk
ON cipsrv_wbd.wbd_hu2_f3_26904(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_26904_u01
ON cipsrv_wbd.wbd_hu2_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_26904_u02
ON cipsrv_wbd.wbd_hu2_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_f3_26904_spx
ON cipsrv_wbd.wbd_hu2_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_f3_26904;

--******************************--
----- materialized views/wbd_hu2_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,a.huc2
,ROUND(ST_X(a.centermass)::NUMERIC,8) AS centermass_x
,ROUND(ST_Y(a.centermass)::NUMERIC,8) AS centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ARRAY_REMOVE(aa.states_array,'XX')       AS states_array
   ,ST_AREA(aa.shape) * 0.000001             AS areasqkm
   ,ST_AREA(aa.shape) * 0.000247105          AS areaacres
   ,ST_TRANSFORM(ST_CENTROID(aa.shape),4269) AS centermass
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ARRAY_AGG(DISTINCT u.val) AS states_array
      ,ST_UNION(aaa.shape)       AS shape 
      FROM (
         SELECT
          aaaa.huc4
         ,STRING_TO_ARRAY(
             CASE WHEN aaaa.states IS NULL THEN 'XX' ELSE aaaa.states END
            ,','
          ) AS states_array
         ,aaaa.shape
         FROM
         cipsrv_wbd.wbd_hu4_f3_32161 aaaa
      ) aaa
      CROSS JOIN
	   LATERAL UNNEST(aaa.states_array) AS u(val)
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_32161_pk
ON cipsrv_wbd.wbd_hu2_f3_32161(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_32161_u01
ON cipsrv_wbd.wbd_hu2_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_32161_u02
ON cipsrv_wbd.wbd_hu2_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_f3_32161_spx
ON cipsrv_wbd.wbd_hu2_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_f3_32161;

--******************************--
----- materialized views/wbd_hu10_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc10
,a.hutype
,a.humod
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc10
   ,aa.hutype
   ,aa.humod
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc10
   ,bb.hutype
   ,bb.humod
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc10
   ,cc.hutype
   ,cc.humod
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc10
   ,dd.hutype
   ,dd.humod
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc10
   ,ee.hutype
   ,ee.humod
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc10
   ,ff.hutype
   ,ff.humod
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10_f3_32702 ff
   UNION ALL
   SELECT
    gg.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,gg.areasqkm
   ,gg.areaacres
   ,gg.name
   ,gg.states
   ,gg.huc10
   ,gg.hutype
   ,gg.humod
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       bbb.tnmid
      ,ggg.metasourceid
      ,ggg.loaddate
      ,ggg.areasqkm
      ,ggg.areaacres
      ,bbb.name
      ,ggg.states
      ,SUBSTR(ggg.huc12,1,10) AS huc10
      ,ggg.hutype
      ,ggg.humod
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,ggg.shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 ggg
      LEFT JOIN
      cipsrv_wbd.wbd_names bbb
      ON
      bbb.huc = SUBSTR(ggg.huc12,1,10)
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
   ) gg
) a;

ALTER TABLE cipsrv_wbd.wbd_hu10_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_pk
ON cipsrv_wbd.wbd_hu10_f3(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_u01
ON cipsrv_wbd.wbd_hu10_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10_f3_u02
ON cipsrv_wbd.wbd_hu10_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_f01
ON cipsrv_wbd.wbd_hu10_f3(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_f02
ON cipsrv_wbd.wbd_hu10_f3(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_f03
ON cipsrv_wbd.wbd_hu10_f3(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_f04
ON cipsrv_wbd.wbd_hu10_f3(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10_f3_spx
ON cipsrv_wbd.wbd_hu10_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10_f3;

--******************************--
----- materialized views/wbd_hu8_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8_f3_32702 ff
   UNION ALL
   SELECT
    gg.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,gg.areasqkm
   ,gg.areaacres
   ,gg.name
   ,gg.states
   ,gg.huc8
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       bbb.tnmid
      ,ggg.metasourceid
      ,ggg.loaddate
      ,ggg.areasqkm
      ,ggg.areaacres
      ,bbb.name
      ,ggg.states
      ,SUBSTR(ggg.huc12,1,8) AS huc8
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,ggg.shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 ggg
      LEFT JOIN
      cipsrv_wbd.wbd_names bbb
      ON
      bbb.huc = SUBSTR(ggg.huc12,1,8)
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
   ) gg
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_pk
ON cipsrv_wbd.wbd_hu8_f3(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_u01
ON cipsrv_wbd.wbd_hu8_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8_f3_u02
ON cipsrv_wbd.wbd_hu8_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_f01
ON cipsrv_wbd.wbd_hu8_f3(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_f02
ON cipsrv_wbd.wbd_hu8_f3(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_f03
ON cipsrv_wbd.wbd_hu8_f3(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8_f3_spx
ON cipsrv_wbd.wbd_hu8_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3;

--******************************--
----- materialized views/wbd_hu6_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6_f3_32702 ff
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc6
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'220400' AS huc6
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,(SELECT ST_UNION(gggg.shape) FROM cipsrv_wbd.wbd_hu12_f3 gggg WHERE SUBSTR(gggg.huc12,1,6) = '220400') AS shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,6) = '220400'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc6
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_pk
ON cipsrv_wbd.wbd_hu6_f3(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_u01
ON cipsrv_wbd.wbd_hu6_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6_f3_u02
ON cipsrv_wbd.wbd_hu6_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_f01
ON cipsrv_wbd.wbd_hu6_f3(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_f02
ON cipsrv_wbd.wbd_hu6_f3(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6_f3_spx
ON cipsrv_wbd.wbd_hu6_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6_f3;


--******************************--
----- materialized views/wbd_hu4_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4_f3(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4_f3_32702 ff
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc4
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'2204' AS huc4
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,(SELECT ST_UNION(gggg.shape) FROM cipsrv_wbd.wbd_hu12_f3 gggg WHERE SUBSTR(gggg.huc12,1,4) = '2204') AS shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc4
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_pk
ON cipsrv_wbd.wbd_hu4_f3(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_u01
ON cipsrv_wbd.wbd_hu4_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4_f3_u02
ON cipsrv_wbd.wbd_hu4_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_f01
ON cipsrv_wbd.wbd_hu4_f3(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4_f3_spx
ON cipsrv_wbd.wbd_hu4_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4_f3;


--******************************--
----- materialized views/wbd_hu2_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2_f3_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) 
        FROM (
           SELECT 
           a1.shape 
           FROM 
           cipsrv_wbd.wbd_hu12_f3 a1 
           WHERE 
           SUBSTR(a1.huc12,1,4) = '2204'
           UNION ALL
           SELECT
           ST_TRANSFORM(a2.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4_f3_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4_f3_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_pk
ON cipsrv_wbd.wbd_hu2_f3(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_u01
ON cipsrv_wbd.wbd_hu2_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2_f3_u02
ON cipsrv_wbd.wbd_hu2_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2_f3_spx
ON cipsrv_wbd.wbd_hu2_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2_f3;


--******************************--
----- materialized views/wbd_hu12sp_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu12sp_np21_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu12sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu12sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_f01
ON cipsrv_wbd.wbd_hu12sp_np21_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_f02
ON cipsrv_wbd.wbd_hu12sp_np21_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_f03
ON cipsrv_wbd.wbd_hu12sp_np21_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_f04
ON cipsrv_wbd.wbd_hu12sp_np21_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_f05
ON cipsrv_wbd.wbd_hu12sp_np21_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu12sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_np21_5070;

--******************************--
----- materialized views/wbd_hu12sp_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_pk
ON cipsrv_wbd.wbd_hu12sp_np21_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_u01
ON cipsrv_wbd.wbd_hu12sp_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_u02
ON cipsrv_wbd.wbd_hu12sp_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_f01
ON cipsrv_wbd.wbd_hu12sp_np21_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_f02
ON cipsrv_wbd.wbd_hu12sp_np21_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_f03
ON cipsrv_wbd.wbd_hu12sp_np21_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_f04
ON cipsrv_wbd.wbd_hu12sp_np21_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_f05
ON cipsrv_wbd.wbd_hu12sp_np21_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_3338_spx
ON cipsrv_wbd.wbd_hu12sp_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_np21_3338;

--******************************--
----- materialized views/wbd_hu12sp_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_pk
ON cipsrv_wbd.wbd_hu12sp_np21_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_u01
ON cipsrv_wbd.wbd_hu12sp_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_u02
ON cipsrv_wbd.wbd_hu12sp_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_f01
ON cipsrv_wbd.wbd_hu12sp_np21_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_f02
ON cipsrv_wbd.wbd_hu12sp_np21_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_f03
ON cipsrv_wbd.wbd_hu12sp_np21_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_f04
ON cipsrv_wbd.wbd_hu12sp_np21_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_f05
ON cipsrv_wbd.wbd_hu12sp_np21_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_26904_spx
ON cipsrv_wbd.wbd_hu12sp_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_np21_26904;

--******************************--
----- materialized views/wbd_hu12sp_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_pk
ON cipsrv_wbd.wbd_hu12sp_np21_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_u01
ON cipsrv_wbd.wbd_hu12sp_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_u02
ON cipsrv_wbd.wbd_hu12sp_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_f01
ON cipsrv_wbd.wbd_hu12sp_np21_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_f02
ON cipsrv_wbd.wbd_hu12sp_np21_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_f03
ON cipsrv_wbd.wbd_hu12sp_np21_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_f04
ON cipsrv_wbd.wbd_hu12sp_np21_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_f05
ON cipsrv_wbd.wbd_hu12sp_np21_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32161_spx
ON cipsrv_wbd.wbd_hu12sp_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_np21_32161;

--******************************--
----- materialized views/wbd_hu12sp_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32655)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_pk
ON cipsrv_wbd.wbd_hu12sp_np21_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_u01
ON cipsrv_wbd.wbd_hu12sp_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_u02
ON cipsrv_wbd.wbd_hu12sp_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f01
ON cipsrv_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f02
ON cipsrv_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f03
ON cipsrv_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f04
ON cipsrv_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_f05
ON cipsrv_wbd.wbd_hu12sp_np21_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32655_spx
ON cipsrv_wbd.wbd_hu12sp_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_np21_32655;

--******************************--
----- materialized views/wbd_hu12sp_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32702)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_np21 a
JOIN
cipsrv_wbd.wbd_hu12_np21 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_pk
ON cipsrv_wbd.wbd_hu12sp_np21_32702(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_u01
ON cipsrv_wbd.wbd_hu12sp_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_u02
ON cipsrv_wbd.wbd_hu12sp_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_f01
ON cipsrv_wbd.wbd_hu12sp_np21_32702(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_f02
ON cipsrv_wbd.wbd_hu12sp_np21_32702(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_f03
ON cipsrv_wbd.wbd_hu12sp_np21_32702(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_f04
ON cipsrv_wbd.wbd_hu12sp_np21_32702(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_f05
ON cipsrv_wbd.wbd_hu12sp_np21_32702(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_np21_32702_spx
ON cipsrv_wbd.wbd_hu12sp_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_np21_32702;

--******************************--
----- materialized views/wbd_hu10sp_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_np21_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_np21 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu10sp_np21_5070(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu10sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu10sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_f01
ON cipsrv_wbd.wbd_hu10sp_np21_5070(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_f02
ON cipsrv_wbd.wbd_hu10sp_np21_5070(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_f03
ON cipsrv_wbd.wbd_hu10sp_np21_5070(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_f04
ON cipsrv_wbd.wbd_hu10sp_np21_5070(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu10sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21_5070;

--******************************--
----- materialized views/wbd_hu10sp_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_np21_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_np21 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_pk
ON cipsrv_wbd.wbd_hu10sp_np21_3338(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_u01
ON cipsrv_wbd.wbd_hu10sp_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_u02
ON cipsrv_wbd.wbd_hu10sp_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_f01
ON cipsrv_wbd.wbd_hu10sp_np21_3338(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_f02
ON cipsrv_wbd.wbd_hu10sp_np21_3338(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_f03
ON cipsrv_wbd.wbd_hu10sp_np21_3338(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_f04
ON cipsrv_wbd.wbd_hu10sp_np21_3338(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_3338_spx
ON cipsrv_wbd.wbd_hu10sp_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21_3338;

--******************************--
----- materialized views/wbd_hu10sp_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_np21_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_np21 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_pk
ON cipsrv_wbd.wbd_hu10sp_np21_26904(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_u01
ON cipsrv_wbd.wbd_hu10sp_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_u02
ON cipsrv_wbd.wbd_hu10sp_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_f01
ON cipsrv_wbd.wbd_hu10sp_np21_26904(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_f02
ON cipsrv_wbd.wbd_hu10sp_np21_26904(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_f03
ON cipsrv_wbd.wbd_hu10sp_np21_26904(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_f04
ON cipsrv_wbd.wbd_hu10sp_np21_26904(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_26904_spx
ON cipsrv_wbd.wbd_hu10sp_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21_26904;

--******************************--
----- materialized views/wbd_hu10sp_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_np21_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_np21 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_pk
ON cipsrv_wbd.wbd_hu10sp_np21_32161(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_u01
ON cipsrv_wbd.wbd_hu10sp_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_u02
ON cipsrv_wbd.wbd_hu10sp_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_f01
ON cipsrv_wbd.wbd_hu10sp_np21_32161(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_f02
ON cipsrv_wbd.wbd_hu10sp_np21_32161(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_f03
ON cipsrv_wbd.wbd_hu10sp_np21_32161(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_f04
ON cipsrv_wbd.wbd_hu10sp_np21_32161(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32161_spx
ON cipsrv_wbd.wbd_hu10sp_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21_32161;

--******************************--
----- materialized views/wbd_hu10sp_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_np21_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_np21 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_pk
ON cipsrv_wbd.wbd_hu10sp_np21_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_u01
ON cipsrv_wbd.wbd_hu10sp_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_u02
ON cipsrv_wbd.wbd_hu10sp_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_f01
ON cipsrv_wbd.wbd_hu10sp_np21_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_f02
ON cipsrv_wbd.wbd_hu10sp_np21_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_f03
ON cipsrv_wbd.wbd_hu10sp_np21_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_f04
ON cipsrv_wbd.wbd_hu10sp_np21_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32655_spx
ON cipsrv_wbd.wbd_hu10sp_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21_32655;

--******************************--
----- materialized views/wbd_hu10sp_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_np21_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_np21 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_pk
ON cipsrv_wbd.wbd_hu10sp_np21_32702(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_u01
ON cipsrv_wbd.wbd_hu10sp_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_u02
ON cipsrv_wbd.wbd_hu10sp_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_f01
ON cipsrv_wbd.wbd_hu10sp_np21_32702(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_f02
ON cipsrv_wbd.wbd_hu10sp_np21_32702(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_f03
ON cipsrv_wbd.wbd_hu10sp_np21_32702(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_f04
ON cipsrv_wbd.wbd_hu10sp_np21_32702(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_32702_spx
ON cipsrv_wbd.wbd_hu10sp_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21_32702;

--******************************--
----- materialized views/wbd_hu8sp_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_np21_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_np21 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu8sp_np21_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu8sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu8sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_f01
ON cipsrv_wbd.wbd_hu8sp_np21_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_f02
ON cipsrv_wbd.wbd_hu8sp_np21_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_f03
ON cipsrv_wbd.wbd_hu8sp_np21_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu8sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_np21_5070;

--******************************--
----- materialized views/wbd_hu8sp_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_np21_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_np21 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_pk
ON cipsrv_wbd.wbd_hu8sp_np21_3338(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_u01
ON cipsrv_wbd.wbd_hu8sp_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_u02
ON cipsrv_wbd.wbd_hu8sp_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_f01
ON cipsrv_wbd.wbd_hu8sp_np21_3338(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_f02
ON cipsrv_wbd.wbd_hu8sp_np21_3338(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_f03
ON cipsrv_wbd.wbd_hu8sp_np21_3338(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_3338_spx
ON cipsrv_wbd.wbd_hu8sp_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_np21_3338;

--******************************--
----- materialized views/wbd_hu8sp_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_np21_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_np21 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_pk
ON cipsrv_wbd.wbd_hu8sp_np21_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_u01
ON cipsrv_wbd.wbd_hu8sp_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_u02
ON cipsrv_wbd.wbd_hu8sp_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_f01
ON cipsrv_wbd.wbd_hu8sp_np21_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_f02
ON cipsrv_wbd.wbd_hu8sp_np21_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_f03
ON cipsrv_wbd.wbd_hu8sp_np21_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_26904_spx
ON cipsrv_wbd.wbd_hu8sp_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_np21_26904;

--******************************--
----- materialized views/wbd_hu8sp_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_np21_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_np21 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_pk
ON cipsrv_wbd.wbd_hu8sp_np21_32161(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_u01
ON cipsrv_wbd.wbd_hu8sp_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_u02
ON cipsrv_wbd.wbd_hu8sp_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_f01
ON cipsrv_wbd.wbd_hu8sp_np21_32161(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_f02
ON cipsrv_wbd.wbd_hu8sp_np21_32161(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_f03
ON cipsrv_wbd.wbd_hu8sp_np21_32161(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32161_spx
ON cipsrv_wbd.wbd_hu8sp_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_np21_32161;

--******************************--
----- materialized views/wbd_hu8sp_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_np21_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_np21 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_pk
ON cipsrv_wbd.wbd_hu8sp_np21_32655(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_u01
ON cipsrv_wbd.wbd_hu8sp_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_u02
ON cipsrv_wbd.wbd_hu8sp_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_f01
ON cipsrv_wbd.wbd_hu8sp_np21_32655(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_f02
ON cipsrv_wbd.wbd_hu8sp_np21_32655(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_f03
ON cipsrv_wbd.wbd_hu8sp_np21_32655(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32655_spx
ON cipsrv_wbd.wbd_hu8sp_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_np21_32655;

--******************************--
----- materialized views/wbd_hu8sp_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_np21_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_np21 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_pk
ON cipsrv_wbd.wbd_hu8sp_np21_32702(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_u01
ON cipsrv_wbd.wbd_hu8sp_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_u02
ON cipsrv_wbd.wbd_hu8sp_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_f01
ON cipsrv_wbd.wbd_hu8sp_np21_32702(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_f02
ON cipsrv_wbd.wbd_hu8sp_np21_32702(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_f03
ON cipsrv_wbd.wbd_hu8sp_np21_32702(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_32702_spx
ON cipsrv_wbd.wbd_hu8sp_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_np21_32702;

--******************************--
----- materialized views/wbd_hu6sp_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_np21_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_np21 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu6sp_np21_5070(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu6sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu6sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_5070_f01
ON cipsrv_wbd.wbd_hu6sp_np21_5070(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_5070_f02
ON cipsrv_wbd.wbd_hu6sp_np21_5070(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu6sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21_5070;

--******************************--
----- materialized views/wbd_hu6sp_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_np21_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_np21 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_3338_pk
ON cipsrv_wbd.wbd_hu6sp_np21_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_3338_u01
ON cipsrv_wbd.wbd_hu6sp_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_3338_u02
ON cipsrv_wbd.wbd_hu6sp_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_3338_f01
ON cipsrv_wbd.wbd_hu6sp_np21_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_3338_f02
ON cipsrv_wbd.wbd_hu6sp_np21_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_3338_spx
ON cipsrv_wbd.wbd_hu6sp_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21_3338;

--******************************--
----- materialized views/wbd_hu6sp_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_np21_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_np21 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_26904_pk
ON cipsrv_wbd.wbd_hu6sp_np21_26904(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_26904_u01
ON cipsrv_wbd.wbd_hu6sp_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_26904_u02
ON cipsrv_wbd.wbd_hu6sp_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_26904_f01
ON cipsrv_wbd.wbd_hu6sp_np21_26904(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_26904_f02
ON cipsrv_wbd.wbd_hu6sp_np21_26904(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_26904_spx
ON cipsrv_wbd.wbd_hu6sp_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21_26904;

--******************************--
----- materialized views/wbd_hu6sp_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_np21_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_np21 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32161_pk
ON cipsrv_wbd.wbd_hu6sp_np21_32161(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32161_u01
ON cipsrv_wbd.wbd_hu6sp_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32161_u02
ON cipsrv_wbd.wbd_hu6sp_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32161_f01
ON cipsrv_wbd.wbd_hu6sp_np21_32161(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32161_f02
ON cipsrv_wbd.wbd_hu6sp_np21_32161(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32161_spx
ON cipsrv_wbd.wbd_hu6sp_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21_32161;

--******************************--
----- materialized views/wbd_hu6sp_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_np21_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_np21 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32655_pk
ON cipsrv_wbd.wbd_hu6sp_np21_32655(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32655_u01
ON cipsrv_wbd.wbd_hu6sp_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32655_u02
ON cipsrv_wbd.wbd_hu6sp_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32655_f01
ON cipsrv_wbd.wbd_hu6sp_np21_32655(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32655_f02
ON cipsrv_wbd.wbd_hu6sp_np21_32655(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32655_spx
ON cipsrv_wbd.wbd_hu6sp_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21_32655;

--******************************--
----- materialized views/wbd_hu6sp_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_np21_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_np21 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32702_pk
ON cipsrv_wbd.wbd_hu6sp_np21_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32702_u01
ON cipsrv_wbd.wbd_hu6sp_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_32702_u02
ON cipsrv_wbd.wbd_hu6sp_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32702_f01
ON cipsrv_wbd.wbd_hu6sp_np21_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32702_f02
ON cipsrv_wbd.wbd_hu6sp_np21_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_32702_spx
ON cipsrv_wbd.wbd_hu6sp_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21_32702;

--******************************--
----- materialized views/wbd_hu4sp_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21_5070(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_np21_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_np21 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu4sp_np21_5070(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu4sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu4sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_5070_f01
ON cipsrv_wbd.wbd_hu4sp_np21_5070(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu4sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21_5070;

--******************************--
----- materialized views/wbd_hu4sp_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21_3338(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_np21_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_np21 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_3338_pk
ON cipsrv_wbd.wbd_hu4sp_np21_3338(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_3338_u01
ON cipsrv_wbd.wbd_hu4sp_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_3338_u02
ON cipsrv_wbd.wbd_hu4sp_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_3338_f01
ON cipsrv_wbd.wbd_hu4sp_np21_3338(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_3338_spx
ON cipsrv_wbd.wbd_hu4sp_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21_3338;

--******************************--
----- materialized views/wbd_hu4sp_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21_26904(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_np21_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_np21 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_26904_pk
ON cipsrv_wbd.wbd_hu4sp_np21_26904(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_26904_u01
ON cipsrv_wbd.wbd_hu4sp_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_26904_u02
ON cipsrv_wbd.wbd_hu4sp_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_26904_f01
ON cipsrv_wbd.wbd_hu4sp_np21_26904(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_26904_spx
ON cipsrv_wbd.wbd_hu4sp_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21_26904;

--******************************--
----- materialized views/wbd_hu4sp_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21_32161(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_np21_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_np21 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32161_pk
ON cipsrv_wbd.wbd_hu4sp_np21_32161(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32161_u01
ON cipsrv_wbd.wbd_hu4sp_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32161_u02
ON cipsrv_wbd.wbd_hu4sp_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_32161_f01
ON cipsrv_wbd.wbd_hu4sp_np21_32161(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_32161_spx
ON cipsrv_wbd.wbd_hu4sp_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21_32161;

--******************************--
----- materialized views/wbd_hu4sp_np21_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21_32655(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_np21_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_np21 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32655_pk
ON cipsrv_wbd.wbd_hu4sp_np21_32655(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32655_u01
ON cipsrv_wbd.wbd_hu4sp_np21_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32655_u02
ON cipsrv_wbd.wbd_hu4sp_np21_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_32655_f01
ON cipsrv_wbd.wbd_hu4sp_np21_32655(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_32655_spx
ON cipsrv_wbd.wbd_hu4sp_np21_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21_32655;

--******************************--
----- materialized views/wbd_hu4sp_np21_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21_32702(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_np21_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_np21 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32702_pk
ON cipsrv_wbd.wbd_hu4sp_np21_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32702_u01
ON cipsrv_wbd.wbd_hu4sp_np21_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_32702_u02
ON cipsrv_wbd.wbd_hu4sp_np21_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_32702_f01
ON cipsrv_wbd.wbd_hu4sp_np21_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_32702_spx
ON cipsrv_wbd.wbd_hu4sp_np21_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21_32702;

--******************************--
----- materialized views/wbd_hu2sp_np21_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_np21_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_np21_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_np21_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_np21 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_np21_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_np21_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_pk
ON cipsrv_wbd.wbd_hu2sp_np21_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_u01
ON cipsrv_wbd.wbd_hu2sp_np21_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_u02
ON cipsrv_wbd.wbd_hu2sp_np21_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_np21_5070_spx
ON cipsrv_wbd.wbd_hu2sp_np21_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_np21_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_np21_5070;

--******************************--
----- materialized views/wbd_hu2sp_np21_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_np21_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_np21_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_np21_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_np21 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_np21_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_np21_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_3338_pk
ON cipsrv_wbd.wbd_hu2sp_np21_3338(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_3338_u01
ON cipsrv_wbd.wbd_hu2sp_np21_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_3338_u02
ON cipsrv_wbd.wbd_hu2sp_np21_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_np21_3338_spx
ON cipsrv_wbd.wbd_hu2sp_np21_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_np21_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_np21_3338;

--******************************--
----- materialized views/wbd_hu2sp_np21_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_np21_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_np21_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_np21_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_np21 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_np21_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_np21_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_26904_pk
ON cipsrv_wbd.wbd_hu2sp_np21_26904(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_26904_u01
ON cipsrv_wbd.wbd_hu2sp_np21_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_26904_u02
ON cipsrv_wbd.wbd_hu2sp_np21_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_np21_26904_spx
ON cipsrv_wbd.wbd_hu2sp_np21_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_np21_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_np21_26904;

--******************************--
----- materialized views/wbd_hu2sp_np21_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_np21_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_np21_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_np21_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_np21 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_np21_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_np21_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_32161_pk
ON cipsrv_wbd.wbd_hu2sp_np21_32161(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_32161_u01
ON cipsrv_wbd.wbd_hu2sp_np21_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_32161_u02
ON cipsrv_wbd.wbd_hu2sp_np21_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_np21_32161_spx
ON cipsrv_wbd.wbd_hu2sp_np21_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_np21_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_np21_32161;

--******************************--
----- materialized views/wbd_hu10sp_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc10
,a.hutype
,a.humod
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc10
   ,aa.hutype
   ,aa.humod
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc10
   ,bb.hutype
   ,bb.humod
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc10
   ,cc.hutype
   ,cc.humod
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc10
   ,dd.hutype
   ,dd.humod
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc10
   ,ee.hutype
   ,ee.humod
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc10
   ,ff.hutype
   ,ff.humod
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_pk
ON cipsrv_wbd.wbd_hu10sp_np21(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_u01
ON cipsrv_wbd.wbd_hu10sp_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_np21_u02
ON cipsrv_wbd.wbd_hu10sp_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_f01
ON cipsrv_wbd.wbd_hu10sp_np21(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_f02
ON cipsrv_wbd.wbd_hu10sp_np21(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_f03
ON cipsrv_wbd.wbd_hu10sp_np21(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_f04
ON cipsrv_wbd.wbd_hu10sp_np21(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_np21_spx
ON cipsrv_wbd.wbd_hu10sp_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_np21;

--******************************--
----- materialized views/wbd_hu8sp_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_pk
ON cipsrv_wbd.wbd_hu8sp_np21(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_u01
ON cipsrv_wbd.wbd_hu8sp_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_np21_u02
ON cipsrv_wbd.wbd_hu8sp_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_f01
ON cipsrv_wbd.wbd_hu8sp_np21(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_f02
ON cipsrv_wbd.wbd_hu8sp_np21(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_f03
ON cipsrv_wbd.wbd_hu8sp_np21(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_np21_spx
ON cipsrv_wbd.wbd_hu8sp_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_np21;

--******************************--
----- materialized views/wbd_hu6sp_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_pk
ON cipsrv_wbd.wbd_hu6sp_np21(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_u01
ON cipsrv_wbd.wbd_hu6sp_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_np21_u02
ON cipsrv_wbd.wbd_hu6sp_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_f01
ON cipsrv_wbd.wbd_hu6sp_np21(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_f02
ON cipsrv_wbd.wbd_hu6sp_np21(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_np21_spx
ON cipsrv_wbd.wbd_hu6sp_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_np21;

--******************************--
----- materialized views/wbd_hu4sp_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_np21(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_np21_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_np21_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_np21_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_pk
ON cipsrv_wbd.wbd_hu4sp_np21(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_u01
ON cipsrv_wbd.wbd_hu4sp_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_np21_u02
ON cipsrv_wbd.wbd_hu4sp_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_f01
ON cipsrv_wbd.wbd_hu4sp_np21(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_np21_spx
ON cipsrv_wbd.wbd_hu4sp_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_np21;


--******************************--
----- materialized views/wbd_hu2sp_np21.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_np21 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_np21(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_np21_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_np21_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_np21_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_np21_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) 
        FROM (
           SELECT
           ST_TRANSFORM(a2.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4sp_np21_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4sp_np21_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12sp_np21 ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_np21 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_np21 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_pk
ON cipsrv_wbd.wbd_hu2sp_np21(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_u01
ON cipsrv_wbd.wbd_hu2sp_np21(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_np21_u02
ON cipsrv_wbd.wbd_hu2sp_np21(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_np21_spx
ON cipsrv_wbd.wbd_hu2sp_np21 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_np21;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_np21;


--******************************--
----- materialized views/wbd_hu12sp_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
JOIN
cipsrv_wbd.wbd_hu12_nphr b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_5070_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_5070;

--******************************--
----- materialized views/wbd_hu12sp_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
JOIN
cipsrv_wbd.wbd_hu12_nphr b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_3338_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_3338;

--******************************--
----- materialized views/wbd_hu12sp_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
JOIN
cipsrv_wbd.wbd_hu12_nphr b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_26904;

--******************************--
----- materialized views/wbd_hu12sp_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
JOIN
cipsrv_wbd.wbd_hu12_nphr b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32161;

--******************************--
----- materialized views/wbd_hu12sp_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32655)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
JOIN
cipsrv_wbd.wbd_hu12_nphr b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32655_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32655;

--******************************--
----- materialized views/wbd_hu12sp_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32702)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_nphr a
JOIN
cipsrv_wbd.wbd_hu12_nphr b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_pk
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_u01
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_u02
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_f01
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_f02
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_f03
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_f04
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_f05
ON cipsrv_wbd.wbd_hu12sp_nphr_32702(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_nphr_32702_spx
ON cipsrv_wbd.wbd_hu12sp_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_nphr_32702;

--******************************--
----- materialized views/wbd_hu10sp_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_nphr_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_nphr c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_pk
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_u01
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_u02
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_f01
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_f02
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_f03
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_f04
ON cipsrv_wbd.wbd_hu10sp_nphr_5070(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_5070_spx
ON cipsrv_wbd.wbd_hu10sp_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_5070;

--******************************--
----- materialized views/wbd_hu10sp_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_nphr_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_nphr c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_pk
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_u01
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_u02
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_f01
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_f02
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_f03
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_f04
ON cipsrv_wbd.wbd_hu10sp_nphr_3338(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_3338_spx
ON cipsrv_wbd.wbd_hu10sp_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_3338;

--******************************--
----- materialized views/wbd_hu10sp_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_nphr_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_nphr c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_f01
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_f02
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_f03
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_f04
ON cipsrv_wbd.wbd_hu10sp_nphr_26904(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu10sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_26904;

--******************************--
----- materialized views/wbd_hu10sp_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_nphr_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_nphr c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_f01
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_f02
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_f03
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_f04
ON cipsrv_wbd.wbd_hu10sp_nphr_32161(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu10sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_32161;

--******************************--
----- materialized views/wbd_hu10sp_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_nphr_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_nphr c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_pk
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_u01
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_u02
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_f01
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_f02
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_f03
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_f04
ON cipsrv_wbd.wbd_hu10sp_nphr_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32655_spx
ON cipsrv_wbd.wbd_hu10sp_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_32655;

--******************************--
----- materialized views/wbd_hu10sp_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_nphr_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_nphr c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_pk
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_u01
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_u02
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_f01
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_f02
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_f03
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_f04
ON cipsrv_wbd.wbd_hu10sp_nphr_32702(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_32702_spx
ON cipsrv_wbd.wbd_hu10sp_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr_32702;

--******************************--
----- materialized views/wbd_hu8sp_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_nphr_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_5070_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_5070;

--******************************--
----- materialized views/wbd_hu8sp_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_nphr_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_3338(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_3338(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_3338(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_3338(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_3338_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_3338;

--******************************--
----- materialized views/wbd_hu8sp_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_nphr_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_26904;

--******************************--
----- materialized views/wbd_hu8sp_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_nphr_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_32161(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_32161(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_32161(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_32161(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_32161;

--******************************--
----- materialized views/wbd_hu8sp_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_nphr_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_32655(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_32655(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_32655(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_32655(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32655_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_32655;

--******************************--
----- materialized views/wbd_hu8sp_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_nphr_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_nphr c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_pk
ON cipsrv_wbd.wbd_hu8sp_nphr_32702(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_u01
ON cipsrv_wbd.wbd_hu8sp_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_u02
ON cipsrv_wbd.wbd_hu8sp_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_f01
ON cipsrv_wbd.wbd_hu8sp_nphr_32702(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_f02
ON cipsrv_wbd.wbd_hu8sp_nphr_32702(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_f03
ON cipsrv_wbd.wbd_hu8sp_nphr_32702(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_32702_spx
ON cipsrv_wbd.wbd_hu8sp_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_nphr_32702;

--******************************--
----- materialized views/wbd_hu6sp_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_nphr_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_nphr c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_5070_pk
ON cipsrv_wbd.wbd_hu6sp_nphr_5070(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_5070_u01
ON cipsrv_wbd.wbd_hu6sp_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_5070_u02
ON cipsrv_wbd.wbd_hu6sp_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_5070_f01
ON cipsrv_wbd.wbd_hu6sp_nphr_5070(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_5070_f02
ON cipsrv_wbd.wbd_hu6sp_nphr_5070(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_5070_spx
ON cipsrv_wbd.wbd_hu6sp_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_5070;

--******************************--
----- materialized views/wbd_hu6sp_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_nphr_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_nphr c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_3338_pk
ON cipsrv_wbd.wbd_hu6sp_nphr_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_3338_u01
ON cipsrv_wbd.wbd_hu6sp_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_3338_u02
ON cipsrv_wbd.wbd_hu6sp_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_3338_f01
ON cipsrv_wbd.wbd_hu6sp_nphr_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_3338_f02
ON cipsrv_wbd.wbd_hu6sp_nphr_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_3338_spx
ON cipsrv_wbd.wbd_hu6sp_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_3338;

--******************************--
----- materialized views/wbd_hu6sp_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_nphr_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_nphr c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu6sp_nphr_26904(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu6sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu6sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_26904_f01
ON cipsrv_wbd.wbd_hu6sp_nphr_26904(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_26904_f02
ON cipsrv_wbd.wbd_hu6sp_nphr_26904(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu6sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_26904;

--******************************--
----- materialized views/wbd_hu6sp_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_nphr_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_nphr c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu6sp_nphr_32161(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu6sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu6sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32161_f01
ON cipsrv_wbd.wbd_hu6sp_nphr_32161(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32161_f02
ON cipsrv_wbd.wbd_hu6sp_nphr_32161(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu6sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_32161;

--******************************--
----- materialized views/wbd_hu6sp_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_nphr_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_nphr c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32655_pk
ON cipsrv_wbd.wbd_hu6sp_nphr_32655(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32655_u01
ON cipsrv_wbd.wbd_hu6sp_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32655_u02
ON cipsrv_wbd.wbd_hu6sp_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32655_f01
ON cipsrv_wbd.wbd_hu6sp_nphr_32655(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32655_f02
ON cipsrv_wbd.wbd_hu6sp_nphr_32655(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32655_spx
ON cipsrv_wbd.wbd_hu6sp_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_32655;

--******************************--
----- materialized views/wbd_hu6sp_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_nphr_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_nphr c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32702_pk
ON cipsrv_wbd.wbd_hu6sp_nphr_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32702_u01
ON cipsrv_wbd.wbd_hu6sp_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32702_u02
ON cipsrv_wbd.wbd_hu6sp_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32702_f01
ON cipsrv_wbd.wbd_hu6sp_nphr_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32702_f02
ON cipsrv_wbd.wbd_hu6sp_nphr_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_32702_spx
ON cipsrv_wbd.wbd_hu6sp_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr_32702;

--******************************--
----- materialized views/wbd_hu4sp_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr_5070(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_nphr_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_nphr c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_5070_pk
ON cipsrv_wbd.wbd_hu4sp_nphr_5070(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_5070_u01
ON cipsrv_wbd.wbd_hu4sp_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_5070_u02
ON cipsrv_wbd.wbd_hu4sp_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_5070_f01
ON cipsrv_wbd.wbd_hu4sp_nphr_5070(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_5070_spx
ON cipsrv_wbd.wbd_hu4sp_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_5070;

--******************************--
----- materialized views/wbd_hu4sp_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr_3338(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_nphr_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_nphr c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_3338_pk
ON cipsrv_wbd.wbd_hu4sp_nphr_3338(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_3338_u01
ON cipsrv_wbd.wbd_hu4sp_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_3338_u02
ON cipsrv_wbd.wbd_hu4sp_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_3338_f01
ON cipsrv_wbd.wbd_hu4sp_nphr_3338(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_3338_spx
ON cipsrv_wbd.wbd_hu4sp_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_3338;

--******************************--
----- materialized views/wbd_hu4sp_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr_26904(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_nphr_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_nphr c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu4sp_nphr_26904(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu4sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu4sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_26904_f01
ON cipsrv_wbd.wbd_hu4sp_nphr_26904(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu4sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_26904;

--******************************--
----- materialized views/wbd_hu4sp_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr_32161(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_nphr_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_nphr c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu4sp_nphr_32161(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu4sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu4sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32161_f01
ON cipsrv_wbd.wbd_hu4sp_nphr_32161(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu4sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_32161;

--******************************--
----- materialized views/wbd_hu4sp_nphr_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr_32655(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_nphr_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_nphr c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32655_pk
ON cipsrv_wbd.wbd_hu4sp_nphr_32655(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32655_u01
ON cipsrv_wbd.wbd_hu4sp_nphr_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32655_u02
ON cipsrv_wbd.wbd_hu4sp_nphr_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32655_f01
ON cipsrv_wbd.wbd_hu4sp_nphr_32655(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32655_spx
ON cipsrv_wbd.wbd_hu4sp_nphr_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_32655;

--******************************--
----- materialized views/wbd_hu4sp_nphr_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr_32702(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_nphr_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_nphr c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32702_pk
ON cipsrv_wbd.wbd_hu4sp_nphr_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32702_u01
ON cipsrv_wbd.wbd_hu4sp_nphr_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32702_u02
ON cipsrv_wbd.wbd_hu4sp_nphr_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32702_f01
ON cipsrv_wbd.wbd_hu4sp_nphr_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_32702_spx
ON cipsrv_wbd.wbd_hu4sp_nphr_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr_32702;

--******************************--
----- materialized views/wbd_hu2sp_nphr_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_nphr_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_nphr_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_nphr_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_nphr c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_nphr_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_nphr_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_5070_pk
ON cipsrv_wbd.wbd_hu2sp_nphr_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_5070_u01
ON cipsrv_wbd.wbd_hu2sp_nphr_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_5070_u02
ON cipsrv_wbd.wbd_hu2sp_nphr_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_nphr_5070_spx
ON cipsrv_wbd.wbd_hu2sp_nphr_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_5070;

--******************************--
----- materialized views/wbd_hu2sp_nphr_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_nphr_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_nphr_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_nphr_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_nphr c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_nphr_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_nphr_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_3338_pk
ON cipsrv_wbd.wbd_hu2sp_nphr_3338(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_3338_u01
ON cipsrv_wbd.wbd_hu2sp_nphr_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_3338_u02
ON cipsrv_wbd.wbd_hu2sp_nphr_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_nphr_3338_spx
ON cipsrv_wbd.wbd_hu2sp_nphr_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_3338;

--******************************--
----- materialized views/wbd_hu2sp_nphr_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_nphr_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_nphr_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_nphr_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_nphr c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_nphr_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_nphr_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_26904_pk
ON cipsrv_wbd.wbd_hu2sp_nphr_26904(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_26904_u01
ON cipsrv_wbd.wbd_hu2sp_nphr_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_26904_u02
ON cipsrv_wbd.wbd_hu2sp_nphr_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_nphr_26904_spx
ON cipsrv_wbd.wbd_hu2sp_nphr_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_26904;

--******************************--
----- materialized views/wbd_hu2sp_nphr_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_nphr_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_nphr_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_nphr_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_nphr c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_nphr_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_nphr_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_32161_pk
ON cipsrv_wbd.wbd_hu2sp_nphr_32161(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_32161_u01
ON cipsrv_wbd.wbd_hu2sp_nphr_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_32161_u02
ON cipsrv_wbd.wbd_hu2sp_nphr_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_nphr_32161_spx
ON cipsrv_wbd.wbd_hu2sp_nphr_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_nphr_32161;

--******************************--
----- materialized views/wbd_hu10sp_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc10
,a.hutype
,a.humod
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc10
   ,aa.hutype
   ,aa.humod
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc10
   ,bb.hutype
   ,bb.humod
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc10
   ,cc.hutype
   ,cc.humod
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc10
   ,dd.hutype
   ,dd.humod
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc10
   ,ee.hutype
   ,ee.humod
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc10
   ,ff.hutype
   ,ff.humod
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_pk
ON cipsrv_wbd.wbd_hu10sp_nphr(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_u01
ON cipsrv_wbd.wbd_hu10sp_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_nphr_u02
ON cipsrv_wbd.wbd_hu10sp_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_f01
ON cipsrv_wbd.wbd_hu10sp_nphr(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_f02
ON cipsrv_wbd.wbd_hu10sp_nphr(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_f03
ON cipsrv_wbd.wbd_hu10sp_nphr(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_f04
ON cipsrv_wbd.wbd_hu10sp_nphr(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_nphr_spx
ON cipsrv_wbd.wbd_hu10sp_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_nphr;

--******************************--
----- materialized views/wbd_hu8sp_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_pk
ON cipsrv_wbd.wbd_hu8sp_nphr(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_u01
ON cipsrv_wbd.wbd_hu8sp_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_nphr_u02
ON cipsrv_wbd.wbd_hu8sp_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_f01
ON cipsrv_wbd.wbd_hu8sp_nphr(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_f02
ON cipsrv_wbd.wbd_hu8sp_nphr(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_f03
ON cipsrv_wbd.wbd_hu8sp_nphr(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_nphr_spx
ON cipsrv_wbd.wbd_hu8sp_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_nphr;

--******************************--
----- materialized views/wbd_hu6sp_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_pk
ON cipsrv_wbd.wbd_hu6sp_nphr(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_u01
ON cipsrv_wbd.wbd_hu6sp_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_nphr_u02
ON cipsrv_wbd.wbd_hu6sp_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_f01
ON cipsrv_wbd.wbd_hu6sp_nphr(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_f02
ON cipsrv_wbd.wbd_hu6sp_nphr(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_nphr_spx
ON cipsrv_wbd.wbd_hu6sp_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_nphr;

--******************************--
----- materialized views/wbd_hu4sp_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_nphr(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_nphr_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_nphr_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_nphr_32702 ff
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_pk
ON cipsrv_wbd.wbd_hu4sp_nphr(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_u01
ON cipsrv_wbd.wbd_hu4sp_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_nphr_u02
ON cipsrv_wbd.wbd_hu4sp_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_f01
ON cipsrv_wbd.wbd_hu4sp_nphr(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_nphr_spx
ON cipsrv_wbd.wbd_hu4sp_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_nphr;


--******************************--
----- materialized views/wbd_hu2sp_nphr.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_nphr CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_nphr(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_nphr_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_nphr_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_nphr_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_nphr_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) 
        FROM (
           SELECT
           ST_TRANSFORM(a2.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4sp_nphr_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269) AS shape
           FROM
           cipsrv_wbd.wbd_hu4sp_nphr_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12sp_nphr ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_nphr OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_nphr TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_pk
ON cipsrv_wbd.wbd_hu2sp_nphr(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_u01
ON cipsrv_wbd.wbd_hu2sp_nphr(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_nphr_u02
ON cipsrv_wbd.wbd_hu2sp_nphr(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_nphr_spx
ON cipsrv_wbd.wbd_hu2sp_nphr USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_nphr;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_nphr;


--******************************--
----- materialized views/wbd_hu12sp_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,5070)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu12sp_f3_5070(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu12sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu12sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f01
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f02
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f03
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f04
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_f05
ON cipsrv_wbd.wbd_hu12sp_f3_5070(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu12sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_5070;

--******************************--
----- materialized views/wbd_hu12sp_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,3338)       AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('19');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu12sp_f3_3338(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu12sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu12sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_f01
ON cipsrv_wbd.wbd_hu12sp_f3_3338(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_f02
ON cipsrv_wbd.wbd_hu12sp_f3_3338(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_f03
ON cipsrv_wbd.wbd_hu12sp_f3_3338(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_f04
ON cipsrv_wbd.wbd_hu12sp_f3_3338(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_f05
ON cipsrv_wbd.wbd_hu12sp_f3_3338(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu12sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_3338;

--******************************--
----- materialized views/wbd_hu12sp_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,26904)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('20');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_pk
ON cipsrv_wbd.wbd_hu12sp_f3_26904(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_u01
ON cipsrv_wbd.wbd_hu12sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_u02
ON cipsrv_wbd.wbd_hu12sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f01
ON cipsrv_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f02
ON cipsrv_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f03
ON cipsrv_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f04
ON cipsrv_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_f05
ON cipsrv_wbd.wbd_hu12sp_f3_26904(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_26904_spx
ON cipsrv_wbd.wbd_hu12sp_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_26904;

--******************************--
----- materialized views/wbd_hu12sp_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32161)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,2) IN ('21');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_pk
ON cipsrv_wbd.wbd_hu12sp_f3_32161(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_u01
ON cipsrv_wbd.wbd_hu12sp_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_u02
ON cipsrv_wbd.wbd_hu12sp_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_f01
ON cipsrv_wbd.wbd_hu12sp_f3_32161(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_f02
ON cipsrv_wbd.wbd_hu12sp_f3_32161(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_f03
ON cipsrv_wbd.wbd_hu12sp_f3_32161(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_f04
ON cipsrv_wbd.wbd_hu12sp_f3_32161(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_f05
ON cipsrv_wbd.wbd_hu12sp_f3_32161(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32161_spx
ON cipsrv_wbd.wbd_hu12sp_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_32161;

--******************************--
----- materialized views/wbd_hu12sp_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32655)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_pk
ON cipsrv_wbd.wbd_hu12sp_f3_32655(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_u01
ON cipsrv_wbd.wbd_hu12sp_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_u02
ON cipsrv_wbd.wbd_hu12sp_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_f01
ON cipsrv_wbd.wbd_hu12sp_f3_32655(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_f02
ON cipsrv_wbd.wbd_hu12sp_f3_32655(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_f03
ON cipsrv_wbd.wbd_hu12sp_f3_32655(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_f04
ON cipsrv_wbd.wbd_hu12sp_f3_32655(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_f05
ON cipsrv_wbd.wbd_hu12sp_f3_32655(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32655_spx
ON cipsrv_wbd.wbd_hu12sp_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_32655;

--******************************--
----- materialized views/wbd_hu12sp_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu12sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu12sp_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc12
   ,hutype
   ,humod
   ,tohuc
   ,noncontributingareaacres
   ,noncontributingareasqkm
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 a.objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,b.areasqkm
,b.areaacres
,a.name
,b.states
,a.huc12
,a.hutype
,a.humod
,a.tohuc
,a.noncontributingareaacres
,a.noncontributingareasqkm
,b.centermass_x
,b.centermass_y
,'{' || uuid_generate_v1() || '}' AS globalid
,ST_TRANSFORM(a.shape,32702)      AS shape
FROM
cipsrv_wbd.wbd_hu12sp_f3 a
JOIN
cipsrv_wbd.wbd_hu12_f3 b
ON
a.huc12 = b.huc12
WHERE
SUBSTR(a.huc12,1,4) IN ('2203');

ALTER TABLE cipsrv_wbd.wbd_hu12sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu12sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_pk
ON cipsrv_wbd.wbd_hu12sp_f3_32702(huc12);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_u01
ON cipsrv_wbd.wbd_hu12sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_u02
ON cipsrv_wbd.wbd_hu12sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_f01
ON cipsrv_wbd.wbd_hu12sp_f3_32702(SUBSTR(huc12,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_f02
ON cipsrv_wbd.wbd_hu12sp_f3_32702(SUBSTR(huc12,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_f03
ON cipsrv_wbd.wbd_hu12sp_f3_32702(SUBSTR(huc12,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_f04
ON cipsrv_wbd.wbd_hu12sp_f3_32702(SUBSTR(huc12,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_f05
ON cipsrv_wbd.wbd_hu12sp_f3_32702(SUBSTR(huc12,1,10));

CREATE INDEX IF NOT EXISTS wbd_hu12sp_f3_32702_spx
ON cipsrv_wbd.wbd_hu12sp_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu12sp_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu12sp_f3_32702;

--******************************--
----- materialized views/wbd_hu10sp_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_f3_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu10sp_f3_5070(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu10sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu10sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_f01
ON cipsrv_wbd.wbd_hu10sp_f3_5070(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_f02
ON cipsrv_wbd.wbd_hu10sp_f3_5070(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_f03
ON cipsrv_wbd.wbd_hu10sp_f3_5070(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_f04
ON cipsrv_wbd.wbd_hu10sp_f3_5070(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu10sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3_5070;

--******************************--
----- materialized views/wbd_hu10sp_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM
      cipsrv_wbd.wbd_hu12sp_f3_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu10sp_f3_3338(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu10sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu10sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_f01
ON cipsrv_wbd.wbd_hu10sp_f3_3338(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_f02
ON cipsrv_wbd.wbd_hu10sp_f3_3338(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_f03
ON cipsrv_wbd.wbd_hu10sp_f3_3338(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_f04
ON cipsrv_wbd.wbd_hu10sp_f3_3338(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu10sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3_3338;

--******************************--
----- materialized views/wbd_hu10sp_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_f3_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_pk
ON cipsrv_wbd.wbd_hu10sp_f3_26904(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_u01
ON cipsrv_wbd.wbd_hu10sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_u02
ON cipsrv_wbd.wbd_hu10sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_f01
ON cipsrv_wbd.wbd_hu10sp_f3_26904(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_f02
ON cipsrv_wbd.wbd_hu10sp_f3_26904(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_f03
ON cipsrv_wbd.wbd_hu10sp_f3_26904(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_f04
ON cipsrv_wbd.wbd_hu10sp_f3_26904(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_26904_spx
ON cipsrv_wbd.wbd_hu10sp_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3_26904;

--******************************--
----- materialized views/wbd_hu10sp_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_f3_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_pk
ON cipsrv_wbd.wbd_hu10sp_f3_32161(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_u01
ON cipsrv_wbd.wbd_hu10sp_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_u02
ON cipsrv_wbd.wbd_hu10sp_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_f01
ON cipsrv_wbd.wbd_hu10sp_f3_32161(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_f02
ON cipsrv_wbd.wbd_hu10sp_f3_32161(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_f03
ON cipsrv_wbd.wbd_hu10sp_f3_32161(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_f04
ON cipsrv_wbd.wbd_hu10sp_f3_32161(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32161_spx
ON cipsrv_wbd.wbd_hu10sp_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3_32161;

--******************************--
----- materialized views/wbd_hu10sp_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu12sp_f3_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_pk
ON cipsrv_wbd.wbd_hu10sp_f3_32655(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_u01
ON cipsrv_wbd.wbd_hu10sp_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_u02
ON cipsrv_wbd.wbd_hu10sp_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f01
ON cipsrv_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f02
ON cipsrv_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f03
ON cipsrv_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_f04
ON cipsrv_wbd.wbd_hu10sp_f3_32655(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32655_spx
ON cipsrv_wbd.wbd_hu10sp_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3_32655;

--******************************--
----- materialized views/wbd_hu10sp_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc10
,CAST(NULL AS VARCHAR(255))           AS hutype
,CAST(NULL AS VARCHAR(30))            AS humod
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc10
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc12,1,10)    AS huc10
      ,ST_UNION(aaa.shape)       AS shape 
      FROM cipsrv_wbd.wbd_hu12sp_f3_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc12,1,10)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu10_f3 c
ON
c.huc10 = a.huc10
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc10;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_pk
ON cipsrv_wbd.wbd_hu10sp_f3_32702(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_u01
ON cipsrv_wbd.wbd_hu10sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_u02
ON cipsrv_wbd.wbd_hu10sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_f01
ON cipsrv_wbd.wbd_hu10sp_f3_32702(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_f02
ON cipsrv_wbd.wbd_hu10sp_f3_32702(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_f03
ON cipsrv_wbd.wbd_hu10sp_f3_32702(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_f04
ON cipsrv_wbd.wbd_hu10sp_f3_32702(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_32702_spx
ON cipsrv_wbd.wbd_hu10sp_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3_32702;

--******************************--
----- materialized views/wbd_hu8sp_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_f3_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu8sp_f3_5070(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu8sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu8sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_f01
ON cipsrv_wbd.wbd_hu8sp_f3_5070(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_f02
ON cipsrv_wbd.wbd_hu8sp_f3_5070(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_f03
ON cipsrv_wbd.wbd_hu8sp_f3_5070(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu8sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_f3_5070;

--******************************--
----- materialized views/wbd_hu8sp_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_f3_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu8sp_f3_3338(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu8sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu8sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_f01
ON cipsrv_wbd.wbd_hu8sp_f3_3338(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_f02
ON cipsrv_wbd.wbd_hu8sp_f3_3338(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_f03
ON cipsrv_wbd.wbd_hu8sp_f3_3338(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu8sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_f3_3338;

--******************************--
----- materialized views/wbd_hu8sp_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_f3_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_pk
ON cipsrv_wbd.wbd_hu8sp_f3_26904(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_u01
ON cipsrv_wbd.wbd_hu8sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_u02
ON cipsrv_wbd.wbd_hu8sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_f01
ON cipsrv_wbd.wbd_hu8sp_f3_26904(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_f02
ON cipsrv_wbd.wbd_hu8sp_f3_26904(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_f03
ON cipsrv_wbd.wbd_hu8sp_f3_26904(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_26904_spx
ON cipsrv_wbd.wbd_hu8sp_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_f3_26904;

--******************************--
----- materialized views/wbd_hu8sp_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_f3_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_pk
ON cipsrv_wbd.wbd_hu8sp_f3_32161(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_u01
ON cipsrv_wbd.wbd_hu8sp_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_u02
ON cipsrv_wbd.wbd_hu8sp_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_f01
ON cipsrv_wbd.wbd_hu8sp_f3_32161(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_f02
ON cipsrv_wbd.wbd_hu8sp_f3_32161(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_f03
ON cipsrv_wbd.wbd_hu8sp_f3_32161(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32161_spx
ON cipsrv_wbd.wbd_hu8sp_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_f3_32161;

--******************************--
----- materialized views/wbd_hu8sp_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_f3_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_pk
ON cipsrv_wbd.wbd_hu8sp_f3_32655(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_u01
ON cipsrv_wbd.wbd_hu8sp_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_u02
ON cipsrv_wbd.wbd_hu8sp_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_f01
ON cipsrv_wbd.wbd_hu8sp_f3_32655(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_f02
ON cipsrv_wbd.wbd_hu8sp_f3_32655(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_f03
ON cipsrv_wbd.wbd_hu8sp_f3_32655(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32655_spx
ON cipsrv_wbd.wbd_hu8sp_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_f3_32655;

--******************************--
----- materialized views/wbd_hu8sp_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc8
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc8
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc10,1,8)     AS huc8
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu10sp_f3_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc10,1,8)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu8_f3 c
ON
c.huc8 = a.huc8
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc8;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_pk
ON cipsrv_wbd.wbd_hu8sp_f3_32702(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_u01
ON cipsrv_wbd.wbd_hu8sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_u02
ON cipsrv_wbd.wbd_hu8sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_f01
ON cipsrv_wbd.wbd_hu8sp_f3_32702(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_f02
ON cipsrv_wbd.wbd_hu8sp_f3_32702(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_f03
ON cipsrv_wbd.wbd_hu8sp_f3_32702(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_32702_spx
ON cipsrv_wbd.wbd_hu8sp_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8sp_f3_32702;

--******************************--
----- materialized views/wbd_hu6sp_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu6sp_f3_5070(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu6sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu6sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_5070_f01
ON cipsrv_wbd.wbd_hu6sp_f3_5070(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_5070_f02
ON cipsrv_wbd.wbd_hu6sp_f3_5070(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu6sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_5070;

--******************************--
----- materialized views/wbd_hu6sp_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu6sp_f3_3338(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu6sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu6sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_f01
ON cipsrv_wbd.wbd_hu6sp_f3_3338(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_f02
ON cipsrv_wbd.wbd_hu6sp_f3_3338(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu6sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_3338;

--******************************--
----- materialized views/wbd_hu6sp_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_26904_pk
ON cipsrv_wbd.wbd_hu6sp_f3_26904(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_26904_u01
ON cipsrv_wbd.wbd_hu6sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_26904_u02
ON cipsrv_wbd.wbd_hu6sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_26904_f01
ON cipsrv_wbd.wbd_hu6sp_f3_26904(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_26904_f02
ON cipsrv_wbd.wbd_hu6sp_f3_26904(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_26904_spx
ON cipsrv_wbd.wbd_hu6sp_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_26904;

--******************************--
----- materialized views/wbd_hu6sp_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32161_pk
ON cipsrv_wbd.wbd_hu6sp_f3_32161(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32161_u01
ON cipsrv_wbd.wbd_hu6sp_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32161_u02
ON cipsrv_wbd.wbd_hu6sp_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32161_f01
ON cipsrv_wbd.wbd_hu6sp_f3_32161(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32161_f02
ON cipsrv_wbd.wbd_hu6sp_f3_32161(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32161_spx
ON cipsrv_wbd.wbd_hu6sp_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_32161;

--******************************--
----- materialized views/wbd_hu6sp_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_32655(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32655_pk
ON cipsrv_wbd.wbd_hu6sp_f3_32655(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32655_u01
ON cipsrv_wbd.wbd_hu6sp_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32655_u02
ON cipsrv_wbd.wbd_hu6sp_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32655_f01
ON cipsrv_wbd.wbd_hu6sp_f3_32655(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32655_f02
ON cipsrv_wbd.wbd_hu6sp_f3_32655(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32655_spx
ON cipsrv_wbd.wbd_hu6sp_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_32655;

--******************************--
----- materialized views/wbd_hu6sp_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3_32702(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc6
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc6
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc8,1,6)      AS huc6
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu8sp_f3_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc8,1,6)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu6_f3 c
ON
c.huc6 = a.huc6
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc6;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_pk
ON cipsrv_wbd.wbd_hu6sp_f3_32702(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_u01
ON cipsrv_wbd.wbd_hu6sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_u02
ON cipsrv_wbd.wbd_hu6sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_f01
ON cipsrv_wbd.wbd_hu6sp_f3_32702(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_f02
ON cipsrv_wbd.wbd_hu6sp_f3_32702(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_32702_spx
ON cipsrv_wbd.wbd_hu6sp_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3_32702;

--******************************--
----- materialized views/wbd_hu4sp_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_5070(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu4sp_f3_5070(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu4sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu4sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_5070_f01
ON cipsrv_wbd.wbd_hu4sp_f3_5070(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu4sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_5070;

--******************************--
----- materialized views/wbd_hu4sp_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_3338(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu4sp_f3_3338(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu4sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu4sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_3338_f01
ON cipsrv_wbd.wbd_hu4sp_f3_3338(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu4sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_3338;

--******************************--
----- materialized views/wbd_hu4sp_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_26904(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_26904_pk
ON cipsrv_wbd.wbd_hu4sp_f3_26904(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_26904_u01
ON cipsrv_wbd.wbd_hu4sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_26904_u02
ON cipsrv_wbd.wbd_hu4sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_26904_f01
ON cipsrv_wbd.wbd_hu4sp_f3_26904(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_26904_spx
ON cipsrv_wbd.wbd_hu4sp_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_26904;

--******************************--
----- materialized views/wbd_hu4sp_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_32161(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32161_pk
ON cipsrv_wbd.wbd_hu4sp_f3_32161(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32161_u01
ON cipsrv_wbd.wbd_hu4sp_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32161_u02
ON cipsrv_wbd.wbd_hu4sp_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32161_f01
ON cipsrv_wbd.wbd_hu4sp_f3_32161(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32161_spx
ON cipsrv_wbd.wbd_hu4sp_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32161;

--******************************--
----- materialized views/wbd_hu4sp_f3_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_32655(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_32655 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32655_pk
ON cipsrv_wbd.wbd_hu4sp_f3_32655(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32655_u01
ON cipsrv_wbd.wbd_hu4sp_f3_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32655_u02
ON cipsrv_wbd.wbd_hu4sp_f3_32655(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32655_f01
ON cipsrv_wbd.wbd_hu4sp_f3_32655(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32655_spx
ON cipsrv_wbd.wbd_hu4sp_f3_32655 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32655;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32655;

--******************************--
----- materialized views/wbd_hu4sp_f3_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3_32702(
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc4
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc4
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc6,1,4)      AS huc4
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu6sp_f3_32702 aaa
      GROUP BY
      SUBSTR(aaa.huc6,1,4)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu4_f3 c
ON
c.huc4 = a.huc4
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc4;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3_32702 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_pk
ON cipsrv_wbd.wbd_hu4sp_f3_32702(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_u01
ON cipsrv_wbd.wbd_hu4sp_f3_32702(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_u02
ON cipsrv_wbd.wbd_hu4sp_f3_32702(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_f01
ON cipsrv_wbd.wbd_hu4sp_f3_32702(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_32702_spx
ON cipsrv_wbd.wbd_hu4sp_f3_32702 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32702;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3_32702;

--******************************--
----- materialized views/wbd_hu2sp_f3_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_f3_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_f3_5070(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_f3_5070 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_f3 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_f3_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_f3_5070 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_5070_pk
ON cipsrv_wbd.wbd_hu2sp_f3_5070(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_5070_u01
ON cipsrv_wbd.wbd_hu2sp_f3_5070(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_5070_u02
ON cipsrv_wbd.wbd_hu2sp_f3_5070(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_f3_5070_spx
ON cipsrv_wbd.wbd_hu2sp_f3_5070 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_f3_5070;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_f3_5070;

--******************************--
----- materialized views/wbd_hu2sp_f3_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_f3_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_f3_3338(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_f3_3338 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_f3 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_f3_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_f3_3338 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_3338_pk
ON cipsrv_wbd.wbd_hu2sp_f3_3338(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_3338_u01
ON cipsrv_wbd.wbd_hu2sp_f3_3338(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_3338_u02
ON cipsrv_wbd.wbd_hu2sp_f3_3338(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_f3_3338_spx
ON cipsrv_wbd.wbd_hu2sp_f3_3338 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_f3_3338;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_f3_3338;

--******************************--
----- materialized views/wbd_hu2sp_f3_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_f3_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_f3_26904(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_f3_26904 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_f3 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_f3_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_f3_26904 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_26904_pk
ON cipsrv_wbd.wbd_hu2sp_f3_26904(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_26904_u01
ON cipsrv_wbd.wbd_hu2sp_f3_26904(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_26904_u02
ON cipsrv_wbd.wbd_hu2sp_f3_26904(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_f3_26904_spx
ON cipsrv_wbd.wbd_hu2sp_f3_26904 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_f3_26904;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_f3_26904;

--******************************--
----- materialized views/wbd_hu2sp_f3_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_f3_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_f3_32161(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
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
,c.areasqkm
,c.areaacres
,b.name
,c.states
,a.huc2
,c.centermass_x
,c.centermass_y
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.huc2
   ,ST_COLLECTIONEXTRACT(aa.shape,3)         AS shape
   FROM (
      SELECT
       SUBSTR(aaa.huc4,1,2)      AS huc2
      ,ST_UNION(aaa.shape)       AS shape 
      FROM 
      cipsrv_wbd.wbd_hu4sp_f3_32161 aaa
      GROUP BY
      SUBSTR(aaa.huc4,1,2)
   ) aa
) a
JOIN
cipsrv_wbd.wbd_hu2_f3 c
ON
c.huc2 = a.huc2
LEFT JOIN
cipsrv_wbd.wbd_names b
ON
b.huc = a.huc2;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_f3_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_f3_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_32161_pk
ON cipsrv_wbd.wbd_hu2sp_f3_32161(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_32161_u01
ON cipsrv_wbd.wbd_hu2sp_f3_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_32161_u02
ON cipsrv_wbd.wbd_hu2sp_f3_32161(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_f3_32161_spx
ON cipsrv_wbd.wbd_hu2sp_f3_32161 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_f3_32161;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_f3_32161;

--******************************--
----- materialized views/wbd_hu10sp_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu10sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu10sp_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc10
   ,hutype
   ,humod
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc10
,a.hutype
,a.humod
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc10
   ,aa.hutype
   ,aa.humod
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc10
   ,bb.hutype
   ,bb.humod
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc10
   ,cc.hutype
   ,cc.humod
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc10
   ,dd.hutype
   ,dd.humod
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc10
   ,ee.hutype
   ,ee.humod
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc10
   ,ff.hutype
   ,ff.humod
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu10sp_f3_32702 ff
   UNION ALL
   SELECT
    gg.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,gg.areasqkm
   ,gg.areaacres
   ,gg.name
   ,gg.states
   ,gg.huc10
   ,gg.hutype
   ,gg.humod
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       bbb.tnmid
      ,ggg.metasourceid
      ,ggg.loaddate
      ,ggg.areasqkm
      ,ggg.areaacres
      ,bbb.name
      ,ggg.states
      ,SUBSTR(ggg.huc12,1,10) AS huc10
      ,ggg.hutype
      ,ggg.humod
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,ggg.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      LEFT JOIN
      cipsrv_wbd.wbd_names bbb
      ON
      bbb.huc = SUBSTR(ggg.huc12,1,10)
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
   ) gg
) a;

ALTER TABLE cipsrv_wbd.wbd_hu10sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu10sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_pk
ON cipsrv_wbd.wbd_hu10sp_f3(huc10);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_u01
ON cipsrv_wbd.wbd_hu10sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu10sp_f3_u02
ON cipsrv_wbd.wbd_hu10sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_f01
ON cipsrv_wbd.wbd_hu10sp_f3(SUBSTR(huc10,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_f02
ON cipsrv_wbd.wbd_hu10sp_f3(SUBSTR(huc10,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_f03
ON cipsrv_wbd.wbd_hu10sp_f3(SUBSTR(huc10,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_f04
ON cipsrv_wbd.wbd_hu10sp_f3(SUBSTR(huc10,1,8));

CREATE INDEX IF NOT EXISTS wbd_hu10sp_f3_spx
ON cipsrv_wbd.wbd_hu10sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu10sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu10sp_f3;

--******************************--
----- materialized views/wbd_hu8sp_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu8sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu8sp_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc8
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc8
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc8
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc8
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc8
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc8
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc8
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc8
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu8sp_f3_32702 ff
   UNION ALL
   SELECT
    gg.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,gg.areasqkm
   ,gg.areaacres
   ,gg.name
   ,gg.states
   ,gg.huc8
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       bbb.tnmid
      ,ggg.metasourceid
      ,ggg.loaddate
      ,ggg.areasqkm
      ,ggg.areaacres
      ,bbb.name
      ,ggg.states
      ,SUBSTR(ggg.huc12,1,8) AS huc8
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,ggg.shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      LEFT JOIN
      cipsrv_wbd.wbd_names bbb
      ON
      bbb.huc = SUBSTR(ggg.huc12,1,8)
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
   ) gg
) a;

ALTER TABLE cipsrv_wbd.wbd_hu8sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu8sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_pk
ON cipsrv_wbd.wbd_hu8sp_f3(huc8);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_u01
ON cipsrv_wbd.wbd_hu8sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu8sp_f3_u02
ON cipsrv_wbd.wbd_hu8sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_f01
ON cipsrv_wbd.wbd_hu8sp_f3(SUBSTR(huc8,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_f02
ON cipsrv_wbd.wbd_hu8sp_f3(SUBSTR(huc8,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_f03
ON cipsrv_wbd.wbd_hu8sp_f3(SUBSTR(huc8,1,6));

CREATE INDEX IF NOT EXISTS wbd_hu8sp_f3_spx
ON cipsrv_wbd.wbd_hu8sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu8sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu8_f3;

--******************************--
----- materialized views/wbd_hu6sp_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu6sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu6sp_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc6
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc6
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc6
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc6
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc6
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc6
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc6
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc6
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu6sp_f3_32702 ff
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc6
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'220400' AS huc6
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,(SELECT ST_UNION(gggg.shape) FROM cipsrv_wbd.wbd_hu12_f3 gggg WHERE SUBSTR(gggg.huc12,1,6) = '220400') AS shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,6) = '220400'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc6
) a;

ALTER TABLE cipsrv_wbd.wbd_hu6sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu6sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_pk
ON cipsrv_wbd.wbd_hu6sp_f3(huc6);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_u01
ON cipsrv_wbd.wbd_hu6sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu6sp_f3_u02
ON cipsrv_wbd.wbd_hu6sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_f01
ON cipsrv_wbd.wbd_hu6sp_f3(SUBSTR(huc6,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_f02
ON cipsrv_wbd.wbd_hu6sp_f3(SUBSTR(huc6,1,4));

CREATE INDEX IF NOT EXISTS wbd_hu6sp_f3_spx
ON cipsrv_wbd.wbd_hu6sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu6sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu6sp_f3;

--******************************--
----- materialized views/wbd_hu4sp_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu4sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu4sp_f3(
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
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc4
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc4
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc4
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc4
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc4
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_f3_32161 dd
   UNION ALL
   SELECT
    ee.tnmid
   ,ee.metasourceid
   ,ee.loaddate
   ,ee.areasqkm
   ,ee.areaacres
   ,ee.name
   ,ee.states
   ,ee.huc4
   ,ee.centermass_x
   ,ee.centermass_y
   ,ee.globalid
   ,ST_TRANSFORM(ee.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_f3_32655 ee
   UNION ALL
   SELECT
    ff.tnmid
   ,ff.metasourceid
   ,ff.loaddate
   ,ff.areasqkm
   ,ff.areaacres
   ,ff.name
   ,ff.states
   ,ff.huc4
   ,ff.centermass_x
   ,ff.centermass_y
   ,ff.globalid
   ,ST_TRANSFORM(ff.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu4sp_f3_32702 ff
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc4
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'2204' AS huc4
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,ggg.globalid
      ,(SELECT ST_UNION(gggg.shape) FROM cipsrv_wbd.wbd_hu12sp_f3 gggg WHERE SUBSTR(gggg.huc12,1,4) = '2204') AS shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc4
) a;

ALTER TABLE cipsrv_wbd.wbd_hu4sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu4sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_pk
ON cipsrv_wbd.wbd_hu4sp_f3(huc4);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_u01
ON cipsrv_wbd.wbd_hu4sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu4sp_f3_u02
ON cipsrv_wbd.wbd_hu4sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_f01
ON cipsrv_wbd.wbd_hu4sp_f3(SUBSTR(huc4,1,2));

CREATE INDEX IF NOT EXISTS wbd_hu4sp_f3_spx
ON cipsrv_wbd.wbd_hu4sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu4sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu4sp_f3;


--******************************--
----- materialized views/wbd_hu2sp_f3.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_wbd.wbd_hu2sp_f3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_wbd.wbd_hu2sp_f3(
    objectid
   ,tnmid
   ,metasourceid
   ,loaddate
   ,areasqkm
   ,areaacres
   ,name
   ,states
   ,huc2
   ,centermass_x
   ,centermass_y
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER() AS objectid
,a.tnmid
,a.metasourceid
,a.loaddate
,a.areasqkm
,a.areaacres
,a.name
,a.states
,a.huc2
,a.centermass_x
,a.centermass_y
,a.globalid
,a.shape
FROM (
   SELECT
    aa.tnmid
   ,aa.metasourceid
   ,aa.loaddate
   ,aa.areasqkm
   ,aa.areaacres
   ,aa.name
   ,aa.states
   ,aa.huc2
   ,aa.centermass_x
   ,aa.centermass_y
   ,aa.globalid
   ,ST_TRANSFORM(aa.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_f3_5070 aa
   UNION ALL
   SELECT
    bb.tnmid
   ,bb.metasourceid
   ,bb.loaddate
   ,bb.areasqkm
   ,bb.areaacres
   ,bb.name
   ,bb.states
   ,bb.huc2
   ,bb.centermass_x
   ,bb.centermass_y
   ,bb.globalid
   ,ST_TRANSFORM(bb.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_f3_3338 bb
   UNION ALL
   SELECT
    cc.tnmid
   ,cc.metasourceid
   ,cc.loaddate
   ,cc.areasqkm
   ,cc.areaacres
   ,cc.name
   ,cc.states
   ,cc.huc2
   ,cc.centermass_x
   ,cc.centermass_y
   ,cc.globalid
   ,ST_TRANSFORM(cc.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_f3_26904 cc
   UNION ALL
   SELECT
    dd.tnmid
   ,dd.metasourceid
   ,dd.loaddate
   ,dd.areasqkm
   ,dd.areaacres
   ,dd.name
   ,dd.states
   ,dd.huc2
   ,dd.centermass_x
   ,dd.centermass_y
   ,dd.globalid
   ,ST_TRANSFORM(dd.shape,4269) AS shape
   FROM 
   cipsrv_wbd.wbd_hu2sp_f3_32161 dd
   UNION ALL
   SELECT
    nn.tnmid
   ,gg.metasourceid
   ,gg.loaddate
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000001   ,4) AS areasqkm
   ,ROUND(ST_AREA(ST_TRANSFORM(gg.shape,4326)::GEOGRAPHY)::NUMERIC * 0.000247105,4) AS areaacres
   ,nn.name
   ,gg.states
   ,gg.huc2
   ,gg.centermass_x
   ,gg.centermass_y
   ,'{' || uuid_generate_v1() || '}' AS globalid
   ,gg.shape
   FROM (
      SELECT
       ggg.metasourceid
      ,ggg.loaddate
      ,ggg.states
      ,'22' AS huc2
      ,ggg.centermass_x
      ,ggg.centermass_y
      ,'{' || uuid_generate_v1() || '}' AS globalid
      ,(SELECT 
        ST_UNION(gggg.shape) 
        FROM (
           SELECT 
           a1.shape 
           FROM 
           cipsrv_wbd.wbd_hu12sp_f3 a1 
           WHERE 
           SUBSTR(a1.huc12,1,4) = '2204'
           UNION ALL
           SELECT
           ST_TRANSFORM(a2.shape,4269)
           FROM
           cipsrv_wbd.wbd_hu4sp_f3_32655 a2
           UNION ALL
           SELECT
           ST_TRANSFORM(a3.shape,4269)
           FROM
           cipsrv_wbd.wbd_hu4sp_f3_32702 a3
        ) gggg
       ) AS shape
      FROM
      cipsrv_wbd.wbd_hu12sp_f3 ggg
      WHERE
      SUBSTR(ggg.huc12,1,4) = '2204'
      LIMIT 1
   ) gg
   LEFT JOIN
   cipsrv_wbd.wbd_names nn
   ON
   nn.huc = gg.huc2
) a;

ALTER TABLE cipsrv_wbd.wbd_hu2sp_f3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_wbd.wbd_hu2sp_f3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_pk
ON cipsrv_wbd.wbd_hu2sp_f3(huc2);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_u01
ON cipsrv_wbd.wbd_hu2sp_f3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS wbd_hu2sp_f3_u02
ON cipsrv_wbd.wbd_hu2sp_f3(globalid);

CREATE INDEX IF NOT EXISTS wbd_hu2sp_f3_spx
ON cipsrv_wbd.wbd_hu2sp_f3 USING gist(shape);

ANALYZE cipsrv_wbd.wbd_hu2sp_f3;

--VACUUM FREEZE ANALYZE cipsrv_wbd.wbd_hu2sp_f3;


