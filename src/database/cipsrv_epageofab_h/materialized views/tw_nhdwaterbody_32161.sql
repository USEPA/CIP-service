DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.tw_nhdwaterbody_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.tw_nhdwaterbody_32161(
    objectid
   ,statecode
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,areasqkm
   ,elevation
   ,reachcode
   ,ftype
   ,fcode
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,onoffnet
   ,purpcode
   ,burn
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER()                  AS objectid
,a.statecode
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.areasqkm
,a.elevation
,a.reachcode
,a.ftype
,a.fcode
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.onoffnet
,a.purpcode
,a.burn
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.statecode
   ,aa.permanent_identifier
   ,aa.fdate
   ,aa.resolution
   ,aa.gnis_id
   ,aa.gnis_name
   ,ST_AREA(ST_TRANSFORM(aa.shape,4326)::GEOGRAPHY)::NUMERIC / 1000000  AS areasqkm
   ,aa.elevation
   ,aa.reachcode
   ,aa.ftype
   ,aa.fcode
   ,aa.visibilityfilter
   ,aa.nhdplusid
   ,aa.vpuid
   ,aa.onoffnet
   ,aa.purpcode
   ,aa.burn
   ,aa.shape
   FROM (
      SELECT
       bbb.stusps                             AS statecode
      ,aaa.permanent_identifier
      ,aaa.fdate
      ,aaa.resolution
      ,aaa.gnis_id
      ,aaa.gnis_name
      ,aaa.elevation
      ,aaa.reachcode
      ,aaa.ftype
      ,aaa.fcode
      ,aaa.visibilityfilter
      ,aaa.nhdplusid
      ,aaa.vpuid
      ,aaa.onoffnet
      ,aaa.purpcode
      ,aaa.burn
      ,ST_COLLECTIONEXTRACT(
          ST_INTERSECTION(
             bbb.shape
            ,aaa.shape
            ,0.05
          )
         ,3
      ) AS shape
      FROM
      cipsrv_nhdplus_h.nhdwaterbody_32161 aaa
      INNER JOIN LATERAL (
         SELECT
          bbbb.stusps
         ,bbbb.shape
         FROM
         cipsrv_support.tiger_fedstatewaters_32161 bbbb
      ) AS bbb
      ON
      ST_INTERSECTS(bbb.shape,aaa.shape)
   ) aa
   WHERE
       aa.shape IS NOT NULL
   AND NOT ST_ISEMPTY(aa.shape)
) a
WHERE
a.areasqkm > 0.00000005;

ALTER TABLE cipsrv_epageofab_h.tw_nhdwaterbody_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.tw_nhdwaterbody_32161 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tw_nhdwaterbody_32161_pk
ON cipsrv_epageofab_h.tw_nhdwaterbody_32161(statecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS tw_nhdwaterbody_32161_u01
ON cipsrv_epageofab_h.tw_nhdwaterbody_32161(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS tw_nhdwaterbody_32161_u02
ON cipsrv_epageofab_h.tw_nhdwaterbody_32161(globalid);

CREATE INDEX IF NOT EXISTS tw_nhdwaterbody_32161_01i
ON cipsrv_epageofab_h.tw_nhdwaterbody_32161(statecode);

CREATE INDEX IF NOT EXISTS tw_nhdwaterbody_32161_02i
ON cipsrv_epageofab_h.tw_nhdwaterbody_32161(nhdplusid);

CREATE INDEX IF NOT EXISTS tw_nhdwaterbody_32161_spx
ON cipsrv_epageofab_h.tw_nhdwaterbody_32161 USING gist(shape);

ANALYZE cipsrv_epageofab_h.tw_nhdwaterbody_32161;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.tw_nhdwaterbody_32161;

