DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.tw_nhdflowline_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.tw_nhdflowline_32655(
    objectid
   ,statecode
   ,nhdplusid
   ,permanent_identifier
   ,lengthkm
   ,totma
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,vpuid
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER()                  AS objectid
,a.statecode
,a.nhdplusid
,a.permanent_identifier
,a.lengthkm
,NULL::NUMERIC AS totma
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.vpuid
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.statecode
   ,aa.nhdplusid
   ,aa.permanent_identifier
   ,ST_LENGTH(ST_TRANSFORM(aa.shape,4326)::GEOGRAPHY)::NUMERIC / 1000  AS lengthkm
   ,aa.reachcode
   ,aa.flowdir
   ,aa.wbarea_permanent_identifier
   ,aa.ftype
   ,aa.fcode
   ,aa.vpuid
   ,aa.shape
   FROM (
      SELECT
       bbb.stusps                             AS statecode
      ,aaa.nhdplusid
      ,aaa.permanent_identifier
      ,aaa.reachcode
      ,aaa.flowdir
      ,aaa.wbarea_permanent_identifier
      ,aaa.ftype
      ,aaa.fcode
      ,aaa.vpuid
      ,ST_COLLECTIONEXTRACT(ST_INTERSECTION(bbb.shape,aaa.shape,0.05),2) AS shape
      FROM
      cipsrv_nhdplus_h.nhdflowline_32655 aaa
      INNER JOIN LATERAL (
         SELECT
          bbbb.stusps
         ,bbbb.shape
         FROM
         cipsrv_support.tiger_fedstatewaters_32655 bbbb
      ) AS bbb
      ON
      ST_INTERSECTS(bbb.shape,aaa.shape)
   ) aa
   WHERE
       aa.shape IS NOT NULL
   AND NOT ST_ISEMPTY(aa.shape)
) a
WHERE
a.lengthkm > 0.000001;

ALTER TABLE cipsrv_epageofab_h.tw_nhdflowline_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.tw_nhdflowline_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tw_nhdflowline_32655_pk
ON cipsrv_epageofab_h.tw_nhdflowline_32655(statecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS tw_nhdflowline_32655_u01
ON cipsrv_epageofab_h.tw_nhdflowline_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS tw_nhdflowline_32655_u02
ON cipsrv_epageofab_h.tw_nhdflowline_32655(globalid);

CREATE INDEX IF NOT EXISTS tw_nhdflowline_32655_01i
ON cipsrv_epageofab_h.tw_nhdflowline_32655(statecode);

CREATE INDEX IF NOT EXISTS tw_nhdflowline_32655_02i
ON cipsrv_epageofab_h.tw_nhdflowline_32655(nhdplusid);

CREATE INDEX IF NOT EXISTS tw_nhdflowline_32655_spx
ON cipsrv_epageofab_h.tw_nhdflowline_32655 USING gist(shape);

ANALYZE cipsrv_epageofab_h.tw_nhdflowline_32655;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.tw_nhdflowline_32655;

