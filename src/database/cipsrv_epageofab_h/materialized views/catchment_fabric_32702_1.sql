DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_32702_1 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_32702_1(
    objectid
   ,catchmentstatecode
   ,nhdplusid
   ,istribal
   ,istribal_areasqkm
   ,sourcefc
   ,gridcode
   ,areasqkm
   ,isnavigable
   ,hasvaa
   ,issink
   ,isheadwater
   ,iscoastal
   ,isocean
   ,isalaskan
   ,h3hexagonaddr
   ,state_count
   ,vpuid
   ,sourcedataset
   ,globalid
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER()                  AS objectid
,CAST(NULL AS VARCHAR(2))             AS catchmentstatecode
,a.nhdplusid::BIGINT                  AS nhdplusid
,CAST(NULL AS VARCHAR(1))             AS istribal
,CAST(NULL AS NUMERIC)                AS istribal_areasqkm
,a.sourcefc
,a.gridcode
,a.areasqkm
,CASE 
 WHEN b.nhdplusid IS NOT NULL AND b.fcode NOT IN (56600)
 THEN
   CAST('Y' AS VARCHAR(1))
 ELSE
   CAST('N' AS VARCHAR(1))
 END AS isnavigable
,CASE 
 WHEN b.nhdplusid IS NOT NULL
 THEN
   CAST('Y' AS VARCHAR(1))
 ELSE
   CAST('N' AS VARCHAR(1))
 END AS hasvaa
,CASE 
 WHEN a.sourcefc IN ('NHDPlusSink','Sink') 
 THEN
   CAST('Y' AS VARCHAR(1))
 ELSE
   CAST('N' AS VARCHAR(1))
 END AS issink
,CASE 
 WHEN b.startflag IS NOT NULL AND b.startflag = 1
 THEN
   CAST('Y' AS VARCHAR(1))
 ELSE
   CAST('N' AS VARCHAR(1))
 END AS isheadwater
,CASE 
 WHEN b.fcode IS NOT NULL AND b.fcode = 56600
 THEN
   CAST('Y' AS VARCHAR(1))
 ELSE
   CAST('N' AS VARCHAR(1))
 END AS iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
,CAST(NULL AS INTEGER)                AS state_count
,a.vpuid
,a.sourcedataset
,'{' || uuid_generate_v1() || '}'     AS globalid
,a.shape
FROM (
   SELECT
    aa.nhdplusid
   ,aa.sourcefc
   ,aa.gridcode
   ,aa.areasqkm
   ,CAST('N' AS VARCHAR(1))   AS isocean
   ,CAST('N' AS VARCHAR(1))   AS isalaskan
   ,CAST(NULL AS VARCHAR(64)) AS h3hexagonaddr
   ,aa.vpuid
   ,CAST('nhdpluscatchment' AS VARCHAR(32)) AS sourcedataset
   ,ST_TRANSFORM(aa.shape,32702) AS shape
   FROM   
   cipsrv_nhdplus_h.nhdpluscatchment aa
   WHERE
   aa.shape && cipsrv_nhdplus_h.generic_common_mbr('32702')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(aa.shape) = 32702
   UNION ALL
   SELECT
    bb.nhdplusid
   ,bb.sourcefc
   ,bb.gridcode
   ,bb.areasqkm
   ,CAST('Y' AS VARCHAR(1))   AS isocean
   ,CAST('N' AS VARCHAR(1))   AS isalaskan
   ,bb.h3hexagonaddr
   ,bb.vpuid
   ,CAST('oceancatchment' AS VARCHAR(32)) AS sourcedataset
   ,ST_TRANSFORM(bb.shape,32702) AS shape
   FROM   
   cipsrv_epageofab_h.oceancatchment bb
   WHERE
       bb.shape && cipsrv_nhdplus_h.generic_common_mbr('32702')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(bb.shape) = 32702
) a
LEFT JOIN
cipsrv_nhdplus_h.networknhdflowline b
ON
b.nhdplusid = a.nhdplusid;

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_32702_1 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_32702_1 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_32702_1_pk
ON cipsrv_epageofab_h.catchment_fabric_32702_1(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_32702_1_u01
ON cipsrv_epageofab_h.catchment_fabric_32702_1(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_32702_1_u02
ON cipsrv_epageofab_h.catchment_fabric_32702_1(globalid);

CREATE INDEX IF NOT EXISTS catchment_fabric_32702_1_spx
ON cipsrv_epageofab_h.catchment_fabric_32702_1 USING gist(shape);

ANALYZE cipsrv_epageofab_h.catchment_fabric_32702_1;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.catchment_fabric_32702_1;

