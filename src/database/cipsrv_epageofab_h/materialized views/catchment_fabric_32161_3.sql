DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_32161_3 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_32161_3(
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
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,ST_AREA(a.shape)::NUMERIC / 1000000  AS areasqkm
,a.isnavigable
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
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
    bb.stusps                             AS catchmentstatecode
   ,aa.nhdplusid
   ,aa.istribal
   ,aa.istribal_areasqkm
   ,aa.sourcefc
   ,aa.gridcode
   ,aa.areasqkm
   ,aa.isnavigable
   ,aa.hasvaa
   ,aa.issink
   ,aa.isheadwater
   ,aa.iscoastal
   ,aa.isocean
   ,aa.isalaskan
   ,aa.h3hexagonaddr
   ,aa.vpuid
   ,aa.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
         cipsrv_nhdplus_h.snap_to_common_grid(
             p_geometry      := bb.shape
            ,p_known_region  := '32161'
            ,p_grid_size     := 0.001
          )
         ,cipsrv_nhdplus_h.snap_to_common_grid(
             p_geometry      := aa.shape
            ,p_known_region  := '32161'
            ,p_grid_size     := 0.001
          )
       )     
      ,3
   ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_32161_2 aa
   INNER JOIN LATERAL (
      SELECT
       bbb.stusps
      ,bbb.shape
      FROM
      cipsrv_support.tiger_fedstatewaters_32161 bbb
   ) AS bb
   ON
   ST_INTERSECTS(bb.shape,aa.shape)
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape)
AND a.areasqkm > 0.00000005;

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_32161_3 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_32161_3 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_32161_3_pk
ON cipsrv_epageofab_h.catchment_fabric_32161_3(catchmentstatecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_32161_3_u01
ON cipsrv_epageofab_h.catchment_fabric_32161_3(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_32161_3_u02
ON cipsrv_epageofab_h.catchment_fabric_32161_3(globalid);

CREATE INDEX IF NOT EXISTS catchment_fabric_32161_3_01i
ON cipsrv_epageofab_h.catchment_fabric_32161_3(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_fabric_32161_3_02i
ON cipsrv_epageofab_h.catchment_fabric_32161_3(nhdplusid);

CREATE INDEX IF NOT EXISTS catchment_fabric_32161_3_spx
ON cipsrv_epageofab_h.catchment_fabric_32161_3 USING gist(shape);

ANALYZE cipsrv_epageofab_h.catchment_fabric_32161_3;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.catchment_fabric_32161_3;

