DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.catchment_5070 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.catchment_5070_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.catchment_5070_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.catchment_5070(
    objectid
   ,nhdplusid
   ---
   ,hydroseq
   ,levelpathi
   ,lengthkm
   ,tonode
   ,fromnode
   ,connector_tonode
   ,connector_fromnode
   ---
   ,fcode
   ---
   ,istribal
   ,istribal_areasqkm
   ,isnavigable
   ,hasvaa
   ,issink
   ,isheadwater
   ,iscoastal
   ,isocean
   ,isalaskan
   ,h3hexagonaddr
   ---
   ,areasqkm
   ,shape
   ,shape_centroid
   ,catchmentstatecodes
   ,vpuid
   ,statesplit
   ,border_status
)
AS
WITH subselect AS (
   SELECT
   s.*
   FROM
   cipsrv_epageofab_h.catchment_fabric s
   WHERE
      s.catchmentstatecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
   AND (
          s.shape && cipsrv_nhdplus_h.generic_common_mbr('5070')
      AND cipsrv_nhdplus_h.determine_grid_srid_f(s.shape) = 5070
   )
)
SELECT
 NEXTVAL('cipsrv_nhdplus_h.catchment_5070_seq') AS objectid
,a.nhdplusid
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,b.fcode::INTEGER           AS fcode
---
,a.istribal
,a.istribal_areasqkm
,a.isnavigable
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
---
,a.areasqkm
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
,a.catchmentstatecodes
,a.vpuid
,a.statesplit
,a.border_status
FROM (
   SELECT
    CAST(aa.nhdplusid AS BIGINT) AS nhdplusid
   ,aa.istribal
   ,aa.istribal_areasqkm
   ,CASE WHEN aa.isnavigable = 'Y' THEN TRUE ELSE FALSE END AS isnavigable
   ,CASE WHEN aa.hasvaa      = 'Y' THEN TRUE ELSE FALSE END AS hasvaa
   ,CASE WHEN aa.issink      = 'Y' THEN TRUE ELSE FALSE END AS issink
   ,CASE WHEN aa.isheadwater = 'Y' THEN TRUE ELSE FALSE END AS isheadwater
   ,CASE WHEN aa.iscoastal   = 'Y' THEN TRUE ELSE FALSE END AS iscoastal
   ,CASE WHEN aa.isocean     = 'Y' THEN TRUE ELSE FALSE END AS isocean
   ,CASE WHEN aa.isalaskan   = 'Y' THEN TRUE ELSE FALSE END AS isalaskan
   ,aa.h3hexagonaddr
   ,aa.areasqkm
   ,cipsrv_nhdplus_h.snap_to_common_grid(
       p_geometry      := ST_Transform(aa.shape,5070)
      ,p_known_region  := '5070'
      ,p_grid_size     := 0.001
    ) AS shape
   ,ARRAY[aa.catchmentstatecode]::VARCHAR[] AS catchmentstatecodes
   ,aa.vpuid
   ,CASE
    WHEN aa.state_count = 1
    THEN
      CAST(0 AS INTEGER)
    ELSE
      CAST(1 AS INTEGER)
    END AS statesplit
   ,aa.border_status
   FROM
   subselect aa
   UNION ALL 
   SELECT
    bb.nhdplusid
   ,bb.istribal
   ,bb.istribal_areasqkm
   ,bb.isnavigable
   ,bb.hasvaa
   ,bb.issink
   ,bb.isheadwater
   ,bb.iscoastal
   ,bb.isocean
   ,bb.isalaskan
   ,bb.h3hexagonaddr
   ,bb.areasqkm
   ,bb.shape
   ,bb.catchmentstatecodes
   ,bb.vpuid
   ,bb.statesplit
   ,bb.border_status
   FROM (
      SELECT
       bbb.nhdplusid::BIGINT AS nhdplusid
      ,(ARRAY_AGG(bbb.istribal ORDER BY CASE WHEN bbb.istribal = 'P' THEN 1 WHEN bbb.istribal = 'F' THEN 2 WHEN bbb.istribal = 'N' THEN 3 ELSE 4 END ASC))[1] AS istribal
      ,SUM(bbb.istribal_areasqkm) AS istribal_areasqkm
      ,bool_or(CASE WHEN bbb.isnavigable = 'Y' THEN TRUE ELSE FALSE END) AS isnavigable
      ,bool_or(CASE WHEN bbb.hasvaa      = 'Y' THEN TRUE ELSE FALSE END) AS hasvaa
      ,bool_or(CASE WHEN bbb.issink      = 'Y' THEN TRUE ELSE FALSE END) AS issink
      ,bool_or(CASE WHEN bbb.isheadwater = 'Y' THEN TRUE ELSE FALSE END) AS isheadwater
      ,bool_or(CASE WHEN bbb.iscoastal   = 'Y' THEN TRUE ELSE FALSE END) AS iscoastal
      ,bool_or(CASE WHEN bbb.isocean     = 'Y' THEN TRUE ELSE FALSE END) AS isocean
      ,bool_or(CASE WHEN bbb.isalaskan   = 'Y' THEN TRUE ELSE FALSE END) AS isalaskan
      ,MAX(bbb.h3hexagonaddr) AS h3hexagonaddr
      ,SUM(bbb.areasqkm) AS areasqkm
      ,ST_COLLECTIONEXTRACT(
          ST_UNION(
              cipsrv_nhdplus_h.snap_to_common_grid(
                 p_geometry      := ST_Transform(bbb.shape,5070)
                ,p_known_region  := '5070'
                ,p_grid_size     := 0.001
              )
          )     
         ,3
       ) AS shape
      ,ARRAY_AGG(bbb.catchmentstatecode)::VARCHAR[] AS catchmentstatecodes
      ,MAX(bbb.vpuid) AS vpuid
      ,CAST(2 AS INTEGER) AS statesplit
      ,MIN(bbb.border_status) AS border_status /* should always be the same across cuts */
      FROM
      subselect bbb
      WHERE
      bbb.state_count > 1
      GROUP BY
      bbb.nhdplusid::BIGINT
   ) bb
) a
LEFT JOIN
cipsrv_nhdplus_h.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid;

ALTER TABLE cipsrv_nhdplus_h.catchment_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_5070 TO public;

CREATE UNIQUE INDEX catchment_5070_01u
ON cipsrv_nhdplus_h.catchment_5070(catchmentstatecodes,nhdplusid);

CREATE UNIQUE INDEX catchment_5070_02u
ON cipsrv_nhdplus_h.catchment_5070(catchmentstatecodes,hydroseq);

CREATE UNIQUE INDEX catchment_5070_03u
ON cipsrv_nhdplus_h.catchment_5070(objectid);

CREATE INDEX catchment_5070_01i
ON cipsrv_nhdplus_h.catchment_5070(nhdplusid);

CREATE INDEX catchment_5070_02i
ON cipsrv_nhdplus_h.catchment_5070(hydroseq);

CREATE INDEX catchment_5070_03i
ON cipsrv_nhdplus_h.catchment_5070(levelpathi);

CREATE INDEX catchment_5070_04i
ON cipsrv_nhdplus_h.catchment_5070(fcode);

CREATE INDEX catchment_5070_05i
ON cipsrv_nhdplus_h.catchment_5070(istribal);

CREATE INDEX catchment_5070_06i
ON cipsrv_nhdplus_h.catchment_5070(isnavigable);

CREATE INDEX catchment_5070_07i
ON cipsrv_nhdplus_h.catchment_5070(iscoastal);

CREATE INDEX catchment_5070_08i
ON cipsrv_nhdplus_h.catchment_5070(isocean);

CREATE INDEX catchment_5070_09i
ON cipsrv_nhdplus_h.catchment_5070(statesplit);

CREATE INDEX catchment_5070_10i
ON cipsrv_nhdplus_h.catchment_5070(vpuid);

CREATE INDEX catchment_5070_11i
ON cipsrv_nhdplus_h.catchment_5070(border_status);

CREATE INDEX catchment_5070_01f
ON cipsrv_nhdplus_h.catchment_5070(SUBSTR(vpuid,1,2));

CREATE INDEX catchment_5070_spx
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape);

CREATE INDEX catchment_5070_spx2
ON cipsrv_nhdplus_h.catchment_5070 USING GIST(shape_centroid);

CREATE INDEX catchment_5070_gin
ON cipsrv_nhdplus_h.catchment_5070 USING GIN(catchmentstatecodes);

ANALYZE cipsrv_nhdplus_h.catchment_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.catchment_5070;

