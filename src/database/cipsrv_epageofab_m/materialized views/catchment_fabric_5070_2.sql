DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_m.catchment_fabric_5070_2 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_epageofab_m.catchment_fabric_5070_2_seq;
CREATE SEQUENCE cipsrv_epageofab_m.catchment_fabric_5070_2_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_epageofab_m.catchment_fabric_5070_2(
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
WITH 
 tribalcatchments AS (
   SELECT
    aa.nhdplusid
   ,CAST(NULL AS VARCHAR(1))                     AS istribal
   ,ST_AREA(aa.tribal_shape)::NUMERIC / 1000000  AS istribal_areasqkm
   ,bb.sourcefc
   ,bb.gridcode
   ,bb.areasqkm
   ,bb.isnavigable
   ,bb.hasvaa
   ,bb.issink
   ,bb.isheadwater
   ,bb.iscoastal
   ,bb.isocean
   ,bb.isalaskan
   ,bb.h3hexagonaddr
   ,bb.vpuid
   ,bb.sourcedataset
   ,bb.shape
   FROM (
      SELECT
       aaa.nhdplusid
      ,ST_UNION(aaa.tribal_shape) AS tribal_shape
      FROM (
         SELECT
          aaaa.nhdplusid
         ,bbbb.geoid
         ,ST_INTERSECTION(bbbb.shape,aaaa.shape,0.05) AS tribal_shape
         FROM 
         cipsrv_epageofab_m.catchment_fabric_5070_1 aaaa
         INNER JOIN LATERAL (
            SELECT
             bbbbb.geoid
            ,bbbbb.shape
            FROM
            cipsrv_support.tiger_aiannha_5070 bbbbb
            WHERE
                bbbbb.classfp IN ('D0','D1','D2','D3')
            AND bbbbb.aiannhr = 'F'
         ) AS bbbb
         ON
         ST_INTERSECTS(bbbb.shape,aaaa.shape)
      ) aaa
      GROUP BY 
      aaa.nhdplusid
   ) aa
   JOIN
   cipsrv_epageofab_m.catchment_fabric_5070_1 bb
   ON
   aa.nhdplusid = bb.nhdplusid
)
,nontribalcatchments AS (
   SELECT
    bb.nhdplusid
   ,CAST('N' AS VARCHAR(1)) AS istribal
   ,CAST(NULL AS NUMERIC)   AS istribal_areasqkm
   ,bb.sourcefc
   ,bb.gridcode
   ,bb.areasqkm
   ,bb.isnavigable
   ,bb.hasvaa
   ,bb.issink
   ,bb.isheadwater
   ,bb.iscoastal
   ,bb.isocean
   ,bb.isalaskan
   ,bb.h3hexagonaddr
   ,bb.vpuid
   ,bb.sourcedataset
   ,bb.shape
   FROM
   cipsrv_epageofab_m.catchment_fabric_5070_1 bb
   WHERE
   NOT EXISTS (
      SELECT
      1
      FROM
      tribalcatchments bbb
      WHERE
      bbb.nhdplusid = bb.nhdplusid
   )
)
SELECT
 NEXTVAL('cipsrv_epageofab_m.catchment_fabric_5070_2_seq')::INTEGER AS objectid
,CAST(NULL AS VARCHAR(2))               AS catchmentstatecode
,a.nhdplusid
,CASE 
 WHEN a.istribal IS NULL
 THEN
   CASE
   WHEN ROUND(a.istribal_areasqkm,4) = ROUND(a.areasqkm::NUMERIC,4)
   THEN
      CAST('F' AS VARCHAR(1))
   ELSE
      CAST('P' AS VARCHAR(1))
   END
 ELSE
   a.istribal
 END                                    AS istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,a.areasqkm
,a.isnavigable
,a.hasvaa
,a.issink
,a.isheadwater
,a.iscoastal
,a.isocean
,a.isalaskan
,a.h3hexagonaddr
,CAST(NULL AS INTEGER)                  AS state_count
,a.vpuid
,a.sourcedataset
,'{' || uuid_generate_v1() || '}'       AS globalid
,a.shape
FROM (
   SELECT
    aa.nhdplusid
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
   ,aa.shape
   FROM
   tribalcatchments aa
   UNION ALL
   SELECT
    bb.nhdplusid
   ,bb.istribal
   ,bb.istribal_areasqkm
   ,bb.sourcefc
   ,bb.gridcode
   ,bb.areasqkm
   ,bb.isnavigable
   ,bb.hasvaa
   ,bb.issink
   ,bb.isheadwater
   ,bb.iscoastal
   ,bb.isocean
   ,bb.isalaskan
   ,bb.h3hexagonaddr
   ,bb.vpuid
   ,bb.sourcedataset
   ,bb.shape
   FROM   
   nontribalcatchments bb
) a;

ALTER TABLE cipsrv_epageofab_m.catchment_fabric_5070_2 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_m.catchment_fabric_5070_2 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_2_pk
ON cipsrv_epageofab_m.catchment_fabric_5070_2(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_2_u01
ON cipsrv_epageofab_m.catchment_fabric_5070_2(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_2_u02
ON cipsrv_epageofab_m.catchment_fabric_5070_2(globalid);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_2_spx
ON cipsrv_epageofab_m.catchment_fabric_5070_2 USING gist(shape);

ANALYZE cipsrv_epageofab_m.catchment_fabric_5070_2;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_m.catchment_fabric_5070_2;

