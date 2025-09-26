/*
 the logic encapsulated in the catchment_fabric_<srid>_3 materialized view is compact and concise.
 however for the 5070 hr version just takes a very long time to complete, on my system its about four days
 the alt version cuts that to several hours by breaking the work into smaller chunks, particularly
 by slicing up the state cutting into much smaller bite-sized chunks that can be spread as needed
 across several cores for simultaneous processing

*/

DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq;
CREATE SEQUENCE cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq START WITH 1;

-------------------------------------------------------------------------------
-- ('AL','AR','AZ','CA','CO','CT','DC','DE') --
DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4ad CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4ad(
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
 NEXTVAL('cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq')::INTEGER AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,CASE 
 WHEN a.codecount = 1
 THEN
   a.areasqkm
 ELSE
   ST_AREA(a.shape)::NUMERIC / 1000000
 END AS areasqkm
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
    bb.catchmentstatecode
   ,bb.codecount
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
   ,aa.shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 aa
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 bb
   ON
   aa.nhdplusid = bb.nhdplusid
   WHERE
       bb.catchmentstatecode IN ('AL','AR','AZ','CA','CO','CT','DC','DE')
   AND bb.codecount = 1
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.codecount
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,cc.vpuid
   ,cc.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
           cipsrv_nhdplus_h.snap_to_common_grid(
              p_geometry      := (SELECT ee.shape FROM cipsrv_support.tiger_fedstatewaters_5070 ee WHERE ee.stusps = dd.catchmentstatecode)
             ,p_known_region  := '5070'
             ,p_grid_size     := 0.001
           )
          ,cc.shape
       )
      ,3
    ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 cc
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 dd
   ON
   cc.nhdplusid = dd.nhdplusid
   WHERE
       dd.catchmentstatecode IN ('AL','AR','AZ','CA','CO','CT','DC','DE')
   AND dd.codecount > 1  
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4ad OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4ad TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4ad_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4ad(catchmentstatecode,nhdplusid);

-------------------------------------------------------------------------------
-- ('FL','GA','HI','ID','IL','IN','IA') --
DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4fi CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4fi(
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
 NEXTVAL('cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq')::INTEGER AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,CASE 
 WHEN a.codecount = 1
 THEN
   a.areasqkm
 ELSE
   ST_AREA(a.shape)::NUMERIC / 1000000
 END AS areasqkm
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
    bb.catchmentstatecode
   ,bb.codecount
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
   ,aa.shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 aa
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 bb
   ON
   aa.nhdplusid = bb.nhdplusid
   WHERE
       bb.catchmentstatecode IN ('FL','GA','HI','ID','IL','IN','IA')
   AND bb.codecount = 1
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.codecount
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,cc.vpuid
   ,cc.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
           cipsrv_nhdplus_h.snap_to_common_grid(
              p_geometry      := (SELECT ee.shape FROM cipsrv_support.tiger_fedstatewaters_5070 ee WHERE ee.stusps = dd.catchmentstatecode)
             ,p_known_region  := '5070'
             ,p_grid_size     := 0.001
           )
          ,cc.shape
       )
      ,3
    ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 cc
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 dd
   ON
   cc.nhdplusid = dd.nhdplusid
   WHERE
       dd.catchmentstatecode IN ('FL','GA','HI','ID','IL','IN','IA')
   AND dd.codecount > 1  
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4fi OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4fi TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4fi_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4fi(catchmentstatecode,nhdplusid);

-------------------------------------------------------------------------------
-- ('KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT') --
DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4km CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4km(
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
 NEXTVAL('cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq')::INTEGER AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,CASE 
 WHEN a.codecount = 1
 THEN
   a.areasqkm
 ELSE
   ST_AREA(a.shape)::NUMERIC / 1000000
 END AS areasqkm
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
    bb.catchmentstatecode
   ,bb.codecount
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
   ,aa.shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 aa
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 bb
   ON
   aa.nhdplusid = bb.nhdplusid
   WHERE
       bb.catchmentstatecode IN ('KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT')
   AND bb.codecount = 1
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.codecount
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,cc.vpuid
   ,cc.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
           cipsrv_nhdplus_h.snap_to_common_grid(
              p_geometry      := (SELECT ee.shape FROM cipsrv_support.tiger_fedstatewaters_5070 ee WHERE ee.stusps = dd.catchmentstatecode)
             ,p_known_region  := '5070'
             ,p_grid_size     := 0.001
           )
          ,cc.shape
       )
      ,3
    ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 cc
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 dd
   ON
   cc.nhdplusid = dd.nhdplusid
   WHERE
       dd.catchmentstatecode IN ('KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT')
   AND dd.codecount > 1  
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4km OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4km TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4km_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4km(catchmentstatecode,nhdplusid);

-------------------------------------------------------------------------------
-- ('NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA') --
DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4np CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4np(
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
 NEXTVAL('cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq')::INTEGER AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,CASE 
 WHEN a.codecount = 1
 THEN
   a.areasqkm
 ELSE
   ST_AREA(a.shape)::NUMERIC / 1000000
 END AS areasqkm
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
    bb.catchmentstatecode
   ,bb.codecount
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
   ,aa.shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 aa
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 bb
   ON
   aa.nhdplusid = bb.nhdplusid
   WHERE
       bb.catchmentstatecode IN ('NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA')
   AND bb.codecount = 1
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.codecount
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,cc.vpuid
   ,cc.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
           cipsrv_nhdplus_h.snap_to_common_grid(
              p_geometry      := (SELECT ee.shape FROM cipsrv_support.tiger_fedstatewaters_5070 ee WHERE ee.stusps = dd.catchmentstatecode)
             ,p_known_region  := '5070'
             ,p_grid_size     := 0.001
           )
          ,cc.shape
       )
      ,3
    ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 cc
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 dd
   ON
   cc.nhdplusid = dd.nhdplusid
   WHERE
       dd.catchmentstatecode IN ('NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA')
   AND dd.codecount > 1  
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4np OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4np TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4np_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4np(catchmentstatecode,nhdplusid);

-------------------------------------------------------------------------------
-- ('RI','SC','SD','TN','TX','UT','VA','VT','WA','WV','WI','WY') --
DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4rw CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4rw(
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
 NEXTVAL('cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq')::INTEGER AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,CASE 
 WHEN a.codecount = 1
 THEN
   a.areasqkm
 ELSE
   ST_AREA(a.shape)::NUMERIC / 1000000
 END AS areasqkm
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
    bb.catchmentstatecode
   ,bb.codecount
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
   ,aa.shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 aa
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 bb
   ON
   aa.nhdplusid = bb.nhdplusid
   WHERE
       bb.catchmentstatecode IN ('RI','SC','SD','TN','TX','UT','VA','VT','WA','WV','WI','WY')
   AND bb.codecount = 1
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.codecount
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,cc.vpuid
   ,cc.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
           cipsrv_nhdplus_h.snap_to_common_grid(
              p_geometry      := (SELECT ee.shape FROM cipsrv_support.tiger_fedstatewaters_5070 ee WHERE ee.stusps = dd.catchmentstatecode)
             ,p_known_region  := '5070'
             ,p_grid_size     := 0.001
           )
          ,cc.shape
       )
      ,3
    ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 cc
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 dd
   ON
   cc.nhdplusid = dd.nhdplusid
   WHERE
       dd.catchmentstatecode IN ('RI','SC','SD','TN','TX','UT','VA','VT','WA','WV','WI','WY')
   AND dd.codecount > 1  
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4rw OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4rw TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4rw_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4rw(catchmentstatecode,nhdplusid);

-------------------------------------------------------------------------------
-- ('CN','MX','OW') --
DROP MATERIALIZED VIEW IF EXISTS cipsrv_epageofab_h.catchment_fabric_5070_3alt4ow CASCADE;

CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4ow(
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
 NEXTVAL('cipsrv_epageofab_h.catchment_fabric_5070_3alt4_seq')::INTEGER AS objectid
,a.catchmentstatecode
,a.nhdplusid
,a.istribal
,a.istribal_areasqkm
,a.sourcefc
,a.gridcode
,CASE 
 WHEN a.codecount = 1
 THEN
   a.areasqkm
 ELSE
   ST_AREA(a.shape)::NUMERIC / 1000000
 END AS areasqkm
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
    bb.catchmentstatecode
   ,bb.codecount
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
   ,aa.shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 aa
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 bb
   ON
   aa.nhdplusid = bb.nhdplusid
   WHERE
       bb.catchmentstatecode IN ('CN','MX','OW')
   AND bb.codecount = 1
   UNION ALL
   SELECT
    dd.catchmentstatecode
   ,dd.codecount
   ,cc.nhdplusid
   ,cc.istribal
   ,cc.istribal_areasqkm
   ,cc.sourcefc
   ,cc.gridcode
   ,cc.areasqkm
   ,cc.isnavigable
   ,cc.hasvaa
   ,cc.issink
   ,cc.isheadwater
   ,cc.iscoastal
   ,cc.isocean
   ,cc.isalaskan
   ,cc.h3hexagonaddr
   ,cc.vpuid
   ,cc.sourcedataset
   ,ST_COLLECTIONEXTRACT(
       ST_INTERSECTION(
           cipsrv_nhdplus_h.snap_to_common_grid(
              p_geometry      := (SELECT ee.shape FROM cipsrv_support.outerwaters_5070 ee WHERE ee.itemcode = dd.catchmentstatecode)
             ,p_known_region  := '5070'
             ,p_grid_size     := 0.001
           )
          ,cc.shape
       )
      ,3
    ) AS shape
   FROM
   cipsrv_epageofab_h.catchment_fabric_5070_2 cc
   JOIN
   cipsrv_epageofab_h.catchment_fabric_5070_3alt3 dd
   ON
   cc.nhdplusid = dd.nhdplusid
   WHERE
       dd.catchmentstatecode IN ('CN','MX','OW')
   AND dd.codecount > 1  
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4ow OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4ow TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4ow_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4ow(catchmentstatecode,nhdplusid);

-------------------------------------------------------------------------------
--- Combine into final results
CREATE MATERIALIZED VIEW cipsrv_epageofab_h.catchment_fabric_5070_3alt4(
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
a.*
FROM 
cipsrv_epageofab_h.catchment_fabric_5070_3alt4ad a
UNION ALL
SELECT
b.*
FROM 
cipsrv_epageofab_h.catchment_fabric_5070_3alt4fi b
UNION ALL
SELECT
c.*
FROM 
cipsrv_epageofab_h.catchment_fabric_5070_3alt4km c
UNION ALL
SELECT
d.*
FROM 
cipsrv_epageofab_h.catchment_fabric_5070_3alt4np d
UNION ALL
SELECT
e.*
FROM 
cipsrv_epageofab_h.catchment_fabric_5070_3alt4rw e
UNION ALL
SELECT
f.*
FROM 
cipsrv_epageofab_h.catchment_fabric_5070_3alt4ow f;

ALTER TABLE cipsrv_epageofab_h.catchment_fabric_5070_3alt4 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4_pk
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4(catchmentstatecode,nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4_u01
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4_u02
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4(globalid);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4_01i
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4(catchmentstatecode);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4_02i
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4(nhdplusid);

CREATE INDEX IF NOT EXISTS catchment_fabric_5070_3alt4_spx
ON cipsrv_epageofab_h.catchment_fabric_5070_3alt4 USING gist(shape);

ANALYZE cipsrv_epageofab_h.catchment_fabric_5070_3alt4;

--VACUUM FREEZE ANALYZE cipsrv_epageofab_h.catchment_fabric_5070_3alt4;
