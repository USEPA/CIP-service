--******************************--
----- materialized views/nhdplusflowlinevaa_catnodes.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(
    nhdplusid
   ,hydroseq
   ,levelpathi
   ,fromnode
   ,tonode
   ,connector_fromnode
   ,connector_tonode
   ,lengthkm
)
AS
WITH cat AS (
    SELECT
     aa.nhdplusid
    ,aa.hydroseq
    ,aa.levelpathi
    ,aa.uphydroseq
    ,aa.dnhydroseq
    ,aa.fromnode
    ,aa.tonode
    ,aa.lengthkm
    FROM
    cipsrv_nhdplus_m.nhdplusflowlinevaa aa
    WHERE 
    EXISTS (SELECT 1 FROM cipsrv_nhdplus_m.catchment_fabric bb WHERE bb.nhdplusid = aa.nhdplusid)
)
,nocat AS (
   SELECT
    cc.hydroseq
   ,cc.levelpathi
   ,cc.fromnode
   ,cc.tonode
   FROM
   cipsrv_nhdplus_m.nhdplusflowlinevaa cc
   WHERE 
   NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_m.catchment_fabric dd WHERE dd.nhdplusid = cc.nhdplusid)
) 
SELECT
 a.nhdplusid
,a.hydroseq
,a.levelpathi
,a.fromnode
,a.tonode
,b.fromnode
,c.tonode
,a.lengthkm
FROM
cat a
LEFT JOIN
nocat b
ON
b.hydroseq = a.uphydroseq
LEFT JOIN 
nocat c
ON
c.hydroseq = a.dnhydroseq;

ALTER TABLE cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_catnodes_01u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(nhdplusid);

CREATE UNIQUE INDEX nhdplusflowlinevaa_catnodes_02u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(hydroseq);

CREATE INDEX nhdplusflowlinevaa_catnodes_01i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(levelpathi);

CREATE INDEX nhdplusflowlinevaa_catnodes_02i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(fromnode);

CREATE INDEX nhdplusflowlinevaa_catnodes_03i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(tonode);

CREATE INDEX nhdplusflowlinevaa_catnodes_04i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(connector_fromnode);

CREATE INDEX nhdplusflowlinevaa_catnodes_05i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes(connector_tonode);

ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes;
--******************************--
----- materialized views/nhdplusflowlinevaa_levelpathi.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(
    levelpathi
   ,max_hydroseq
   ,min_hydroseq
   ,fromnode
   ,tonode
   ,levelpathilengthkm
)
AS
SELECT
 a.levelpathi
,a.max_hydroseq
,a.min_hydroseq
,(SELECT c.fromnode FROM cipsrv_nhdplus_m.nhdplusflowlinevaa c WHERE c.hydroseq = a.max_hydroseq) AS fromnode
,(SELECT d.tonode   FROM cipsrv_nhdplus_m.nhdplusflowlinevaa d WHERE d.hydroseq = a.min_hydroseq) AS tonode
,a.levelpathilengthkm 
FROM (
   SELECT
    aa.levelpathi
   ,MAX(aa.hydroseq) AS max_hydroseq
   ,MIN(aa.hydroseq) AS min_hydroseq
   ,SUM(aa.lengthkm) AS levelpathilengthkm
   FROM
   cipsrv_nhdplus_m.nhdplusflowlinevaa aa
   GROUP BY
   aa.levelpathi
) a;

ALTER TABLE cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi TO public;

CREATE UNIQUE INDEX nhdplusflowlinevaa_levelpathi_01u
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(levelpathi);

CREATE INDEX nhdplusflowlinevaa_levelpathi_01i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(max_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_02i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(min_hydroseq);

CREATE INDEX nhdplusflowlinevaa_levelpathi_03i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(fromnode);

CREATE INDEX nhdplusflowlinevaa_levelpathi_04i
ON cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi(tonode);

ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.nhdplusflowlinevaa_nocat;
--******************************--
----- materialized views/catchment_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_3338(
    nhdplusid
   ,areasqkm
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
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,3338)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AK')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_3338 TO public;

CREATE UNIQUE INDEX catchment_3338_01u
ON cipsrv_nhdplus_m.catchment_3338(nhdplusid);

CREATE UNIQUE INDEX catchment_3338_02u
ON cipsrv_nhdplus_m.catchment_3338(hydroseq);

CREATE INDEX catchment_3338_01i
ON cipsrv_nhdplus_m.catchment_3338(levelpathi);

CREATE INDEX catchment_3338_02i
ON cipsrv_nhdplus_m.catchment_3338(fcode);

CREATE INDEX catchment_3338_spx
ON cipsrv_nhdplus_m.catchment_3338 USING GIST(shape);

CREATE INDEX catchment_3338_spx2
ON cipsrv_nhdplus_m.catchment_3338 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_3338;

--******************************--
----- materialized views/catchment_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_5070(
    nhdplusid
   ,areasqkm
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
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,5070)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode NOT IN ('AK','HI','PR','VI','GU','MP','AS')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_5070 TO public;

CREATE UNIQUE INDEX catchment_5070_01u
ON cipsrv_nhdplus_m.catchment_5070(nhdplusid);

CREATE UNIQUE INDEX catchment_5070_02u
ON cipsrv_nhdplus_m.catchment_5070(hydroseq);

CREATE INDEX catchment_5070_01i
ON cipsrv_nhdplus_m.catchment_5070(levelpathi);

CREATE INDEX catchment_5070_02i
ON cipsrv_nhdplus_m.catchment_5070(fcode);

CREATE INDEX catchment_5070_spx
ON cipsrv_nhdplus_m.catchment_5070 USING GIST(shape);

CREATE INDEX catchment_5070_spx2
ON cipsrv_nhdplus_m.catchment_5070 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_5070;

--******************************--
----- materialized views/catchment_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_26904(
    nhdplusid
   ,areasqkm
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
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,26904)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('HI')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_26904 TO public;

CREATE UNIQUE INDEX catchment_26904_01u
ON cipsrv_nhdplus_m.catchment_26904(nhdplusid);

CREATE UNIQUE INDEX catchment_26904_02u
ON cipsrv_nhdplus_m.catchment_26904(hydroseq);

CREATE INDEX catchment_26904_01i
ON cipsrv_nhdplus_m.catchment_26904(levelpathi);

CREATE INDEX catchment_26904_02i
ON cipsrv_nhdplus_m.catchment_26904(fcode);

CREATE INDEX catchment_26904_spx
ON cipsrv_nhdplus_m.catchment_26904 USING GIST(shape);

CREATE INDEX catchment_26904_spx2
ON cipsrv_nhdplus_m.catchment_26904 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_26904;

--******************************--
----- materialized views/catchment_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_32161(
    nhdplusid
   ,areasqkm
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
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,32161)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('PR','VI')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32161 TO public;

CREATE UNIQUE INDEX catchment_32161_01u
ON cipsrv_nhdplus_m.catchment_32161(nhdplusid);

CREATE UNIQUE INDEX catchment_32161_02u
ON cipsrv_nhdplus_m.catchment_32161(hydroseq);

CREATE INDEX catchment_32161_01i
ON cipsrv_nhdplus_m.catchment_32161(levelpathi);

CREATE INDEX catchment_32161_02i
ON cipsrv_nhdplus_m.catchment_32161(fcode);

CREATE INDEX catchment_32161_spx
ON cipsrv_nhdplus_m.catchment_32161 USING GIST(shape);

CREATE INDEX catchment_32161_spx2
ON cipsrv_nhdplus_m.catchment_32161 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_32161;

--******************************--
----- materialized views/catchment_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_32655(
    nhdplusid
   ,areasqkm
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
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,32655)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('GU','MP')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32655 TO public;

CREATE UNIQUE INDEX catchment_32655_01u
ON cipsrv_nhdplus_m.catchment_32655(nhdplusid);

CREATE UNIQUE INDEX catchment_32655_02u
ON cipsrv_nhdplus_m.catchment_32655(hydroseq);

CREATE INDEX catchment_32655_01i
ON cipsrv_nhdplus_m.catchment_32655(levelpathi);

CREATE INDEX catchment_32655_02i
ON cipsrv_nhdplus_m.catchment_32655(fcode);

CREATE INDEX catchment_32655_spx
ON cipsrv_nhdplus_m.catchment_32655 USING GIST(shape);

CREATE INDEX catchment_32655_spx2
ON cipsrv_nhdplus_m.catchment_32655 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_32655;

--******************************--
----- materialized views/catchment_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_m.catchment_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_m.catchment_32702(
    nhdplusid
   ,areasqkm
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
   ,shape
   ,shape_centroid
)
AS
SELECT
 a.nhdplusid::BIGINT        AS nhdplusid
,a.areasqkm
---
,b.hydroseq
,b.levelpathi
,b.lengthkm
,b.tonode
,b.fromnode
,b.connector_tonode
,b.connector_fromnode
---
,c.fcode::INTEGER           AS fcode
---
,a.shape
,ST_PointOnSurface(a.shape) AS shape_centroid
FROM (
   SELECT
    aa.nhdplusid::BIGINT
   ,SUM(aa.areasqkm) AS areasqkm
   ,ST_UNION(ST_Transform(aa.shape,32702)) AS shape
   FROM
   cipsrv_nhdplus_m.catchment_fabric aa
   WHERE
   aa.catchmentstatecode IN ('AS')
   GROUP BY
   aa.nhdplusid::BIGINT
) a
LEFT JOIN
cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes b
ON
a.nhdplusid = b.nhdplusid
LEFT JOIN
cipsrv_nhdplus_m.nhdflowline c
ON
a.nhdplusid = c.nhdplusid;

ALTER TABLE cipsrv_nhdplus_m.catchment_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32702 TO public;

CREATE UNIQUE INDEX catchment_32702_01u
ON cipsrv_nhdplus_m.catchment_32702(nhdplusid);

CREATE UNIQUE INDEX catchment_32702_02u
ON cipsrv_nhdplus_m.catchment_32702(hydroseq);

CREATE INDEX catchment_32702_01i
ON cipsrv_nhdplus_m.catchment_32702(levelpathi);

CREATE INDEX catchment_32702_02i
ON cipsrv_nhdplus_m.catchment_32702(fcode);

CREATE INDEX catchment_32702_spx
ON cipsrv_nhdplus_m.catchment_32702 USING GIST(shape);

CREATE INDEX catchment_32702_spx2
ON cipsrv_nhdplus_m.catchment_32702 USING GIST(shape_centroid);

ANALYZE cipsrv_nhdplus_m.catchment_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_m.catchment_32702;

--******************************--
----- functions/generic_common_mbr.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.generic_common_mbr(
   IN  p_input  VARCHAR
) RETURNS GEOMETRY 
IMMUTABLE
AS
$BODY$ 
DECLARE
   str_input VARCHAR(4000) := UPPER(p_input);
   
BEGIN
   
   IF str_input IN ('5070','CONUS','US','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-128.0 20.2,-64.0 20.2,-64.0 52.0,-128.0 52.0,-128.0 20.2))',4326)::geography;
      
   ELSIF str_input IN ('3338','AK','ALASKA')
   THEN
      RETURN ST_MPolyFromText('MULTIPOLYGON(((-180 48,-128 48,-128 90,-180 90,-180 48)),((168 48,180 48,180 90,168 90,168 48)))',4326)::geography;
      
   ELSIF str_input IN ('26904','HI','HAWAII')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-180.0 10.0,-146.0 10.0,-146.0 35.0,-180.0 35.0,-180.0 10.0))',4326)::geography;
      
   ELSIF str_input IN ('32161','PR','VI','PR/VI','PRVI')
   THEN
      RETURN ST_PolygonFromText('POLYGON((-69.0 16.0,-63.0 16.0,-63.0 20.0,-69.0 20.0,-69.0 16.0))',4326)::geography;
   
   ELSIF str_input IN ('32655','GUMP','GUAM','MP','GU')
   THEN
      RETURN ST_PolyFromText('POLYGON((136.0 8.0,154.0 8.0,154.0 25.0,136.0 25.0,136.0 8.0))',4326)::geography;
         
   ELSIF str_input IN ('32702','SAMOA','AS')
   THEN
      RETURN ST_PolyFromText('POLYGON((-178.0 -20.0, -163.0 -20.0, -163.0 -5.0, -178.0 -5.0, -178.0 -20.0))',4326)::geography;
        
   ELSE
      RAISE EXCEPTION 'unknown generic mbr code';
      
   END IF;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.generic_common_mbr(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.generic_common_mbr(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/query_generic_common_mbr.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.query_generic_common_mbr(
   IN  p_input  GEOMETRY
) RETURNS VARCHAR
IMMUTABLE
AS
$BODY$ 
DECLARE
   sdo_point GEOGRAPHY;
   
BEGIN

   IF p_input IS NULL
   THEN
      RETURN NULL;
      
   END IF;
   
   sdo_point := ST_Transform(
       ST_PointOnSurface(p_input)
      ,4326
   )::GEOGRAPHY;
   
   IF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_m.generic_common_mbr('CONUS')
   )
   THEN
      RETURN 'CONUS';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_m.generic_common_mbr('HI')
   )
   THEN
      RETURN 'HI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_m.generic_common_mbr('PRVI')
   )
   THEN
      RETURN 'PRVI';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_m.generic_common_mbr('AK')
   )
   THEN
      RETURN 'AK';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_m.generic_common_mbr('GUMP')
   )
   THEN
      RETURN 'GUMP';
      
   ELSIF ST_Intersects(
       sdo_point
      ,cipsrv_nhdplus_m.generic_common_mbr('SAMOA')
   )
   THEN
      RETURN 'SAMOA';
      
   END IF;
   
   RETURN NULL;  
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.query_generic_common_mbr(
   GEOMETRY
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.query_generic_common_mbr(
   GEOMETRY
) TO PUBLIC;

--******************************--
----- functions/determine_grid_srid.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.determine_grid_srid(
    IN  p_geometry          GEOMETRY
   ,IN  p_known_region      VARCHAR
   ,OUT out_srid            INTEGER
   ,OUT out_grid_size       NUMERIC
   ,OUT out_return_code     INTEGER
   ,OUT out_status_message  VARCHAR
)
STABLE
AS $BODY$ 
DECLARE
   sdo_results        GEOMETRY;
   str_region         VARCHAR(255) := p_known_region;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF  p_geometry IS NULL
   AND str_region IS NULL
   THEN
      RAISE EXCEPTION 'input geometry and known region cannot both be null';
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Determine the region from geometry if known region value not provided
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      str_region := cipsrv_nhdplus_m.query_generic_common_mbr(
         p_input := p_geometry
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Validate region and determine srid
   ----------------------------------------------------------------------------
   IF str_region IS NULL
   THEN
      out_return_code    := -1;
      out_status_message := 'Geometry is outside nhdplus_h coverage.';
      RETURN;

   ELSIF str_region IN ('5070','CONUS','USA',
   'AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
   'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
   'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
   'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
   'TN','TX','UT','VA','VT','WA','WI','WV','WY')
   THEN
      out_srid       := 5070;
      out_grid_size  := 30;
      
   ELSIF str_region IN ('3338','AK')
   THEN  
      out_srid       := 3338;
      out_grid_size  := NULL;
   
   ELSIF str_region IN ('32702','SAMOA','AS')
   THEN
      out_srid       := 32702;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('32655','GUMP','GU','MP')
   THEN
      out_srid       := 32655;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('26904','HI')
   THEN
      out_srid       := 26904;
      out_grid_size  := 10;
      
   ELSIF str_region IN ('32161','PRVI','PR','VI')
   THEN
      out_srid       := 32161;
      out_grid_size  := 10;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   out_return_code := 0;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.determine_grid_srid(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/temp_table_exists.sql 

CREATE or REPLACE FUNCTION cipsrv_nhdplus_m.temp_table_exists(
   IN p_table_name VARCHAR
) RETURNS BOOLEAN 
STABLE
AS $BODY$
DECLARE
   str_table_name VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Query catalog for temp table
   ----------------------------------------------------------------------------
   SELECT 
    n.nspname
   INTO str_table_name
   FROM 
   pg_catalog.pg_class c 
   LEFT JOIN 
   pg_catalog.pg_namespace n 
   ON 
   n.oid = c.relnamespace
   where 
       n.nspname like 'pg_temp_%'
   AND pg_catalog.pg_table_is_visible(c.oid)
   AND UPPER(relname) = UPPER(p_table_name);

   ----------------------------------------------------------------------------
   -- Step 20
   -- See what we gots and exit accordingly
   ----------------------------------------------------------------------------
   IF str_table_name IS NULL 
   THEN
      RETURN FALSE;

   ELSE
      RETURN TRUE;

   END IF;

END;
$BODY$ LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.temp_table_exists(
   VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.temp_table_exists(
   VARCHAR
) TO PUBLIC;

--******************************--
----- functions/create_line_temp_tables.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.create_line_temp_tables()
RETURNS INT4
VOLATILE
AS $BODY$
DECLARE
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Create tmp_line temp table
   ----------------------------------------------------------------------------
   IF cipsrv_nhdplus_m.temp_table_exists('tmp_line')
   THEN
      TRUNCATE TABLE tmp_line;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line(
          nhdplusid               BIGINT
         ,areasqkm                NUMERIC
         ,overlapmeasure          NUMERIC
         ,eventpercentage         NUMERIC
         ,nhdpercentage           NUMERIC
         ,hydroseq                BIGINT
         ,levelpathi              BIGINT
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
      );

      CREATE UNIQUE INDEX tmp_line_pk 
      ON tmp_line(nhdplusid);
      
      CREATE UNIQUE INDEX tmp_line_pk2
      ON tmp_line(hydroseq);
      
      CREATE INDEX tmp_line_01i
      ON tmp_line(levelpathi);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create tmp_line_dd temp table
   ----------------------------------------------------------------------------
   IF cipsrv_nhdplus_m.temp_table_exists('tmp_line_levelpathi')
   THEN
      TRUNCATE TABLE tmp_line_levelpathi;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_line_levelpathi(
          levelpathi              BIGINT
         ,max_hydroseq            BIGINT
         ,min_hydroseq            BIGINT
         ,totaleventpercentage    NUMERIC
         ,totaloverlapmeasure     NUMERIC
         ,levelpathilengthkm      NUMERIC
         ,fromnode                BIGINT
         ,tonode                  BIGINT
         ,connector_fromnode      BIGINT
         ,connector_tonode        BIGINT
      );

      CREATE UNIQUE INDEX tmp_line_levelpathi_pk 
      ON tmp_line_levelpathi(levelpathi);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 70
   -- I guess that went okay
   ----------------------------------------------------------------------------
   RETURN 0;
   
END;
$BODY$ 
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.create_line_temp_tables() OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.create_line_temp_tables() TO PUBLIC;

--******************************--
----- functions/index_point_simple.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_point_simple(
    IN  p_geometry             GEOMETRY
   ,IN  p_known_region         VARCHAR
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;

BEGIN

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry      := p_geometry
      ,p_known_region  := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
      
   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cipsrv_nhdplus_m.catchment_5070 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cipsrv_nhdplus_m.catchment_3338 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cipsrv_nhdplus_m.catchment_26904 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cipsrv_nhdplus_m.catchment_32161 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cipsrv_nhdplus_m.catchment_32655 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM
      cipsrv_nhdplus_m.catchment_32702 a
      WHERE
      ST_Intersects(
          a.shape
         ,geom_input
      )
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_point_simple(
    GEOMETRY
   ,VARCHAR
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_point_simple(
    GEOMETRY
   ,VARCHAR
) TO PUBLIC;

--******************************--
----- functions/index_line_simple.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_line_simple(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threashold_perc    NUMERIC
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;
   num_lin_threshold   NUMERIC;
   int_count           INTEGER;

BEGIN

   IF p_line_threashold_perc IS NULL
   THEN
      num_lin_threshold := 0;
   
   ELSE
      num_lin_threshold := p_line_threashold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;

   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_m.catchment_5070 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;

   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_m.catchment_3338 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_m.catchment_26904 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_m.catchment_32161 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_m.catchment_32655 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,CASE
          WHEN aa.overlapmeasure >= p_geometry_lengthkm
          THEN
            1
          WHEN p_geometry_lengthkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
          END AS eventpercentage
         ,CASE
          WHEN aa.overlapmeasure >= aa.lengthkm
          THEN
            1
          ELSE
            ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
          END AS nhdpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
            ,aaa.lengthkm
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,2
                ) AS geom_overlap
               ,aaaa.lengthkm
               FROM
               cipsrv_nhdplus_m.catchment_32702 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
      num_lin_threshold IS NULL OR a.nhdpercentage >= num_lin_threshold
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;

   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_line_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/index_line_levelpath.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_line_levelpath(
    IN  p_geometry                GEOMETRY
   ,IN  p_geometry_lengthkm       NUMERIC
   ,IN  p_known_region            VARCHAR
   ,IN  p_line_threashold_perc    NUMERIC
   ,OUT out_return_code           INTEGER
   ,OUT out_status_message        VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                    RECORD;
   str_known_region       VARCHAR;
   int_srid               INTEGER;
   geom_input             GEOMETRY;
   geom_part              GEOMETRY;
   num_lin_threshold      NUMERIC;
   int_count              INTEGER;
   num_main_levelpathi    BIGINT;
   ary_main_lp_int_nodes  BIGINT[];
   ary_done_levelpathis   BIGINT[];
   num_fromnode           BIGINT;
   num_tonode             BIGINT;
   num_connector_fromnode BIGINT;
   num_connector_tonode   BIGINT;
   num_min_hydroseq       BIGINT;
   num_max_hydroseq       BIGINT;
   boo_check              BOOLEAN;
   int_debug              INTEGER;
   str_debug              VARCHAR;
   int_geom_count         INTEGER;

BEGIN

   out_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF p_line_threashold_perc IS NULL
   THEN
      num_lin_threshold := 0;
   
   ELSE
      num_lin_threshold := p_line_threashold_perc / 100;
      
   END IF;
   
   str_known_region := p_known_region;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Validate the known region
   ----------------------------------------------------------------------------
   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Loop through any multi-part linestrings
   ----------------------------------------------------------------------------
   int_geom_count := ST_NumGeometries(p_geometry);
   FOR i IN 1 .. int_geom_count
   LOOP
      out_return_code := cipsrv_nhdplus_m.create_line_temp_tables();
      geom_part := ST_GeometryN(p_geometry,i);
      
   ----------------------------------------------------------------------------
   -- Step 40
   -- Load the temp table
   ----------------------------------------------------------------------------      
      IF str_known_region = '5070'
      THEN
         geom_input := ST_Transform(geom_part,5070);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= p_geometry_lengthkm
             THEN
               1
             WHEN p_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  FROM
                  cipsrv_nhdplus_m.catchment_5070 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '3338'
      THEN
         geom_input := ST_Transform(geom_part,3338);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= p_geometry_lengthkm
             THEN
               1
             WHEN p_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  FROM
                  cipsrv_nhdplus_m.catchment_3338 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
      
      ELSIF str_known_region = '26904'
      THEN
         geom_input := ST_Transform(geom_part,26904);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= p_geometry_lengthkm
             THEN
               1
             WHEN p_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  FROM
                  cipsrv_nhdplus_m.catchment_26904 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32161'
      THEN
         geom_input := ST_Transform(geom_part,32161);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= p_geometry_lengthkm
             THEN
               1
             WHEN p_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  FROM
                  cipsrv_nhdplus_m.catchment_32161 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32655'
      THEN
         geom_input := ST_Transform(geom_part,32655);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= p_geometry_lengthkm
             THEN
               1
             WHEN p_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  FROM
                  cipsrv_nhdplus_m.catchment_32655 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
         
      ELSIF str_known_region = '32702'
      THEN
         geom_input := ST_Transform(geom_part,32702);
         
         INSERT INTO tmp_line(
             nhdplusid
            ,areasqkm
            ,overlapmeasure
            ,eventpercentage
            ,nhdpercentage
            ,hydroseq
            ,levelpathi
            ,tonode
            ,fromnode
            ,connector_tonode
            ,connector_fromnode
         ) 
         SELECT
          a.nhdplusid
         ,a.areasqkm
         ,a.overlapmeasure
         ,a.eventpercentage
         ,a.nhdpercentage
         ,a.hydroseq
         ,a.levelpathi
         ,a.tonode
         ,a.fromnode
         ,a.connector_tonode
         ,a.connector_fromnode
         FROM (
            SELECT
             aa.nhdplusid
            ,aa.areasqkm
            ,aa.overlapmeasure
            ,CASE
             WHEN aa.overlapmeasure >= p_geometry_lengthkm
             THEN
               1
             WHEN p_geometry_lengthkm = 0
             THEN
               0
             ELSE
               ROUND(aa.overlapmeasure / p_geometry_lengthkm,8)
             END AS eventpercentage
            ,CASE
             WHEN aa.overlapmeasure >= aa.lengthkm
             THEN
               1
             ELSE
               ROUND(aa.overlapmeasure / GREATEST(aa.lengthkm,0.0001),8)
             END AS nhdpercentage
            ,aa.hydroseq
            ,aa.levelpathi
            ,aa.tonode
            ,aa.fromnode
            ,aa.connector_tonode
            ,aa.connector_fromnode
            FROM (
               SELECT
                aaa.nhdplusid
               ,aaa.areasqkm
               ,ROUND(ST_Length(aaa.geom_overlap)::NUMERIC / 1000,8) AS overlapmeasure
               ,aaa.lengthkm
               ,aaa.hydroseq
               ,aaa.levelpathi
               ,aaa.tonode
               ,aaa.fromnode
               ,aaa.connector_tonode
               ,aaa.connector_fromnode
               FROM (
                  SELECT
                   aaaa.nhdplusid
                  ,aaaa.areasqkm
                  ,ST_CollectionExtract(
                     ST_Intersection(
                         aaaa.shape
                        ,geom_input
                      )
                     ,2
                   ) AS geom_overlap
                  ,aaaa.lengthkm
                  ,aaaa.hydroseq
                  ,aaaa.levelpathi
                  ,aaaa.tonode
                  ,aaaa.fromnode
                  ,aaaa.connector_tonode
                  ,aaaa.connector_fromnode
                  FROM
                  cipsrv_nhdplus_m.catchment_32702 aaaa
                  WHERE
                  ST_Intersects(
                      aaaa.shape
                     ,geom_input
                  )              
               ) aaa
               WHERE
               aaa.geom_overlap IS NOT NULL
            ) aa
         ) a;
      
      ELSE
         RAISE EXCEPTION 'err %',str_known_region;
         
      END IF;
      
      GET DIAGNOSTICS int_count = ROW_COUNT;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Determine the best fit mainpath right upfront
   -- Collect other levelpathis excluding any that touch interior of main
   ----------------------------------------------------------------------------
      IF int_count > 0
      THEN
      
         SELECT
          a.levelpathi
         ,MAX(a.hydroseq)
         ,MIN(a.hydroseq)
         INTO
          num_main_levelpathi
         ,num_max_hydroseq
         ,num_min_hydroseq
         FROM
         tmp_line a
         WHERE
            (num_lin_threshold IS NULL OR a.nhdpercentage > num_lin_threshold)
         OR a.eventpercentage = 1
         GROUP BY
         a.levelpathi
         ORDER BY
         SUM(a.eventpercentage) DESC
         LIMIT 1;
         
         SELECT ARRAY(
            SELECT 
            a.tonode::BIGINT
            FROM
            cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes a
            WHERE
                a.levelpathi = num_main_levelpathi
            AND a.hydroseq >= num_min_hydroseq
            AND a.hydroseq <  num_max_hydroseq
         ) 
         INTO ary_main_lp_int_nodes;
         
         INSERT INTO tmp_line_levelpathi(
             levelpathi
            ,max_hydroseq
            ,min_hydroseq
            ,totaleventpercentage
            ,totaloverlapmeasure
            ,levelpathilengthkm     
            ,fromnode
            ,tonode
            ,connector_fromnode
            ,connector_tonode
         )
         SELECT
          a.levelpathi
         ,a.max_hydroseq
         ,a.min_hydroseq
         ,a.totaleventpercentage
         ,a.totaloverlapmeasure
         ,b.levelpathilengthkm
         ,c.fromnode
         ,d.tonode
         ,c.connector_fromnode
         ,d.connector_tonode
         FROM (
            SELECT
             aa.levelpathi
            ,MAX(aa.hydroseq)        AS max_hydroseq
            ,MIN(aa.hydroseq)        AS min_hydroseq
            ,SUM(aa.eventpercentage) AS totaleventpercentage
            ,SUM(aa.overlapmeasure)  AS totaloverlapmeasure
            FROM
            tmp_line aa
            WHERE (
                  aa.levelpathi = num_main_levelpathi
               OR NOT (aa.tonode = ANY(ary_main_lp_int_nodes))
            )
            AND ( 
               (num_lin_threshold IS NULL OR aa.nhdpercentage > num_lin_threshold)
               OR aa.eventpercentage = 1
            )
            GROUP BY
            aa.levelpathi
         ) a
         JOIN
         cipsrv_nhdplus_m.nhdplusflowlinevaa_levelpathi b
         ON
         a.levelpathi = b.levelpathi
         JOIN
         tmp_line c
         ON
         a.max_hydroseq = c.hydroseq
         JOIN
         tmp_line d
         ON
         a.min_hydroseq = d.hydroseq;
         
         GET DIAGNOSTICS int_count = ROW_COUNT;

         INSERT INTO tmp_cip(
            nhdplusid
         ) 
         SELECT 
         a.nhdplusid
         FROM
         cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes a
         WHERE
             a.levelpathi = num_main_levelpathi
         AND a.hydroseq >= num_min_hydroseq
         AND a.hydroseq <= num_max_hydroseq
         ON CONFLICT DO NOTHING;
 
   ----------------------------------------------------------------------------
   -- Step 70
   -- Loop through the additional levelpathis extending main levelpathi
   ----------------------------------------------------------------------------
         IF int_count > 1
         THEN
            SELECT
             a.levelpathi
            ,a.max_hydroseq
            ,a.min_hydroseq 
            ,a.fromnode
            ,a.tonode
            ,a.connector_fromnode
            ,a.connector_tonode
            INTO
             num_main_levelpathi
            ,num_max_hydroseq
            ,num_min_hydroseq
            ,num_fromnode
            ,num_tonode
            ,num_connector_fromnode
            ,num_connector_tonode
            FROM
            tmp_line_levelpathi a
            WHERE
            a.levelpathi = num_main_levelpathi;
            
            ary_done_levelpathis := '{}';
            
            <<sanity_loop>>
            FOR i IN 1 .. 10
            LOOP
               FOR rec IN (
                  SELECT
                   a.levelpathi
                  ,a.max_hydroseq
                  ,a.min_hydroseq 
                  ,a.fromnode
                  ,a.tonode
                  ,a.connector_fromnode
                  ,a.connector_tonode
                  FROM
                  tmp_line_levelpathi a
                  WHERE
                  a.levelpathi != num_main_levelpathi
                  AND NOT (a.levelpathi = ANY(ary_done_levelpathis))
               ) LOOP
                  boo_check := FALSE;
                  
                  IF num_fromnode IN (rec.fromnode,rec.connector_fromnode)
                  OR num_connector_fromnode IN (rec.fromnode,rec.connector_fromnode)
                  THEN
                     boo_check := TRUE;
                     num_fromnode := rec.tonode;
                     num_connector_fromnode := rec.connector_tonode;
                  
                  ELSIF num_fromnode IN (rec.tonode,rec.connector_tonode)
                  OR num_connector_fromnode IN (rec.tonode,rec.connector_tonode)
                  THEN
                     boo_check := TRUE;
                     num_fromnode := rec.fromnode;
                     num_connector_fromnode := rec.connector_fromnode;
                     
                  ELSIF num_tonode IN (rec.fromnode,rec.connector_fromnode)
                  OR num_connector_tonode IN (rec.fromnode,rec.connector_fromnode)
                  THEN
                     boo_check := TRUE;
                     num_tonode := rec.tonode;
                     num_connector_tonode := rec.connector_tonode;
                  
                  ELSIF num_tonode IN (rec.tonode,rec.connector_tonode)
                  OR num_connector_tonode IN (rec.tonode,rec.connector_tonode)
                  THEN
                     boo_check := TRUE;
                     num_tonode := rec.fromnode;
                     num_connector_tonode := rec.connector_fromnode;   
                     
                  END IF;
                  
                  IF boo_check
                  THEN
                     ary_done_levelpathis := array_append(ary_done_levelpathis,rec.levelpathi);
                     
                     INSERT INTO tmp_cip(
                        nhdplusid
                     ) 
                     SELECT 
                     a.nhdplusid
                     FROM
                     cipsrv_nhdplus_m.nhdplusflowlinevaa_catnodes a
                     WHERE
                         a.levelpathi = rec.levelpathi
                     AND a.hydroseq >= rec.min_hydroseq
                     AND a.hydroseq <= rec.max_hydroseq
                     ON CONFLICT DO NOTHING;
                  
                  ELSE
                     EXIT sanity_loop;
                     
                  END IF;
                  
               END LOOP;
               
            END LOOP;
            
         END IF;
         
      END IF;

   END LOOP;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_line_levelpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_line_levelpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/index_area_simple.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_area_simple(
    IN  p_geometry             GEOMETRY
   ,IN  p_geometry_areasqkm    NUMERIC
   ,IN  p_known_region         VARCHAR
   ,IN  p_cat_threashold_perc  NUMERIC
   ,IN  p_evt_threashold_perc  NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;
   num_cat_threshold   NUMERIC;
   num_evt_threshold   NUMERIC;
   int_count           INTEGER;

BEGIN

   IF p_cat_threashold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threashold_perc / 100;
      
   END IF;
   
   IF p_evt_threashold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threashold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
      
   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_5070 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )  
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_3338 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_26904 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32161 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )  
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32655 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )  
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,CASE
             WHEN aaa.geom_overlap IS NULL
             THEN
               0
             ELSE
               ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8)
             END AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32702 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_area_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_area_simple(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/index_area_centroid.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_area_centroid(
    IN  p_geometry             GEOMETRY
   ,IN  p_geometry_areasqkm    NUMERIC
   ,IN  p_known_region         VARCHAR
   ,IN  p_cat_threashold_perc  NUMERIC
   ,IN  p_evt_threashold_perc  NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;
   num_cat_threshold   NUMERIC;
   num_evt_threshold   NUMERIC;

BEGIN

   IF p_cat_threashold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threashold_perc / 100;
      
   END IF;
   
   IF p_evt_threashold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threashold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry      := p_geometry
      ,p_known_region  := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
      
   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_5070 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_3338 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_26904 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32161 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      )
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32655 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32702 aaaa
               WHERE
               ST_Intersects(
                   aaaa.shape_centroid
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_area_centroid(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_area_centroid(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

--******************************--
----- functions/index_area_artpath.sql 

CREATE OR REPLACE FUNCTION cipsrv_nhdplus_m.index_area_artpath(
    IN  p_geometry             GEOMETRY
   ,IN  p_geometry_areasqkm    NUMERIC
   ,IN  p_known_region         VARCHAR
   ,IN  p_cat_threashold_perc  NUMERIC
   ,IN  p_evt_threashold_perc  NUMERIC
   ,OUT out_return_code        INTEGER
   ,OUT out_status_message     VARCHAR
)
VOLATILE
AS $BODY$
DECLARE
   rec                 RECORD;
   str_known_region    VARCHAR;
   int_srid            INTEGER;
   geom_input          GEOMETRY;
   num_cat_threshold   NUMERIC;
   num_evt_threshold   NUMERIC;

BEGIN

   IF p_cat_threashold_perc IS NULL
   THEN
      num_cat_threshold := 0;
   
   ELSE
      num_cat_threshold := p_cat_threashold_perc / 100;
      
   END IF;
   
   IF p_evt_threashold_perc IS NULL
   THEN
      num_evt_threshold := 0;
   
   ELSE
      num_evt_threshold := p_evt_threashold_perc / 100;
      
   END IF;

   str_known_region := p_known_region;

   rec := cipsrv_nhdplus_m.determine_grid_srid(
       p_geometry       := p_geometry
      ,p_known_region   := p_known_region
   );
   int_srid           := rec.out_srid;
   out_return_code    := rec.out_return_code;
   out_status_message := rec.out_status_message;
   
   IF out_return_code != 0
   THEN
      RETURN;
      
   END IF;
   
   str_known_region := int_srid::VARCHAR;
      
   IF str_known_region = '5070'
   THEN
      geom_input := ST_Transform(p_geometry,5070);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_5070 aaaa
               WHERE
                   aaaa.fcode = 55800
               AND ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '3338'
   THEN
      geom_input := ST_Transform(p_geometry,3338);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_3338 aaaa
               WHERE
                   aaaa.fcode = 55800
               AND ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSIF str_known_region = '26904'
   THEN
      geom_input := ST_Transform(p_geometry,26904);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_26904 aaaa
               WHERE
                   aaaa.fcode = 55800
               AND ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32161'
   THEN
      geom_input := ST_Transform(p_geometry,32161);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32161 aaaa
               WHERE
                   aaaa.fcode = 55800
               AND ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32655'
   THEN
      geom_input := ST_Transform(p_geometry,32655);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32655 aaaa
               WHERE
                   aaaa.fcode = 55800
               AND ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
      
   ELSIF str_known_region = '32702'
   THEN
      geom_input := ST_Transform(p_geometry,32702);
      
      INSERT INTO tmp_cip(
         nhdplusid
      ) 
      SELECT 
      a.nhdplusid
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.overlapmeasure
         ,ROUND(aa.overlapmeasure / aa.areasqkm,8) AS nhdpercentage
         ,CASE
          WHEN p_geometry_areasqkm = 0
          THEN
            0
          ELSE
            ROUND(aa.overlapmeasure / p_geometry_areasqkm,8)
          END AS eventpercentage
         FROM (
            SELECT
             aaa.nhdplusid
            ,aaa.areasqkm
            ,ROUND(ST_Area(aaa.geom_overlap)::NUMERIC / 1000000,8) AS overlapmeasure
            FROM (
               SELECT
                aaaa.nhdplusid
               ,aaaa.areasqkm
               ,ST_CollectionExtract(
                  ST_Intersection(
                      aaaa.shape
                     ,geom_input
                   )
                  ,3
                ) AS geom_overlap
               FROM
               cipsrv_nhdplus_m.catchment_32702 aaaa
               WHERE
                   aaaa.fcode = 55800
               AND ST_Intersects(
                   aaaa.shape
                  ,geom_input
               )
            ) aaa
            WHERE 
            aaa.geom_overlap IS NOT NULL
         ) aa 
      ) a
      WHERE
         (num_cat_threshold IS NULL OR a.nhdpercentage   >= num_cat_threshold)
      OR (num_evt_threshold IS NULL OR a.eventpercentage >= num_evt_threshold)
      ON CONFLICT DO NOTHING;
   
   ELSE
      out_return_code    := -10;
      out_status_message := 'err ' || str_known_region;
      
   END IF;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_nhdplus_m.index_area_artpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_nhdplus_m.index_area_artpath(
    GEOMETRY
   ,NUMERIC
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
) TO PUBLIC;

