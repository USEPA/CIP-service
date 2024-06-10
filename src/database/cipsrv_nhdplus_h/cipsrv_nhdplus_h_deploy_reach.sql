--******************************--
----- materialized views/nhdflowline_3338.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_3338 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_3338(
    permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,shape
)
AS
SELECT
 a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,ST_Transform(a.shape,3338) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
WHERE
SUBSTR(a.vpuid,1,2) IN ('19');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_3338 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_3338 TO public;

CREATE UNIQUE INDEX nhdflowline_3338_01u
ON cipsrv_nhdplus_h.nhdflowline_3338(nhdplusid);

CREATE INDEX nhdflowline_3338_02i
ON cipsrv_nhdplus_h.nhdflowline_3338(fcode);

CREATE INDEX nhdflowline_3338_spx
ON cipsrv_nhdplus_h.nhdflowline_3338 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_3338;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_3338;

--******************************--
----- materialized views/nhdflowline_5070.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_5070 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_5070(
    permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,shape
)
AS
SELECT
 a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,ST_Transform(a.shape,5070) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
WHERE
SUBSTR(a.vpuid,1,2) NOT IN ('19','20','21','22');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_5070 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_5070 TO public;

CREATE UNIQUE INDEX nhdflowline_5070_01u
ON cipsrv_nhdplus_h.nhdflowline_5070(nhdplusid);

CREATE INDEX nhdflowline_5070_02i
ON cipsrv_nhdplus_h.nhdflowline_5070(fcode);

CREATE INDEX nhdflowline_5070_spx
ON cipsrv_nhdplus_h.nhdflowline_5070 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_5070;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_5070;


--******************************--
----- materialized views/nhdflowline_26904.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_26904 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_26904(
    permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,shape
)
AS
SELECT
 a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,ST_Transform(a.shape,26904) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
WHERE
SUBSTR(a.vpuid,1,2) IN ('20');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_26904 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_26904 TO public;

CREATE UNIQUE INDEX nhdflowline_26904_01u
ON cipsrv_nhdplus_h.nhdflowline_26904(nhdplusid);

CREATE INDEX nhdflowline_26904_02i
ON cipsrv_nhdplus_h.nhdflowline_26904(fcode);

CREATE INDEX nhdflowline_26904_spx
ON cipsrv_nhdplus_h.nhdflowline_26904 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_26904;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_26904;

--******************************--
----- materialized views/nhdflowline_32161.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32161 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32161(
    permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,shape
)
AS
SELECT
 a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,ST_Transform(a.shape,32161) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
WHERE
SUBSTR(a.vpuid,1,2) IN ('21');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_32161 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_32161 TO public;

CREATE UNIQUE INDEX nhdflowline_32161_01u
ON cipsrv_nhdplus_h.nhdflowline_32161(nhdplusid);

CREATE INDEX nhdflowline_32161_02i
ON cipsrv_nhdplus_h.nhdflowline_32161(fcode);

CREATE INDEX nhdflowline_32161_spx
ON cipsrv_nhdplus_h.nhdflowline_32161 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32161;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32161;

--******************************--
----- materialized views/nhdflowline_32655.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32655 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32655(
    permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,shape
)
AS
SELECT
 a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,ST_Transform(a.shape,32655) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
WHERE
SUBSTR(a.vpuid,1,4) IN ('2201','2202');

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_32655 TO public;

CREATE UNIQUE INDEX nhdflowline_32655_01u
ON cipsrv_nhdplus_h.nhdflowline_32655(nhdplusid);

CREATE INDEX nhdflowline_32655_02i
ON cipsrv_nhdplus_h.nhdflowline_32655(fcode);

CREATE INDEX nhdflowline_32655_spx
ON cipsrv_nhdplus_h.nhdflowline_32655 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32655;

--******************************--
----- materialized views/nhdflowline_32702.sql 

DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_32702 CASCADE;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_32702(
    permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,enabled
   ,fmeasure
   ,tmeasure
   ,shape
)
AS
SELECT
 a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,a.enabled
,a.fmeasure
,a.tmeasure
,ST_Transform(a.shape,32702) AS shape
FROM
cipsrv_nhdplus_h.nhdflowline a
WHERE
SUBSTR(a.vpuid,1,4) = '2203';

ALTER TABLE cipsrv_nhdplus_h.nhdflowline_32702 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdflowline_32702 TO public;

CREATE UNIQUE INDEX nhdflowline_32702_01u
ON cipsrv_nhdplus_h.nhdflowline_32702(nhdplusid);

CREATE INDEX nhdflowline_32702_02i
ON cipsrv_nhdplus_h.nhdflowline_32702(fcode);

CREATE INDEX nhdflowline_32702_spx
ON cipsrv_nhdplus_h.nhdflowline_32702 USING GIST(shape);

ANALYZE cipsrv_nhdplus_h.nhdflowline_32702;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdflowline_32702;
