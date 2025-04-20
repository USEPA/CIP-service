DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdwaterbody_32655 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdwaterbody_32655_seq;
CREATE SEQUENCE cipsrv_nhdplus_h.nhdwaterbody_32655_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdwaterbody_32655(
    objectid
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
 NEXTVAL('cipsrv_nhdplus_h.nhdwaterbody_32655_seq') AS objectid
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
    aa.permanent_identifier
   ,aa.fdate
   ,aa.resolution
   ,aa.gnis_id
   ,aa.gnis_name
   ,aa.areasqkm
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
   ,ST_TRANSFORM(aa.shape,32655) AS shape
   FROM   
   cipsrv_nhdplus_h.nhdwaterbody aa
   WHERE
   aa.shape && cipsrv_nhdplus_h.generic_common_mbr('32655')
   AND cipsrv_nhdplus_h.determine_grid_srid_f(aa.shape) = 32655
) a;

ALTER TABLE cipsrv_nhdplus_h.nhdwaterbody_32655 OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.nhdwaterbody_32655 TO public;

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_32655_pk
ON cipsrv_nhdplus_h.nhdwaterbody_32655(nhdplusid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_32655_u01
ON cipsrv_nhdplus_h.nhdwaterbody_32655(objectid);

CREATE UNIQUE INDEX IF NOT EXISTS nhdwaterbody_32655_u02
ON cipsrv_nhdplus_h.nhdwaterbody_32655(globalid);

CREATE INDEX IF NOT EXISTS nhdwaterbody_32655_spx
ON cipsrv_nhdplus_h.nhdwaterbody_32655 USING gist(shape);

ANALYZE cipsrv_nhdplus_h.nhdwaterbody_32655;

--VACUUM FREEZE ANALYZE cipsrv_nhdplus_h.nhdwaterbody_32655;

