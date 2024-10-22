CREATE OR REPLACE VIEW cipsrv_nhdplus_m.catchment_32161_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_m.catchment_32161 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_m.catchment_32161_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32161_full TO public;
