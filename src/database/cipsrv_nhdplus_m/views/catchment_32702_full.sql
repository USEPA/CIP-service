CREATE OR REPLACE VIEW cipsrv_nhdplus_m.catchment_32702_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_m.catchment_32702 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_m.catchment_32702_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32702_full TO public;
