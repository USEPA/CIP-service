CREATE OR REPLACE VIEW cipsrv_nhdplus_m.catchment_32655_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_m.catchment_32655 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_m.catchment_32655_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_32655_state TO public;
