CREATE OR REPLACE VIEW cipsrv_nhdplus_m.catchment_3338_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_m.catchment_3338 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_m.catchment_3338_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_3338_state TO public;
