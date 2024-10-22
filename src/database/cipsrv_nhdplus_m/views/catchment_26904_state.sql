CREATE OR REPLACE VIEW cipsrv_nhdplus_m.catchment_26904_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_m.catchment_26904 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_m.catchment_26904_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_m.catchment_26904_state TO public;
