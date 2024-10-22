CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_32702_state
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_32702 a
WHERE
a.statesplit IN (0,1);

ALTER TABLE cipsrv_nhdplus_h.catchment_32702_state OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_32702_state TO public;
