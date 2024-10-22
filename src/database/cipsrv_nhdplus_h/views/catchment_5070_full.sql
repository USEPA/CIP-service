CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_5070_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_5070 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_5070_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_5070_full TO public;
