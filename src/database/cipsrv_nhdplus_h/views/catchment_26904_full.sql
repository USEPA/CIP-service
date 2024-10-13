CREATE OR REPLACE VIEW cipsrv_nhdplus_h.catchment_26904_full
AS
SELECT
a.*
FROM
cipsrv_nhdplus_h.catchment_26904 a
WHERE
a.statesplit IN (0,2);

ALTER TABLE cipsrv_nhdplus_h.catchment_26904_full OWNER TO cipsrv;
GRANT SELECT ON cipsrv_nhdplus_h.catchment_26904_full TO public;
