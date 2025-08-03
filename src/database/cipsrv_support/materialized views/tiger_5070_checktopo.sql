DROP MATERIALIZED VIEW IF EXISTS cipsrv_support.tiger_5070_checktopo CASCADE;

CREATE MATERIALIZED VIEW cipsrv_support.tiger_5070_checktopo(
    objectid
   ,itemcode
   ,shape
)
AS
SELECT
 ROW_NUMBER() OVER () AS objectid
,a.itemcode
,a.shape
FROM (
   SELECT
    aa.stusps AS itemcode
   ,aa.shape
   FROM
   cipsrv_support.tiger_fedstatewaters_5070 aa
   UNION ALL
   SELECT
    bb.itemcode 
   ,bb.shape
   FROM
   cipsrv_support.outerwaters_5070 bb
) a;

ALTER TABLE cipsrv_support.tiger_5070_checktopo OWNER TO cipsrv;
GRANT SELECT ON cipsrv_support.tiger_5070_checktopo TO public;

CREATE UNIQUE INDEX IF NOT EXISTS tiger_5070_checktopo_pk
ON cipsrv_support.tiger_5070_checktopo(itemcode);

CREATE UNIQUE INDEX IF NOT EXISTS tiger_5070_checktopo_u01
ON cipsrv_support.tiger_5070_checktopo(objectid);

CREATE INDEX IF NOT EXISTS tiger_5070_checktopo_spx
ON cipsrv_support.tiger_5070_checktopo USING gist(shape);

ANALYZE cipsrv_support.tiger_5070_checktopo;

--VACUUM FREEZE ANALYZE cipsrv_support.tiger_5070_checktopo;

