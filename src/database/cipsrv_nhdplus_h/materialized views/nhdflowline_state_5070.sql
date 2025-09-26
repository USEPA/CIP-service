DROP MATERIALIZED VIEW IF EXISTS cipsrv_nhdplus_h.nhdflowline_state_5070 CASCADE;

DROP SEQUENCE IF EXISTS cipsrv_nhdplus_h.nhdflowline_state_5070_seq;
CREATE SEQUENCE IF NOT EXISTS cipsrv_nhdplus_h.nhdflowline_state_5070_seq START WITH 1;

CREATE MATERIALIZED VIEW cipsrv_nhdplus_h.nhdflowline_state_5070(
    objectid
   ,flowlinestatecode
   ,permanent_identifier
   ,fdate
   ,resolution
   ,gnis_id
   ,gnis_name
   ,lengthkm
   ,totma
   ,reachcode
   ,flowdir
   ,wbarea_permanent_identifier
   ,ftype
   ,fcode
   ,mainpath
   ,innetwork
   ,visibilityfilter
   ,nhdplusid
   ,vpuid
   ,fmeasure
   ,tmeasure
   ,hasvaa
   ,isnavigable
   ,hydroseq
   ,inside_flag
   ,shape
)
AS
SELECT
 NEXTVAL('cipsrv_nhdplus_h.nhdflowline_state_5070_seq') AS objectid
,a.flowlinestatecode
,a.permanent_identifier
,a.fdate
,a.resolution
,a.gnis_id
,a.gnis_name
,a.lengthkm
,a.totma
,a.reachcode
,a.flowdir
,a.wbarea_permanent_identifier
,a.ftype
,a.fcode
,a.mainpath
,a.innetwork
,a.visibilityfilter
,a.nhdplusid
,a.vpuid
,CASE
 WHEN a.fmeasure > a.tmeasure
 THEN
    a.tmeasure
 ELSE
    a.fmeasure
 END AS fmeasure
,CASE
 WHEN a.fmeasure > a.tmeasure
 THEN
   a.fmeasure
 ELSE
   a.tmeasure
 END AS tmeasure
,a.hasvaa
,a.isnavigable
,a.hydroseq
,a.inside_flag
,a.shape
FROM (
   SELECT
    aa.flowlinestatecode
   ,aa.permanent_identifier
   ,aa.fdate
   ,aa.resolution
   ,aa.gnis_id
   ,aa.gnis_name
   ,CASE
    WHEN aa.inside_flag
    THEN
      aa.lengthkm
    ELSE
      ST_LENGTH(aa.shape)::NUMERIC / 1000
    END AS lengthkm
   ,aa.totma
   ,aa.reachcode
   ,aa.flowdir
   ,aa.wbarea_permanent_identifier
   ,aa.ftype
   ,aa.fcode
   ,aa.mainpath
   ,aa.innetwork
   ,aa.visibilityfilter
   ,aa.nhdplusid
   ,aa.vpuid
   ,CASE
    WHEN aa.inside_flag
    THEN
       ROUND(aa.fmeasure,5)
    ELSE
       ROUND(ST_M(ST_EndPoint(aa.shape))::NUMERIC,5)
    END AS fmeasure
   ,CASE
    WHEN aa.inside_flag
    THEN
      ROUND(aa.tmeasure,5)
    ELSE
      ROUND(ST_M(ST_StartPoint(aa.shape))::NUMERIC,5)
    END AS tmeasure
   ,aa.hasvaa
   ,aa.isnavigable
   ,aa.hydroseq
   ,aa.inside_flag
   ,aa.shape
   FROM (
      SELECT
       aaa.flowlinestatecode
      ,aaa.permanent_identifier
      ,aaa.fdate
      ,aaa.resolution
      ,aaa.gnis_id
      ,aaa.gnis_name
      ,aaa.lengthkm
      ,aaa.totma
      ,aaa.reachcode
      ,aaa.flowdir
      ,aaa.wbarea_permanent_identifier
      ,aaa.ftype
      ,aaa.fcode
      ,aaa.mainpath
      ,aaa.innetwork
      ,aaa.visibilityfilter
      ,aaa.nhdplusid
      ,aaa.vpuid
      ,aaa.fmeasure
      ,aaa.tmeasure
      ,aaa.hasvaa
      ,aaa.isnavigable
      ,aaa.hydroseq
      ,aaa.inside_flag
      ,(ST_DUMP(aaa.shape)).geom AS shape
      FROM (
         SELECT
          bbbb.stusps AS flowlinestatecode
         ,aaaa.permanent_identifier
         ,aaaa.fdate
         ,aaaa.resolution
         ,aaaa.gnis_id
         ,aaaa.gnis_name
         ,aaaa.lengthkm
         ,aaaa.totma
         ,aaaa.reachcode
         ,aaaa.flowdir
         ,aaaa.wbarea_permanent_identifier
         ,aaaa.ftype
         ,aaaa.fcode
         ,aaaa.mainpath
         ,aaaa.innetwork
         ,aaaa.visibilityfilter
         ,aaaa.nhdplusid
         ,aaaa.vpuid
         ,aaaa.fmeasure
         ,aaaa.tmeasure
         ,aaaa.hasvaa
         ,aaaa.isnavigable
         ,aaaa.hydroseq
         ,ST_Within(aaaa.shape,bbbb.shape) AS inside_flag
         ,CASE
          WHEN ST_Within(aaaa.shape,bbbb.shape)
          THEN
            aaaa.shape
          ELSE
            cipsrv_engine.lrs_intersection(
                p_geometry1 := cipsrv_nhdplus_h.snap_to_common_grid(
                   p_geometry      := aaaa.shape
                  ,p_known_region  := '5070'
                  ,p_grid_size     := 0.001
                )
               ,p_geometry2 := cipsrv_nhdplus_h.snap_to_common_grid(
                   p_geometry      := bbbb.shape
                  ,p_known_region  := '5070'
                  ,p_grid_size     := 0.001
                )
            )
          END AS shape
         FROM
         cipsrv_nhdplus_h.nhdflowline_5070 aaaa
         INNER JOIN LATERAL (
            SELECT
             bbbbb.stusps
            ,bbbbb.shape
            FROM 
            cipsrv_support.tiger_fedstatewaters_5070 bbbbb
         ) AS bbbb
         ON
         ST_INTERSECTS(bbbb.shape,aaaa.shape)
      ) aaa
   ) aa
) a
WHERE
    a.shape IS NOT NULL
AND NOT ST_ISEMPTY(a.shape);