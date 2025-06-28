DO $$DECLARE 
   r record;
   staticgeo GEOMETRY;
   patchgeo  GEOMETRY;
   step1geo  GEOMETRY;
   step2geo  GEOMETRY;
   int_srid  INTEGER := 5070;
   int_count INTEGER;
   
BEGIN
   /* Remove Overlaps - vals are (keep,clip) */
   FOR r IN SELECT * FROM (VALUES  
       (65000300078747,65000300125171)
      ,(65000300105455,65000300125171)
      ,(65000300047806,65000300116951)
      ,(65000300055088,65000300110550)
      ,(65000300001720,65000300110550)
      ,(65000300081895,65000300110550)
      ,(65000300014249,65000300138431)
      ,(65000300110540,65000300138431)
      ,(65000300054185,65000300138431)
      ,(65000300040925,65000300138431)
      ,(65000300067593,65000300138431)
      ,(65000300094404,65000300138431)
      ,(65000300014185,65000300138431)
      ,(65000300013333,65000300111746)
      ,(65000300027565,65000300111746)
      ,(65000300054026,65000300111746)
      ,(65000300040764,65000300126030)
      ,(65000300053919,65000300126030)
      ,(65000300070029,65000300096613)
      ,(65000300083463,65000300096613)
      ,(65000300095572,65000300082356)
      ,(65000200070291,65000300080247)
      ,(65000300040128,65000300080247)
      ,(65000300026922,65000300080247)
      ,(65000300026922,65000300080247)
      ,(65000300013390,65000300080247)
      ,(65000300066819,65000300080247)
      ,(65000300000008,65000300080247)
      ,(65000300000234,65000300000005)
      ,(65000400001708,23002800120555)
      ,(65000400006958,23002800080358)
      ,(23002800180790,23002800040435)
      ,(23002800020402,23002800040435)
      ,(23002800198916,23002800040435)
      ,(23002800163390,23002800040435)
      ,(55000800278518,55000800127717)
      ,(55000800127719,55000800127717)
      ,(55000800127718,55000800127717)
   ) AS t(keep,clip)
   LOOP
      IF r.keep = r.clip
      OR r.keep IS NULL
      OR r.clip IS NULL
      THEN
         RAISE EXCEPTION 'QA problem % %',r.keep,r.clip;
      
      END IF;
      
      SELECT 
      cipsrv_nhdplus_h.snap_to_common_grid(ST_TRANSFORM(b.shape,int_srid),int_srid::VARCHAR,0.001)
      INTO
      step1geo
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.shape
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment aa
         UNION ALL
         SELECT
          bb.nhdplusid
         ,bb.shape
         FROM
         cipsrv_epageofab_h.grid0catchment bb
      ) b 
      WHERE 
      b.nhdplusid = r.keep;
      
      IF step1geo IS NULL
      OR ST_ISEMPTY(step1geo)
      OR ST_SRID(step1geo) = 0
      THEN
         RAISE EXCEPTION '% %',r.keep,r.clip;
         
      END IF;
      
      UPDATE cipsrv_nhdplus_h.nhdpluscatchment a
      SET shape = ST_TRANSFORM(ST_DIFFERENCE(
          cipsrv_nhdplus_h.snap_to_common_grid(ST_TRANSFORM(a.shape,int_srid),int_srid::VARCHAR,0.001)
         ,step1geo
      ),4269)
      WHERE 
      a.nhdplusid = r.clip;
      
      GET DIAGNOSTICS int_count = ROW_COUNT;

      IF int_count != 1
      THEN
         RAISE EXCEPTION 'error finding % in nhdpluscatchment',r.keep;

      END IF;         
      
      UPDATE cipsrv_nhdplus_h.nhdpluscatchment a
      SET areasqkm = ST_AREA(ST_TRANSFORM(a.shape,int_srid))::NUMERIC / 1000000
      WHERE 
      a.nhdplusid = r.clip;
        
   END LOOP;
   
   /* Remove Gaps I - vals are (expand,static,patch,size)*/
   FOR r IN SELECT * FROM (VALUES
       (65000300125171,ARRAY[65000300025456,65000300078930],ST_POINT(-90.90431106,49.07273633,4269),150)
      ,(65000300116951,ARRAY[65000300101237],ST_POINT(-92.26115192,49.03191262,4269),35)
      ,(65000300110550,ARRAY[65000300055089,65000300055089],ST_POINT(-94.10149185,49.71796727,4269),800)
      ,(65000300138431,ARRAY[65000300067593,65000300040925],ST_POINT(-94.56138383,49.69309876,4269),80)
      ,(65000300138431,ARRAY[65000300040897,65000300040925],ST_POINT(-94.56719492,49.68322295,4269),400)
      ,(65000300111746,ARRAY[65000300110699],ST_POINT(-94.63875551,49.62138844,4269),400)
   ) AS t(expand,static,patch,size)
   LOOP
      IF r.expand = ANY(r.static)
      THEN
         RAISE EXCEPTION 'QA failure for % with same value in static list',r.expand;
      
      END IF;   
   
      BEGIN
         patchgeo := ST_BUFFER(
             ST_TRANSFORM(r.patch,int_srid)
            ,r.size
            ,'quad_segs=2'
         );
         
      EXCEPTION
         WHEN OTHERS THEN
            RAISE WARNING '% % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size;
            RAISE;
      
      END;
      
      SELECT
      ST_UNION(
         cipsrv_nhdplus_h.snap_to_common_grid(ST_TRANSFORM(a.shape,int_srid),int_srid::VARCHAR,0.001)
      )
      INTO staticgeo
      FROM (
         SELECT
          aa.nhdplusid
         ,aa.shape
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment aa
         UNION ALL
         SELECT
          bb.nhdplusid
         ,bb.shape
         FROM
         cipsrv_epageofab_h.grid0catchment bb
      ) a
      WHERE 
      a.nhdplusid = ANY(r.static);
      
      IF staticgeo IS NULL
      OR ST_ISEMPTY(staticgeo)
      OR ST_SRID(staticgeo) = 0
      THEN
         RAISE EXCEPTION '% % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size;
         
      END IF;
      
      IF ST_DISTANCE(
          patchgeo
         ,staticgeo
      ) > 50
      THEN
         RAISE EXCEPTION 'QA distance check for % %',r.expand,r.static;
      
      END IF;
      
      BEGIN
         SELECT
         ST_UNION(
             cipsrv_nhdplus_h.snap_to_common_grid(ST_TRANSFORM(a.shape,int_srid),int_srid::VARCHAR,0.001)
            ,cipsrv_nhdplus_h.snap_to_common_grid(patchgeo,int_srid::VARCHAR,0.001)
         )
         INTO step1geo
         FROM
         cipsrv_nhdplus_h.nhdpluscatchment a
         WHERE
         a.nhdplusid = r.expand;
      
      EXCEPTION
         WHEN OTHERS THEN
            RAISE WARNING '% % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size;
            RAISE;
      
      END;
      
      IF step1geo IS NULL
      OR ST_ISEMPTY(step1geo)
      OR ST_SRID(step1geo) = 0
      THEN
         RAISE EXCEPTION '% % % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size,ST_ASEWKT(step1geo);
         
      END IF;
      
      BEGIN
         SELECT
         ST_DIFFERENCE(
             step1geo
            ,staticgeo
         )
         INTO step2geo;
      
      EXCEPTION
         WHEN OTHERS THEN
            RAISE WARNING '% % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size;
            RAISE;
      
      END;
      
      IF step2geo IS NULL
      OR ST_ISEMPTY(step2geo)
      OR ST_SRID(step2geo) = 0
      THEN
         RAISE EXCEPTION '% % % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size,ST_ASEWKT(step2geo);
         
      END IF;
      
      BEGIN
         UPDATE cipsrv_nhdplus_h.nhdpluscatchment a
         SET shape = ST_TRANSFORM(step2geo,4269)
         WHERE 
         a.nhdplusid = r.expand;
         
      EXCEPTION
         WHEN OTHERS THEN
            RAISE WARNING '% % % %',r.expand,r.static,ST_ASEWKT(r.patch),r.size;
         RAISE;
         
      END;
         
      UPDATE cipsrv_nhdplus_h.nhdpluscatchment a
      SET areasqkm = ST_AREA(ST_TRANSFORM(a.shape,int_srid))::NUMERIC / 1000000
      WHERE 
      a.nhdplusid = r.expand;
        
   END LOOP;
    
END$$;

