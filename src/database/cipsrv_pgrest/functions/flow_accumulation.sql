DO $$DECLARE 
   a VARCHAR;b VARCHAR;
BEGIN
   SELECT p.oid::regproc,pg_get_function_identity_arguments(p.oid)
   INTO a,b FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.oid::regproc::text = 'cipsrv_pgrest.flow_accumulation';
   IF b IS NOT NULL THEN 
   EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s(%s)',a,b);ELSE
   IF a IS NOT NULL THEN EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s',a);END IF;END IF;
END$$;

CREATE OR REPLACE FUNCTION cipsrv_pgrest.flow_accumulation(
   JSONB
) RETURNS JSON
VOLATILE
AS
$BODY$ 
DECLARE
   rec                               RECORD;
   json_input                        JSONB := $1;
   sdo_area_of_interest              GEOMETRY;
   int_default_weight                INTEGER := 1;
   str_known_region                  VARCHAR;
   str_nhdplus_version               VARCHAR;
   rst_temp                          RASTER;
   rst_temp2                         RASTER;
   rst_flow_accumulation             RASTER;
   base64_raster                     TEXT;
   int_raster_srid                   INTEGER;
   int_max_accumulation              INTEGER;
   int_max_accumulation_x            INTEGER;
   int_max_accumulation_y            INTEGER;
   sdo_max_accumulation_pt           GEOMETRY;
   str_image_format                  VARCHAR;
   int_return_code                   INTEGER;
   str_status_message                VARCHAR;
   str_image_mimetype                VARCHAR;
   sdo_raster_bounding_box           GEOMETRY;
   ary_raster_bounding_box           NUMERIC[];
   
BEGIN
   
   int_return_code := 0;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF JSONB_PATH_EXISTS(json_input,'$.area_of_interest')
   AND json_input->'area_of_interest' IS NOT NULL
   AND json_input->>'area_of_interest' != ''
   THEN
      sdo_area_of_interest := cipsrv_engine.json2geometry(json_input->'area_of_interest');
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code',    -10
         ,'status_message', 'area_of_interest is required.'
      );
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.default_weight')
   AND json_input->'default_weight' IS NOT NULL
   AND json_input->>'default_weight' != ''
   THEN
      int_default_weight := cipsrv_engine.json2integer(json_input->'default_weight');
      
   ELSE
      int_default_weight := 1;
   
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.known_region')
   AND json_input->>'known_region' IS NOT NULL
   AND json_input->>'known_region' != ''
   THEN
      str_known_region := json_input->>'known_region';
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.nhdplus_version')
   AND json_input->>'nhdplus_version' IS NOT NULL
   AND json_input->>'nhdplus_version' != ''
   THEN
      str_nhdplus_version := json_input->>'nhdplus_version';
      
   ELSE
      RETURN JSONB_BUILD_OBJECT(
          'return_code', -10
         ,'status_message', 'nhdplus_version required.'
      );
      
   END IF;
   
   IF JSONB_PATH_EXISTS(json_input,'$.image_format')
   AND json_input->>'image_format' IS NOT NULL
   AND json_input->>'image_format' != ''
   THEN
      str_image_format := json_input->>'image_format';
     
   ELSE
      str_image_format := 'PNG';
         
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Call the navigation engine
   ----------------------------------------------------------------------------
   IF str_nhdplus_version = 'nhdplus_m'
   THEN
      rec := cipsrv_nhdplus_m.fdr_flowaccumulation(
          p_area_of_interest     := sdo_area_of_interest
         ,p_default_weight       := int_default_weight
         ,p_known_region         := str_known_region
      );
      rst_flow_accumulation      := rec.out_flow_accumulation;
      int_max_accumulation       := rec.out_max_accumulation;
      int_max_accumulation_x     := rec.out_max_accumulation_x;
      int_max_accumulation_y     := rec.out_max_accumulation_y;
      sdo_max_accumulation_pt    := rec.out_max_accumulation_pt;
      int_raster_srid            := rec.out_raster_srid; 
      int_return_code            := rec.out_return_code;
      str_status_message         := rec.out_status_message;
      
   ELSIF str_nhdplus_version = 'nhdplus_h'
   THEN
      rec := cipsrv_nhdplus_h.fdr_flowaccumulation(
          p_area_of_interest     := sdo_area_of_interest
         ,p_default_weight       := int_default_weight
         ,p_known_region         := str_known_region
      );
      rst_flow_accumulation      := rec.out_flow_accumulation;
      int_max_accumulation       := rec.out_max_accumulation;
      int_max_accumulation_x     := rec.out_max_accumulation_x;
      int_max_accumulation_y     := rec.out_max_accumulation_y;
      sdo_max_accumulation_pt    := rec.out_max_accumulation_pt;
      int_raster_srid            := rec.out_raster_srid; 
      int_return_code            := rec.out_return_code;
      str_status_message         := rec.out_status_message;
   
   ELSE
      RAISE EXCEPTION 'err %',str_nhdplus_version;
      
   END IF;
   
   IF int_return_code != 0
   THEN
      RETURN JSON_BUILD_OBJECT(
          'flow_accumulation'  , NULL
         ,'image_format'       , NULL
         ,'image_bbox'         , NULL
         ,'max_accumulation'   , NULL
         ,'max_accumulation_x' , NULL
         ,'max_accumulation_y' , NULL
         ,'max_accumulation_pt', NULL
         ,'raster_srid'        , NULL
         ,'nhdplus_version'    , str_nhdplus_version
         ,'return_code'        , int_return_code
         ,'status_message'     , str_status_message
      );
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Convert raster to base64 image
   ----------------------------------------------------------------------------
   IF str_image_format = 'PNG'
   THEN
      str_image_mimetype := 'image/png';
      
      rst_temp := public.ST_MAPALGEBRA(
          rst_flow_accumulation
         ,1
         ,'32BF'
         ,'CASE WHEN [rast] > 0 THEN LOG([rast]) ELSE [rast] END'
         ,-1
      );
      
      rst_temp2 := public._ST_MapAlgebra(
          ARRAY[ROW(rst_temp,1)]::public.rastbandarg[]
         ,'public.ST_Max4ma(double precision[][][],integer[][],text[])'::regprocedure
         ,NULL::text
         ,3
         ,3
         ,'FIRST'::text
         ,NULL::raster
         ,NULL::double precision []
         ,NULL::boolean
         ,VARIADIC NULL::text[]
      );

      base64_raster := ENCODE(
          public.ST_ASPNG(
             rast        := public.ST_COLORMAP(
                rast        := rst_temp2
               ,nband       := 1
               ,colormap    := 
'100% 255   0   0 255
  90% 253  31  31 255
  80% 252  63  63 255
  60% 251  94  94 255
  40% 249 126 126 255
   0% 248 157 157 255
    0 247 189 189 255
   nv 246 221 221 192'
               ,method      := 'INTERPOLATE'
             )
          )
         ,'base64'
      );      
      
      sdo_raster_bounding_box := public.ST_TRANSFORM(
          public.ST_ENVELOPE(rst_flow_accumulation)
         ,4326
      );
      
      ary_raster_bounding_box := ARRAY[
          public.ST_XMIN(sdo_raster_bounding_box)
         ,public.ST_YMIN(sdo_raster_bounding_box)
         ,public.ST_XMAX(sdo_raster_bounding_box)
         ,public.ST_YMAX(sdo_raster_bounding_box)
      ];      
      
   ELSIF str_image_format = 'GTIFF'
   THEN
      str_image_mimetype := 'image/tiff; application=geotiff';
      
      base64_raster := ENCODE(
          public.ST_ASGDALRASTER(
             rast        := rst_flow_accumulation
            ,format      := 'GTiff'
            ,srid        := int_raster_srid
          )
         ,'base64'
      );      
      
      sdo_raster_bounding_box := public.ST_TRANSFORM(
          public.ST_ENVELOPE(rst_flow_accumulation)
         ,4326
      );
      
      ary_raster_bounding_box := ARRAY[
          public.ST_XMIN(sdo_raster_bounding_box)
         ,public.ST_YMIN(sdo_raster_bounding_box)
         ,public.ST_XMAX(sdo_raster_bounding_box)
         ,public.ST_YMAX(sdo_raster_bounding_box)
      ];  
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN JSON_BUILD_OBJECT(
       'flow_accumulation'  , base64_raster
      ,'image_format'       , str_image_mimetype
      ,'image_bbox'         , ARRAY_TO_JSON(ary_raster_bounding_box)
      ,'max_accumulation'   , int_max_accumulation
      ,'max_accumulation_x' , int_max_accumulation_x
      ,'max_accumulation_y' , int_max_accumulation_y
      ,'max_accumulation_pt', JSON_BUILD_OBJECT(
           'type'      , 'Feature'
          ,'geometry'  , public.ST_ASGEOJSON(public.ST_TRANSFORM(sdo_max_accumulation_pt,4326))::JSON
          ,'properties', JSON_BUILD_OBJECT(
              'max_accumulation', int_max_accumulation
           )
       )
      ,'raster_srid'        , int_raster_srid
      ,'nhdplus_version'    , str_nhdplus_version
      ,'return_code'        , int_return_code
      ,'status_message'     , str_status_message
   );
      
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_pgrest.flow_accumulation(
   JSONB
) OWNER TO cipsrv_pgrest;

GRANT EXECUTE ON FUNCTION cipsrv_pgrest.flow_accumulation(
   JSONB
) TO PUBLIC;

