CREATE OR REPLACE FUNCTION cipsrv_engine.parse_catchment_filter(
    IN  p_catchment_filter             VARCHAR[]
   ,OUT out_filter_by_state            BOOLEAN
   ,OUT out_state_filters              VARCHAR[]
   ,OUT out_filter_by_tribal           BOOLEAN
   ,OUT out_filter_by_notribal         BOOLEAN
   ,OUT out_return_code                INTEGER
   ,OUT out_status_message             VARCHAR
)
IMMUTABLE
AS $BODY$ 
DECLARE 
   rec           RECORD;
   sdo_geom      GEOMETRY;
   num_lengthkm  NUMERIC;
   num_areasqkm  NUMERIC;
   ary_states    VARCHAR[];
   
BEGIN

   out_return_code        := 0;
   out_filter_by_state    := FALSE;
   out_filter_by_tribal   := FALSE;
   out_filter_by_notribal := FALSE;
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   ary_states := ARRAY[
       'AL','AK','AS','AZ','AR','CA','CO','CT','DE','DC','FL','GA','GU','HI'
      ,'ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MO','MP'
      ,'MS','MT','NE','NV','NH','NJ','NM','NY','NC','ND','MP','OH','OK','OR'
      ,'PA','PR','RI','SC','SD','TN','TX','UT','VT','VI','VA','WA','WV','WI'
      ,'WY'
   ];
   
   IF p_catchment_filter IS NOT NULL
   AND array_length(p_catchment_filter,1) > 0
   THEN
      FOR i IN 1 .. array_length(p_catchment_filter,1)
      LOOP
         IF UPPER(p_catchment_filter[i]) IN ('ALLTRIBES','TRIBAL')
         THEN
            out_filter_by_tribal   := TRUE;
            out_filter_by_notribal := FALSE;
            
         ELSIF UPPER(p_catchment_filter[i]) IN ('NOTRIBES','NOTRIBAL')
         THEN
            out_filter_by_tribal   := FALSE;
            out_filter_by_notribal := TRUE;
            
         ELSIF UPPER(p_catchment_filter[i]) = ANY(ary_states)
         THEN
            out_filter_by_state := TRUE;
            out_state_filters := array_append(out_state_filters,UPPER(p_catchment_filter[i]));
         
         END IF;
         
      END LOOP;
      
   END IF;
   
   RETURN;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION cipsrv_engine.parse_catchment_filter(
    VARCHAR[]
) OWNER TO cipsrv;

GRANT EXECUTE ON FUNCTION cipsrv_engine.parse_catchment_filter(
    VARCHAR[]
) TO PUBLIC;

