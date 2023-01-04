## Current Indexing Methods:

1. [Point Simple](#point-simple) ([code](../src/database/cipsrv_nhdplus_m/functions/index_point_simple.sql))
2. [Line Simple](#line-sSimple) ([code](../src/database/cipsrv_nhdplus_m/functions/index_line_simple.sql))
3. [Line LevelPath](#line-levelpath) ([code](../src/database/cipsrv_nhdplus_m/functions/index_line_levelpath.sql))
4. [Area Simple](#area-simple) ([code](../src/database/cipsrv_nhdplus_m/functions/index_area_simple.sql))
5. [Area Centroid](#area-centroid) ([code](../src/database/cipsrv_nhdplus_m/functions/index_area_centroid.sql))
6. [Area ArtPath](#area-artpath) ([code](../src/database/cipsrv_nhdplus_m/functions/index_area_artpath.sql))

### Point Simple

Parameters: None

This method allocates catchments to a point or multi-point using straight-forward spatial intersection logic.

Use Cases:
- All point events

### Line Simple

Parameters:
- Line Threshold Percentage

This method allocates catchments to a line or multi-line using straight-forward spatial intersection logic.  Providing a line threshold percentage value 
will eliminate catchments whereby the linear event overlap amount divided by the partner flowline length is less than the threshold.  The threshold is provided as a whole
number between 0 and 100 meaning 30 equals 30% or 0.30 in calculations.

Caveats:
- The Line Threshold Percentage functions to remove very small overlaps but only acts against catchments with a partner flowline.  It has no effect against sinks or ocean catchments whereby any spatial interaction will make an allocation.

### Line LevelPath

Parameters:
- Line Threshold Percentage

This method allocates catchments to a line through a process examining NHD LevelPath information.  When multilines are provided each part is run separately through the process.
The provided line is first spatially intersected against all catchments to determine an initial universe of possible levelpaths.  From the results a primary levelpath 
with its start and stop conditions (nodes) is determined via comparative intersection length.  Any gaps between the start and stop along that levelpath are filled in.  
Next other levelpaths in the universe are examined as to whether they connect to the start and stop nodes and if so, these levelpaths are appended to the main levelpath.  Further 
iterations test all remaining levelpaths as to whether they connect to the growing start and stop nodes.  Providing a line threshold percentage value 
will eliminate catchments whereby the linear event overlap amount divided by the partner flowline length is less than the threshold.  The threshold is provided as a whole
number between 0 and 100 meaning 30 equals 30% or 0.30 in calculations.

Note incoming line events do not necessarily need to respect network flow information.  So thus an input line event might flow down stream A to an intersection where it pours into stream B but the event instead changes direction and runs up stream C. 

Caveats:
- By definition the Line Levelpath method only returns catchments having a partner flowline and where that flowline is part of the NHDPlusFlow network (e.g. having LevelPath information).

### Area Simple

Parameters:
- Area Event Threshold Percentage
- Area Catchment Threshold Percentage

This method allocates catchments to a polygon or multi-polygon using straight-forward spatial intersection logic.  Providing an area event threshold value will eliminate 
catchments whereby the event overlap area divided by the size of the event is less than the threshold.  The area event threshold is provided as a whole
number between 0 and 100.  Providing an area catchment threshold value will eliminate catchments whereby the overlap area divided by the size of the catchment is less than the 
threshold.  The area catchment threshold is provided as a whole number.

### Area Centroid

Parameters:
- Area Event Threshold Percentage
- Area Catchment Threshold Percentage

This method allocates catchments to a polygon or multi-polygon using spatial intersection against the centroid of the catchment.  The centroid is determined using the 
PostGIS (ST_PointOnSurface)[https://postgis.net/docs/ST_PointOnSurface.html] algorithm which provides a reasonable centroid that always occurs within the catchment.  Providing an area event threshold value will eliminate 
catchments whereby the event overlap area divided by the size of the event is less than the threshold.  The area event threshold is provided as a whole
number between 0 and 100.  Providing an area catchment threshold value will eliminate catchments whereby the overlap area divided by the size of the catchment is less than the 
threshold.  The area catchment threshold is provided as a whole number.

### Area ArtPath

Parameters:
- Area Event Threshold Percentage
- Area Catchment Threshold Percentage

This method allocates catchments to polygon or multi-polygon using spatial intersection against a catchment universe limited to catchments having a partner flowline where that
flowline has an NHD FCode of 55800 (Artificial Path).   Providing an area event threshold value will eliminate 
catchments whereby the event overlap area divided by the size of the event is less than the threshold.  The area event threshold is provided as a whole
number between 0 and 100.  Providing an area catchment threshold value will eliminate catchments whereby the overlap area divided by the size of the catchment is less than the 
threshold.  The area catchment threshold is provided as a whole number.

Caveats:
- By definition this method will only select catchments having a partner flowline and that flowline being an artificial path.  It will never select ocean catchments or sinks.
