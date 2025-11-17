## EPA Office of Water Linked Data

The CIP-service project builds upon a long lineage of efforts indexing and analyzing water based events upon a common hydrologic framework.  CIP-service provides logic to index, persist, analyze and export these events.  The data model used for this purpose is termed the OWLD model and directly builds from the older Reach Address Database and NHD Event models.  Earlier models generally did not persist the incoming source datasets and did not support catchment indexing.  The following model is meant to both expand upon and encompass earlier models.

![owld model diagram](owld_model.drawio.png)

click on image to expand for closer inspection

Tables in blue store source information, tables in red concern catchment indexing, whilst tables in green support reach indexing.  Datasets may populate all or a portion of these sections.  Traditionally Esri software has been used to persist and distribute EPA water quality information.  The use of single guid keys to tie together the model supports the limited single field joins available in the Esri stack.  

### \<prog\>_control

The **control** table contains keyword pairs providing metadata and context to the dataset useful to clients and analysis tools wokring with the data.  Common keywords would include:

* NAME - providing a full descriptive name for the dataset
* DESCRIPTION - providing a longer description of the dataset
* URL - providing a link to the dataset program
* EVENTTYPE - providing updfront the eventtype domain value used by the dataset
* VINTAGE - providing a general date stamp of the dataset
* RESOLUTION[] - multiple entries providing the NHDPlus resolutions contained in the dataset
* PRECISION[] - multiple entries showing the indexing methods used to create the dataset
* XWALK_NHDPLUS_VERSION - providing the XWALK_HUC12_VERSION used by the dataset

|     | column name                 | datatype     |  len | nullable | description             |
|----:|:----------------------------|:-------------|-----:|:--------:|:------------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key      |
|   2 | keyword                     | varchar      |      |        N | keyword in keyword pair |
|   3 | value_str                   | varchar      |      |        Y | value when textual      |
|   4 | value_num                   | numeric      |      |        Y | value when numeric      |
|   5 | value_date                  | date         |      |        Y | value when date         |
|   6 | globalid                    | varchar      |   40 |        N | vendor guid key         |

### \<prog\>_sfid

The **sfid** table serves as the foundation of the model.  Each row represents a unit of work for the program.  Traditionally this has been keyed via cominations of source_originator, source_featureid, source_series, start_date and end_date as needed.  The addition of source_featureid2 and source_subdivision were added to support even more nuanced entity systems.  The counts in the tables may be used by clients to more efficiently interrogate the data model.

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | source_originator           | varchar      |  130 |        N | key value used to represent the origin of data, for example a state government or federal agency. |
|   3 | source_featureid            | varchar      |  100 |        N | key value for the source entity |
|   4 | source_featureid2           | varchar      |  100 |        Y | optional secondary key value for the source entity |
|   5 | source_series               | varchar      |  100 |        Y | optional secondary key value for the source entity, often a grouping or batching key used by the program |
|   6 | source_subdivision          | varchar      |  100 |        Y | optional secondary key value for the source entity |
|   7 | source_joinkey              | varchar      |   40 |        N | singular guid key for the record, used to join tables in systems without multi-column joins |
|   8 | start_date                  | timestamp&nbsp;tz |      |        Y | optional secondary key value used for temporal bounding |
|   9 | end_date                    | timestamp&nbsp;tz |      |        Y | optional secondary key value used for temporal bounding |
|  10 | sfiddetailurl               | varchar      |  255 |        Y | reference URL to source program entity  |
|  11 | load_id                     | varchar      |  255 |        Y | identifier for user or system adding the particular entity record |
|  12 | load_date                   | timestamp&nbsp;tz |      |        Y | date the entity was added to the dataset |
|  13 | src_event_count             | integer      |      |        Y | optional summary count of component source records of the entity |
|  14 | src_point_count             | integer      |      |        Y | optional summary count of component source point records of the entity |
|  15 | src_line_count              | integer      |      |        Y | optional summary count of component source line records of the entity |
|  16 | src_area_count              | integer      |      |        Y | optional summary count of component source area records of the entity |
|  17 | cat_mr_count                | integer      |      |        Y | optional summary count of medium resolution NHDPlus CIP catchment records of the entity |
|  18 | cat_hr_count                | integer      |      |        Y | optional summary count of high resolution NHDPlus CIP catchment records of the entity |
|  19 | xwalk_huc12_np21_count      | integer      |      |        Y | optional summary count of NP21 version XWALK HUC12 components |
|  20 | rad_mr_event_count          | integer      |      |        Y | optional summary count of medium resolution NHDPlus reached events |         |
|  21 | rad_hr_event_count          | integer      |      |        Y | optional summary count of high resolution NHDPlus reached events |     
|  22 | rad_mr_point_count          | integer      |      |        Y | optional summary count of medium resolution NHDPlus reached point events |     
|  23 | rad_hr_point_count          | integer      |      |        Y | optional summary count of high resolution NHDPlus reached point events |     
|  24 | rad_mr_line_count           | integer      |      |        Y | optional summary count of medium resolution NHDPlus reached line events |     
|  25 | rad_hr_line_count           | integer      |      |        Y | optional summary count of high resolution NHDPlus reached line events |     
|  26 | rad_mr_area_count           | integer      |      |        Y | optional summary count of medium resolution NHDPlus reached area events |     
|  27 | rad_hr_area_count           | integer      |      |        Y | optional summary count of high resolution NHDPlus reached area events |     
|  28 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  29 | shape                       | geometry     |      |        Y | optional bounding box polygon meant to provide a way to quickly zoom to the region of the entity. If not required or not applicable (such as with point entities), just leave empty. |

### \<prog\>_src_p

Each entity may be comprised of zero or more source elements.  Source events are partitioned into three tables to support the Esri requirement of one geometry type per table.  Geometry in the **src_p** table is limited to point and multipoint types.  

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permid_joinkey              | varchar      |   40 |        N | singular guid key for the source element |
|   3 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|   4 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|   5 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|   6 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|   7 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|   8 | source_joinkey              | varchar      |   40 |        N | source joinkey value of the parent entity |
|   9 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  10 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  11 | featuredetailurl            | varchar      |  255 |        Y | reference URL specific to the source element.  Depending on availability, this may be the same as the sfiddetailurl of the parent entity. |
|  12 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  13 | shape                       | geometry     |      |        Y | geometry of the source element |

### \<prog\>_src_l

Geometry in the **src_l** table is limited to line and multilinestring types. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permid_joinkey              | varchar      |   40 |        N | singular guid key for the source element |
|   3 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|   4 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|   5 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|   6 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|   7 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|   8 | source_joinkey              | varchar      |   40 |        N | source joinkey value of the parent entity |
|   9 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  10 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  11 | featuredetailurl            | varchar      |  255 |        Y | reference URL specific to the source element.  Depending on availability, this may be the same as the sfiddetailurl of the parent entity. |
|  12 | lengthkm                    | numeric      |      |        Y | total length of the geometry element in kilometers |
|  13 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  14 | shape                       | geometry     |      |        Y | geometry of the source element |

### \<prog\>_src_a

Geometry in the **src_a** table is limited to polygon and multipolygon types. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permid_joinkey              | varchar      |   40 |        N | singular guid key for the source element |
|   3 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|   4 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|   5 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|   6 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|   7 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|   8 | source_joinkey              | varchar      |   40 |        N | source joinkey value of the parent entity |
|   9 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  10 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  11 | featuredetailurl            | varchar      |  255 |        Y | reference URL specific to the source element.  Depending on availability, this may be the same as the sfiddetailurl of the parent entity. |
|  12 | areasqkm                    | numeric      |      |        Y | total area of the geometry element in square kilometers |
|  13 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  14 | shape                       | geometry     |      |        Y | geometry of the source element |

### \<prog\>_src2cip

The **src2cip** table is meant to persist information specific to the CIP indexing action.

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | src2cip_joinkey             | varchar      |   40 |        N | singular guid key for the element |
|   3 | cip_joinkey                 | varchar      |   40 |        N | joinkey guid value of the indexed CIP catchment results |
|   4 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|   5 | permid_joinkey              | varchar      |   40 |        Y | joinkey guid value of the parent src element |
|   6 | cat_joinkey                 | varchar      |   40 |        N | legacy joinkey value of the indexed catchment, usually the state code + nhdplusid  |
|   7 | catchmentstatecode          | varchar      |    2 |        N | state code when indexing against state-cut catchments |
|   8 | nhdplusid                   | numeric      |      |        N | nhdplusid of the indexed catchment |
|   9 | catchmentresolution         | varchar      |    2 |        Y | nhdplus version use for catchment indexing |
|  10 | cip_action                  | varchar      |  255 |        Y | CIP keyword describing the action taking to index the source element |
|  11 | overlap_measure             | numeric      |      |        Y | numeric QA value describing a percentage or ratio of overlap between source and catchment |
|  12 | cip_method                  | varchar      |  255 |        Y | CIP keyword describing detailed methodolody use in catchment indexing    |
|  13 | cip_parms                   | varchar      |  255 |        Y | specific parameters and thresholds used in catchment indexing |
|  14 | cip_date                    | timestamp&nbsp;tz |      |        Y | vintage of CIP logic used for indexing |
|  15 | cip_version                 | varchar      |  255 |        Y | version of CIP logic used for indexing |
|  16 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_cip

The **cip** table contains the results of CIP indexing.

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | cip_joinkey                 | varchar      |   40 |        N | singular guid key for the element |
|   3 | permid_joinkey              | varchar      |   40 |        Y | joinkey guid value of the parent src element |
|   4 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|   5 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|   6 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|   7 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|   8 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|   9 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|  10 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  11 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  12 | cat_joinkey                 | varchar      |   40 |        N | legacy joinkey value of the indexed catchment, usually the state code + nhdplusid  |
|  13 | catchmentstatecode          | varchar      |    2 |        N | state code when indexing against state-cut catchments |
|  14 | nhdplusid                   | numeric      |      |        N | nhdplusid of the indexed catchment |
|  15 | istribal                    | varchar      |    1 |        Y | flag indicating the catchment indexed is wholly or partially under tribal land |
|  16 | istribal_areasqkm           | numeric      |      |        Y | tribal area of the catchment in square kilometers  |
|  17 | catchmentresolution         | varchar      |    2 |        Y | the NHDPlus version of the catchment layer used for CIP indexing |
|  18 | catchmentareasqkm           | numeric      |      |        Y | the area of the catchment in square kilometers |
|  19 | xwalk_huc12                 | varchar      |   12 |        Y | the crosswalk HUC12 value of the indexed catchment |
|  20 | xwalk_method                | varchar      |   18 |        Y | the method used to craft the crosswalk between catchments and HUC12s |
|  21 | xwalk_huc12_version         | varchar      |   16 |        Y | the HUC12 layer used in the determining the HUC12 value of the catchment |
|  22 | isnavigable                 | varchar      |    1 |        Y | flag indicating the indexed catchment has a flowline suitable for network navigation |
|  23 | hasvaa                      | varchar      |    1 |        Y | flag indicating the indexed catchment is part of the NHDPlus VAA network |
|  24 | issink                      | varchar      |    1 |        Y | flag indicating the indexed catchment is a sink |
|  25 | isheadwater                 | varchar      |    1 |        Y | flag indicating the indexed catchment has a headwater flowline |
|  26 | iscoastal                   | varchar      |    1 |        Y | flag indicating the indexed catchment has a coastal flowline |
|  27 | isocean                     | varchar      |    1 |        Y | flag indicating the indexed catchment is part of the ocean catchment extension to NHDPlus |
|  28 | isalaskan                   | varchar      |    1 |        Y | flag indicating the indexed catchment is part of the Alaska catchment extension to NHDPlus |
|  29 | h3hexagonaddr               | varchar      |   64 |        Y | for catchments in extensions, the value of the level 8 H3 hex address for the catchment |
|  30 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_cip_geo

The optional **cip_geo** table provides catchment geometries for visualization and further analysis.  For programs where this table is not populated, users should be provided clear instructions on where to find the geometries from other sources.  Geometry in the **cip_geo** table is limited to polygon and multipolygon type. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | cat_joinkey                 | varchar      |   40 |        N | legacy joinkey value of the indexed catchment, usually the state code + nhdplusid  |
|   3 | catchmentstatecode          | varchar      |    2 |        N | state code when indexing against state-cut catchments |
|   4 | nhdplusid                   | numeric      |      |        N | nhdplusid of the indexed catchment |
|   5 | catchmentresolution         | varchar      |    2 |        N | the NHDPlus version of the catchment layer used for CIP indexing |
|   6 | catchmentareasqkm           | numeric      |      |        Y | the area of the catchment in square kilometers |
|   7 | xwalk_huc12                 | varchar      |   12 |        Y | the crosswalk HUC12 value of the indexed catchment |
|   8 | xwalk_method                | varchar      |   18 |        Y | the method used to craft the crosswalk between catchments and HUC12s |
|   9 | xwalk_huc12_version         | varchar      |   16 |        Y | the HUC12 layer used in the determining the HUC12 value of the catchment |
|  10 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  11 | shape                       | geometry     |      |        Y | geometry of the catchment     |

### \<prog\>_huc12

The **huc12** table lists all HUC12 values for a given source as determined by catchment indexing.

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|   3 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|   4 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|   5 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|   6 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|   7 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|   8 | permid_joinkey              | varchar      |   40 |        Y | joinkey guid value of the parent src element |
|   9 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  10 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  11 | xwalk_huc12                 | varchar      |   12 |        N | the crosswalk HUC12 value of the indexed catchment |
|  12 | xwalk_catresolution         | varchar      |    2 |        N | the NHDPlus version of the catchments used to determine the HUC12 |
|  13 | xwalk_huc12_version         | varchar      |   16 |        N | the HUC12 layer used in the determining the HUC12 value of the catchment |
|  14 | xwalk_huc12_areasqkm        | numeric      |      |        Y | the area in square kilometers of the crosswalk HUC12 |
|  15 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_huc12_geo

The optional **huc12_geo** table provides crosswalk HUC12 geometries for visualization and further analysis.  Geometry in the **huc12_geo** table is limited to polygon and multipolygon type. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | xwalk_huc12                 | varchar      |   12 |        N | the crosswalk HUC12 value of the indexed catchment |
|   3 | xwalk_catresolution         | varchar      |    2 |        N | the NHDPlus version of the catchments used to determine the HUC12 |
|   4 | xwalk_huc12_version         | varchar      |   16 |        N | the HUC12 layer used in the determining the HUC12 value of the catchment |
|   5 | xwalk_huc12_areasqkm        | numeric      |      |        N | the area in square kilometers of the crosswalk HUC12 |
|   6 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|   7 | shape                       | geometry     |      |        Y | geometry of the crosswalk HUC12 polygon |

### \<prog\>_src2rad

The **src2rad** table is meant to persist information specific to the reach indexing action.

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N | singular guid key for the event |
|   3 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|   4 | permid_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent src element |
|   5 | reach_indexing_action       | varchar      |  255 |        Y | keyword describing the action taking to reach index the source element |
|   6 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_rad_p

Geometry in the **rad_p** table is limited to point type. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N | singular guid key for the event |
|   3 | eventdate                   | timestamp&nbsp;tz |      |        Y | date the event was created |
|   4 | reachcode                   | varchar      |   14 |        Y | reach code on which the event is located |
|   5 | reachsmdate                 | timestamp&nbsp;tz |      |        Y | deprecated reach version date.  these dates no longer exist in newer versions of NHDPlus and the field remains for legacy usage. |
|   6 | reachresolution             | varchar      |    2 |        Y | nhdplus version of the layer used for reach indexing |
|   7 | feature_permanent_identifier| varchar      |   40 |        Y | optional identifier for tying to a specific NHDPlus flowline when applicable.  mostly used in the reach indexing of points to allow quick access to the specific flowline. |
|   8 | featureclassref             | integer      |      |        Y | legacy domain value to indicate the type of feature in the feature_permanent_identifier field |
|   9 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|  10 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|  11 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|  12 | source_datadesc             | varchar      |  100 |        Y | optional textual description of the event.  should not be used for primary keying of events. |
|  13 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|  14 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|  15 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|  16 | permid_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent src element |
|  17 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  18 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  19 | featuredetailurl            | varchar      |  255 |        Y | reference URL specific to the reach indexed event |  
|  20 | measure                     | numeric      |      |        Y | reach measure at which the point event occurs on the reach |
|  21 | eventtype                   | integer      |      |        Y | domain value representing the name or version of the dataset |
|  22 | geogstate                   | varchar      |    2 |        Y | optional legacy column used to indicate the US state into which and event falls or mostly falls  |
|  23 | xwalk_huc12                 | varchar      |   12 |        Y | optional crosswalk HUC12 value of the reach indexed point |
|  24 | xwalk_method                | varchar      |   16 |        Y | the method used to craft the crosswalk between reached points and HUC12s |
|  25 | xwalk_huc12_version         | varchar      |   16 |        Y | the HUC12 layer used in the determining the HUC12 value of the catchment |
|  26 | isnavigable                 | varchar      |    1 |        Y | flag indicating the reached point is upon a flowline suitable for network navigation |
|  27 | hasvaa                      | varchar      |    1 |        Y | flag indicating the reached point is upon a flowline in the VAA NHDPlus network |
|  28 | isheadwater                 | varchar      |    1 |        Y | flag indicating the reached point is upon a flowline which is a headwater |
|  29 | iscoastal                   | varchar      |    1 |        Y | flag indicating the reached point is upon a flowline which is a coastline |
|  30 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  31 | shape                       | geometry     |      |        Y | geometry of the reach indexed event |

### \<prog\>_rad_l

Geometry in the **rad_p** table is limited to linestring type. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N | singular guid key for the event |
|   3 | eventdate                   | timestamp&nbsp;tz |      |        Y | date the event was created |
|   4 | reachcode                   | varchar      |   14 |        Y | reach code on which the event is located  |
|   5 | reachsmdate                 | timestamp&nbsp;tz |      |        Y | deprecated reach version date.  these dates no longer exist in newer versions of NHDPlus and the field remains for legacy usage. |
|   6 | reachresolution             | varchar      |   16 |        Y | nhdplus version of the layer used for reach indexing |
|   7 | feature_permanent_identifier| varchar      |   40 |        Y | optional identifier for tying to a specific NHDPlus flowline when applicable. |
|   8 | featureclassref             | integer      |      |        Y | legacy domain value to indicate the type of feature in the feature_permanent_identifier field |
|   9 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|  10 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|  11 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|  12 | source_datadesc             | varchar      |  100 |        Y | optional textual description of the event.  should not be used for primary keying of events. |
|  13 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|  14 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|  15 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|  16 | permid_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent src element |
|  17 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  18 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  19 | featuredetailurl            | varchar      |  255 |        Y | reference URL specific to the reach indexed event |  
|  20 | fmeasure                    | numeric      |      |        Y | starting measure of the event on the reach |
|  21 | tmeasure                    | numeric      |      |        Y | ending measure of the event on the reach |
|  22 | eventtype                   | integer      |      |        Y | domain value representing the name or version of the dataset |
|  23 | eventoffset                 | numeric      |      |        Y | very optional legacy column indicating an offset for event from a reached flowline.  positive values are to the left of the travel of the flowline with negative values to the right |
|  24 | event_lengthkm              | numeric      |      |        Y | length of the reached event in kilometers|
|  25 | geogstate                   | varchar      |    2 |        Y | optional legacy column used to indicate the US state into which and event falls or mostly falls |
|  26 | xwalk_huc12                 | varchar      |   12 |        Y | optional crosswalk HUC12 value of the reach indexed flowline.  May not be fully accurate if flowlines cross multiple HUC12 boundaries |
|  27 | xwalk_method                | varchar      |   16 |        Y | the method used to craft the crosswalk between reached flowlines and HUC12s |
|  28 | xwalk_huc12_version         | varchar      |   16 |        Y | the HUC12 layer used in the determining the HUC12 value of the catchment |
|  29 | isnavigable                 | varchar      |    1 |        Y | flag indicating the reached line is upon a flowline suitable for network navigation |
|  30 | hasvaa                      | varchar      |    1 |        Y | flag indicating the reached line is upon a flowline in the VAA NHDPlus network |
|  31 | isheadwater                 | varchar      |    1 |        Y | flag indicating the reached point is upon a flowline which is a headwater |
|  32 | iscoastal                   | varchar      |    1 |        Y | flag indicating the reached point is upon a flowline which is a coastline |
|  33 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  34 | shape                       | geometry     |      |        Y | geometry of the reach indexed event |

### \<prog\>_rad_a

Geometry in the **rad_a** table is limited to polygon and multipolygon type. 

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N | singular guid key for the event |
|   3 | eventdate                   | timestamp&nbsp;tz |      |        Y | date the event was created |
|   4 | reachcode                   | varchar      |   14 |        Y | reach code on which the event is located  |
|   5 | reachsmdate                 | timestamp&nbsp;tz |      |        Y | deprecated reach version date.  these dates no longer exist in newer versions of NHDPlus and the field remains for legacy usage. |
|   6 | reachresolution             | varchar      |    2 |        Y | nhdplus version of the layer used for reach indexing |
|   7 | feature_permanent_identifier| varchar      |   40 |        Y | optional identifier for tying to a specific NHDPlus flowline when applicable. |
|   8 | featureclassref             | integer      |      |        Y | legacy domain value to indicate the type of feature in the feature_permanent_identifier field |
|   9 | source_originator           | varchar      |  130 |        N | source originator value of the parent entity |
|  10 | source_featureid            | varchar      |  100 |        N | source featureid value of the parent entity |
|  11 | source_featureid2           | varchar      |  100 |        Y | source featureid2 value of the parent entity |
|  12 | source_datadesc             | varchar      |  100 |        Y | optional textual description of the event.  should not be used for primary keying of events. |
|  13 | source_series               | varchar      |  100 |        Y | source series value of the parent entity |
|  14 | source_subdivision          | varchar      |  100 |        Y | source subdivision value of the parent entity |
|  15 | source_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent sfid entity |
|  16 | permid_joinkey              | varchar      |   40 |        N | joinkey guid value of the parent src element |
|  17 | start_date                  | timestamp&nbsp;tz |      |        Y | start date value of the parent entity |
|  18 | end_date                    | timestamp&nbsp;tz |      |        Y | end_date value of the parent entity |
|  19 | featuredetailurl            | varchar      |  255 |        Y | reference URL specific to the reach indexed event |  
|  20 | eventtype                   | integer      |      |        Y | domain value representing the name or version of the dataset |
|  21 | event_areasqkm              | numeric      |      |        Y | area of the reached event in square kilometers |
|  22 | geogstate                   | varchar      |    2 |        Y | optional legacy column used to indicate the US state into which and event falls or mostly falls |
|  23 | xwalk_huc12                 | varchar      |   12 |        Y | optional crosswalk HUC12 value of the reach indexed flowline.  May not be fully accurate if flowlines cross multiple HUC12 boundaries |
|  24 | xwalk_method                | varchar      |   16 |        Y | the method used to craft the crosswalk between reached areas and HUC12s |
|  25 | xwalk_huc12_version         | varchar      |   16 |        Y | the HUC12 layer used in the determining the HUC12 value of the catchment |
|  26 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  27 | shape                       | geometry     |      |        Y | geometry of the reach indexed event |

### \<prog\>_rad_evt2meta

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N | guid key for the event |
|   3 | meta_processid              | varchar      |   40 |        N | guid key for the metadata record |
|   4 | reachresolution             | varchar      |    2 |        Y | nhdplus version of the layer used for reach indexing |
|   5 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_rad_metadata

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | meta_processid              | varchar      |   40 |        N | singular guid key for the metadata record |
|   3 | processdescription          | varchar      | 4000 |        Y | explanation of the process used to create the data, including parameters or tolerances |
|   4 | processdate                 | timestamp&nbsp;tz |      |        Y | date when the data was completed |
|   5 | attributeaccuracyreport     | varchar      | 1800 |        Y | explanation of entities and assignments of values in dataset |
|   6 | logicalconsistencyreport    | varchar      | 1000 |        Y | explanation of fidelity and relationships in dataset, and tests used |
|   7 | completenessreport          | varchar      | 2400 |        Y | information about omissions, criteria, definitions used to derive dataset |
|   8 | horizpositionalaccuracyrepor| varchar      | 3100 |        Y | horizontal coordinate measurements and description of test used |
|   9 | vertpositionalaccuracyreport| varchar      | 3100 |        Y | vertical coordinate measurements and description of test used |
|  10 | metadatastandardname        | varchar      |  100 |        Y | name of the metadata standard used to document the dataset |
|  11 | metadatastandardversion     | varchar      |   40 |        Y | identification of the version of the metadata standard used to document it |
|  12 | metadatadate                | timestamp&nbsp;tz |      |        Y | date the metadata was last created or updated |
|  13 | datasetcredit               | varchar      | 4000 |        Y | recognition of those who contributed to the dataset |
|  14 | contactorganization         | varchar      |  100 |        Y | name or organization to which type of contact applies |
|  15 | addresstype                 | varchar      |   40 |        Y | information provided about the address, i.e. mailing, physical, etc |
|  16 | address                     | varchar      |  100 |        Y | address line for the address |
|  17 | city                        | varchar      |   40 |        Y | city of the address |
|  18 | stateorprovince             | varchar      |   30 |        Y | state or province of the address |
|  19 | postalcode                  | varchar      |   20 |        Y | ZIP or other postal code of the address |
|  20 | contactvoicetelephone       | varchar      |   40 |        Y | telephone number to reach organization or individual |
|  21 | contactinstructions         | varchar      |  120 |        Y | supplemental instructions to contact organization or individual |
|  22 | contactemailaddress         | varchar      |   40 |        Y | email address to reach organization or individual |
|  23 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_rad_srccit

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | title                       | varchar      |  255 |        Y | name by which the dataset is known |
|   3 | source_datasetid            | varchar      |   40 |        N | singular guid key for the source citation record |
|   4 | sourcecitationabbreviation  | varchar      |  255 |        Y | short form alias for source citation |
|   5 | originator                  | varchar      |  400 |        Y | name of organization or individual who developed the dataset |
|   6 | publicationdate             | timestamp&nbsp;tz |      |        Y | date the dataset is published or made available for release |
|   7 | beginningdate               | timestamp&nbsp;tz |      |        Y | first year of the event (if range of dates applies) |
|   8 | endingdate                  | timestamp&nbsp;tz |      |        Y | last year of the event (if range of dates applies) |
|   9 | sourcecontribution          | varchar      |  255 |        Y | brief statement identifying information contributed by source to dataset |
|  10 | sourcescaledenominator      | numeric      |      |        Y | denomination of representative fraction on a map |
|  11 | typeofsourcemedia           | varchar      |  255 |        Y | the medium of the source dataset, i.e. paper, CD-ROM, online, etc |
|  12 | calendardate                | timestamp&nbsp;tz |      |        Y | the year (if single date applies) |
|  13 | sourcecurrentnessreference  | varchar      |  255 |        Y | the basis on which the source time period of content information of the source data set is determined |
|  14 | meta_processid              | varchar      |   40 |        N | guid key for the parent metadata record |
|  15 | globalid                    | varchar      |   40 |        N | vendor guid key    |

