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
* PRECISION[] - multiple entries showing the indeixng methods used to create the dataset
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

The **sfid** table serves as the foundation of the model.  Each row represents a unit of work for the program.  Traditionally this has been keyed via cominations of source_originator, source_featureid, source_series, start_date and end_date as needed.  The addition of source_featureid2  and source_subdivision were added to support even more nuanced work breakdowns.

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | source_originator           | varchar      |  130 |        N | key value used to represent the origin of data, for example a state government or federal agency. |
|   3 | source_featureid            | varchar      |  100 |        N | key value for the source |
|   4 | source_featureid2           | varchar      |  100 |        Y | optional secondary key value for the source |
|   5 | source_series               | varchar      |  100 |        Y | optional             |
|   6 | source_subdivision          | varchar      |  100 |        Y |             |
|   7 | source_joinkey              | varchar      |   40 |        N |             |
|   8 | start_date                  | timestamp tz |      |        Y |             |
|   9 | end_date                    | timestamp tz |      |        Y |             |
|  10 | sfiddetailurl               | varchar      |  255 |        Y |             |
|  11 | load_id                     | varchar      |  255 |        Y |             |
|  12 | load_date                   | timestamp tz |      |        Y |             |
|  13 | src_event_count             | integer      |      |        Y |             |
|  14 | src_point_count             | integer      |      |        Y |             |
|  15 | src_line_count              | integer      |      |        Y |             |
|  16 | src_area_count              | integer      |      |        Y |             |
|  17 | cat_mr_count                | integer      |      |        Y |             |
|  18 | cat_hr_count                | integer      |      |        Y |             |
|  19 | xwalk_huc12_np21_count      | integer      |      |        Y |             |
|  20 | rad_mr_event_count          | integer      |      |        Y |             |
|  21 | rad_hr_event_count          | integer      |      |        Y |             |
|  22 | rad_mr_point_count          | integer      |      |        Y |             |
|  23 | rad_hr_point_count          | integer      |      |        Y |             |
|  24 | rad_mr_line_count           | integer      |      |        Y |             |
|  25 | rad_hr_line_count           | integer      |      |        Y |             |
|  26 | rad_mr_area_count           | integer      |      |        Y |             |
|  27 | rad_hr_area_count           | integer      |      |        Y |             |
|  28 | globalid                    | varchar      |   40 |        N |             |
|  29 | shape                       | geometry     |      |        Y | vendor guid key    |

### \<prog\>_src_p

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permid_joinkey              | varchar      |   40 |        N |             |
|   3 | source_originator           | varchar      |  130 |        N |             |
|   4 | source_featureid            | varchar      |  100 |        N |             |
|   5 | source_featureid2           | varchar      |  100 |        Y |             |
|   6 | source_series               | varchar      |  100 |        Y |             |
|   7 | source_subdivision          | varchar      |  100 |        Y |             |
|   8 | source_joinkey              | varchar      |   40 |        N |             |
|   9 | start_date                  | timestamp tz |      |        Y |             |
|  10 | end_date                    | timestamp tz |      |        Y |             |
|  11 | featuredetailurl            | varchar      |  255 |        Y |             |
|  12 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  13 | shape                       | geometry     |      |        Y |             |

### \<prog\>_src_l

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permid_joinkey              | varchar      |   40 |        N |             |
|   3 | source_originator           | varchar      |  130 |        N |             |
|   4 | source_featureid            | varchar      |  100 |        N |             |
|   5 | source_featureid2           | varchar      |  100 |        Y |             |
|   6 | source_series               | varchar      |  100 |        Y |             |
|   7 | source_subdivision          | varchar      |  100 |        Y |             |
|   8 | source_joinkey              | varchar      |   40 |        N |             |
|   9 | start_date                  | timestamp tz |      |        Y |             |
|  10 | end_date                    | timestamp tz |      |        Y |             |
|  11 | featuredetailurl            | varchar      |  255 |        Y |             |
|  12 | lengthkm                    | numeric      |      |        Y |             |
|  13 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  14 | shape                       | geometry     |      |        Y |             |

### \<prog\>_src_a

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permid_joinkey              | varchar      |   40 |        N |             |
|   3 | source_originator           | varchar      |  130 |        N |             |
|   4 | source_featureid            | varchar      |  100 |        N |             |
|   5 | source_featureid2           | varchar      |  100 |        Y |             |
|   6 | source_series               | varchar      |  100 |        Y |             |
|   7 | source_subdivision          | varchar      |  100 |        Y |             |
|   8 | source_joinkey              | varchar      |   40 |        N |             |
|   9 | start_date                  | timestamp tz |      |        Y |             |
|  10 | end_date                    | timestamp tz |      |        Y |             |
|  11 | featuredetailurl            | varchar      |  255 |        Y |             |
|  12 | areasqkm                    | numeric      |      |        Y |             |
|  13 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  14 | shape                       | geometry     |      |        Y |             | 

### \<prog\>_src2cip

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | src2cip_joinkey             | varchar      |   40 |        N |             |
|   3 | cip_joinkey                 | varchar      |   40 |        N |             |
|   4 | source_joinkey              | varchar      |   40 |        N |             |
|   5 | permid_joinkey              | varchar      |   40 |        Y |             |
|   6 | cat_joinkey                 | varchar      |   40 |        N |             |
|   7 | catchmentstatecode          | varchar      |    2 |        N |             |
|   8 | nhdplusid                   | numeric      |      |        N |             |
|   9 | catchmentresolution         | varchar      |    2 |        Y |             |
|  10 | cip_action                  | varchar      |  255 |        Y |             |
|  11 | overlap_measure             | numeric      |      |        Y |             |
|  12 | cip_method                  | varchar      |  255 |        Y |             |
|  13 | cip_parms                   | varchar      |  255 |        Y |             |
|  14 | cip_date                    | timestamp tz |      |        Y |             |
|  15 | cip_version                 | varchar      |  255 |        Y |             |
|  16 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_cip

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | cip_joinkey                 | varchar      |   40 |        N |             |
|   3 | permid_joinkey              | varchar      |   40 |        Y |             |
|   4 | source_originator           | varchar      |  130 |        N |             |
|   5 | source_featureid            | varchar      |  100 |        N |             |
|   6 | source_featureid2           | varchar      |  100 |        Y |             |
|   7 | source_series               | varchar      |  100 |        Y |             |
|   8 | source_subdivision          | varchar      |  100 |        Y |             |
|   9 | source_joinkey              | varchar      |   40 |        N |             |
|  10 | start_date                  | timestamp tz |      |        Y |             |
|  11 | end_date                    | timestamp tz |      |        Y |             |
|  12 | cat_joinkey                 | varchar      |   40 |        N |             |
|  13 | catchmentstatecode          | varchar      |    2 |        N |             |
|  14 | nhdplusid                   | numeric      |      |        N |             |
|  15 | istribal                    | varchar      |    1 |        Y |             |
|  16 | istribal_areasqkm           | numeric      |      |        Y |             |
|  17 | catchmentresolution         | varchar      |    2 |        Y |             |
|  18 | catchmentareasqkm           | numeric      |      |        Y |             |
|  19 | xwalk_huc12                 | varchar      |   12 |        Y |             |
|  20 | xwalk_method                | varchar      |   18 |        Y |             |
|  21 | xwalk_huc12_version         | varchar      |   16 |        Y |             |
|  22 | isnavigable                 | varchar      |    1 |        Y |             |
|  23 | hasvaa                      | varchar      |    1 |        Y |             |
|  24 | issink                      | varchar      |    1 |        Y |             |
|  25 | isheadwater                 | varchar      |    1 |        Y |             |
|  26 | iscoastal                   | varchar      |    1 |        Y |             |
|  27 | isocean                     | varchar      |    1 |        Y |             |
|  28 | isalaskan                   | varchar      |    1 |        Y |             |
|  29 | h3hexagonaddr               | varchar      |   64 |        Y |             |
|  30 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_cip_geo

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | cat_joinkey                 | varchar      |   40 |        N |             |
|   3 | catchmentstatecode          | varchar      |    2 |        N |             |
|   4 | nhdplusid                   | numeric      |      |        N |             |
|   5 | catchmentresolution         | varchar      |    2 |        N |             |
|   6 | catchmentareasqkm           | numeric      |      |        Y |             |
|   7 | xwalk_huc12                 | varchar      |   12 |        Y |             |
|   8 | xwalk_method                | varchar      |   18 |        Y |             |
|   9 | xwalk_huc12_version         | varchar      |   16 |        Y |             |
|  10 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  11 | shape                       | geometry     |      |        Y |             |

### \<prog\>_huc12

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | source_originator           | varchar      |  130 |        N |             |
|   3 | source_featureid            | varchar      |  100 |        N |             |
|   4 | source_featureid2           | varchar      |  100 |        Y |             |
|   5 | source_series               | varchar      |  100 |        Y |             |
|   6 | source_subdivision          | varchar      |  100 |        Y |             |
|   7 | source_joinkey              | varchar      |   40 |        N |             |
|   8 | permid_joinkey              | varchar      |   40 |        Y |             |
|   9 | start_date                  | timestamp tz |      |        Y |             |
|  10 | end_date                    | timestamp tz |      |        Y |             |
|  11 | xwalk_huc12                 | varchar      |   12 |        N |             |
|  12 | xwalk_catresolution         | varchar      |    2 |        N |             |
|  13 | xwalk_huc12_version         | varchar      |   16 |        N |             |
|  14 | xwalk_huc12_areasqkm        | numeric      |      |        Y |             |
|  15 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_huc12_geo

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | xwalk_huc12                 | varchar      |   12 |        N |             |
|   3 | xwalk_catresolution         | varchar      |    2 |        N |             |
|   4 | xwalk_huc12_version         | varchar      |   16 |        N |             |
|   5 | xwalk_huc12_areasqkm        | numeric      |      |        N |             |
|   6 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|   7 | shape                       | geometry     |      |        Y |             |

### \<prog\>_src2rad

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N |             |
|   3 | source_joinkey              | varchar      |   40 |        N |             |
|   4 | permid_joinkey              | varchar      |   40 |        N |             |
|   5 | reach_indexing_actio        | varchar      |  255 |        Y |             |
|   6 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|   7 | shape                       | geometry     |      |        Y |             |

### \<prog\>_rad_p

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N |             |
|   3 | eventdate                   | timestamp tz |      |        Y |             |
|   4 | reachcode                   | varchar      |   14 |        Y |             |
|   5 | reachsmdate                 | timestamp tz |      |        Y |             |
|   6 | reachresolution             | varchar      |    2 |        Y |             |
|   7 | feature_permanent_identifier| varchar      |   40 |        Y |             |
|   8 | featureclassref             | integer      |      |        Y |             |
|   9 | source_originator           | varchar      |  130 |        N |             |
|  10 | source_featureid            | varchar      |  100 |        N |             |
|  11 | source_featureid2           | varchar      |  100 |        Y |             |
|  12 | source_datadesc             | varchar      |  100 |        Y |             |
|  13 | source_series               | varchar      |  100 |        Y |             |
|  14 | source_subdivision          | varchar      |  100 |        Y |             |
|  15 | source_joinkey              | varchar      |   40 |        N |             |
|  16 | permid_joinkey              | varchar      |   40 |        N |             |
|  17 | start_date                  | timestamp tz |      |        Y |             |
|  18 | end_date                    | timestamp tz |      |        Y |             |
|  19 | featuredetailurl            | varchar      |  255 |        Y |             |
|  20 | measure                     | numeric      |      |        Y |             |
|  21 | eventtype                   | integer      |      |        Y |             |
|  22 | eventoffset                 | numeric      |      |        Y |             |
|  23 | geogstate                   | varchar      |    2 |        Y |             |
|  24 | xwalk_huc12                 | varchar      |   12 |        Y |             |
|  25 | xwalk_method                | varchar      |   16 |        Y |             |
|  26 | xwalk_huc12_version         | varchar      |   16 |        Y |             |
|  27 | isnavigable                 | varchar      |    1 |        Y |             |
|  28 | hasvaa                      | varchar      |    1 |        Y |             |
|  29 | issink                      | varchar      |    1 |        Y |             |
|  30 | isheadwater                 | varchar      |    1 |        Y |             |
|  31 | iscoastal                   | varchar      |    1 |        Y |             |
|  32 | isocean                     | varchar      |    1 |        Y |             |
|  33 | isalaskan                   | varchar      |    1 |        Y |             |
|  34 | h3hexagonaddr               | varchar      |   64 |        Y |             |
|  35 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  36 | shape                       | geometry     |      |        Y |             |

### \<prog\>_rad_l

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N |             |
|   3 | eventdate                   | timestamp tz |      |        Y |             |
|   4 | reachcode                   | varchar      |   14 |        Y |             |
|   5 | reachsmdate                 | timestamp tz |      |        Y |             |
|   6 | reachresolution             | varchar      |   16 |        Y |             |
|   7 | feature_permanent_identifier| varchar      |   40 |        Y |             |
|   8 | featureclassref             | integer      |      |        Y |             |
|   9 | source_originator           | varchar      |  130 |        N |             |
|  10 | source_featureid            | varchar      |  100 |        N |             |
|  11 | source_featureid2           | varchar      |  100 |        Y |             |
|  12 | source_datadesc             | varchar      |  100 |        Y |             |
|  13 | source_series               | varchar      |  100 |        Y |             |
|  14 | source_subdivision          | varchar      |  100 |        Y |             |
|  15 | source_joinkey              | varchar      |   40 |        N |             |
|  16 | permid_joinkey              | varchar      |   40 |        N |             |
|  17 | start_date                  | timestamp tz |      |        Y |             |
|  18 | end_date                    | timestamp tz |      |        Y |             |
|  19 | featuredetailurl            | varchar      |  255 |        Y |             |
|  20 | fmeasure                    | numeric      |      |        Y |             |
|  21 | tmeasure                    | numeric      |      |        Y |             |
|  22 | eventtype                   | integer      |      |        Y |             |
|  23 | eventoffset                 | numeric      |      |        Y |             |
|  24 | event_lengthkm              | numeric      |      |        Y |             |
|  25 | geogstate                   | varchar      |    2 |        Y |             |
|  26 | xwalk_huc12                 | varchar      |   12 |        Y |             |
|  27 | xwalk_method                | varchar      |   16 |        Y |             |
|  28 | xwalk_huc12_version         | varchar      |   16 |        Y |             |
|  29 | isnavigable                 | varchar      |    1 |        Y |             |
|  30 | hasvaa                      | varchar      |    1 |        Y |             |
|  31 | issink                      | varchar      |    1 |        Y |             |
|  32 | isheadwater                 | varchar      |    1 |        Y |             |
|  33 | iscoastal                   | varchar      |    1 |        Y |             |
|  34 | isocean                     | varchar      |    1 |        Y |             |
|  35 | isalaskan                   | varchar      |    1 |        Y |             |
|  36 | h3hexagonaddr               | varchar      |   64 |        Y |             |
|  37 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  38 | shape                       | geometry     |      |        Y |             |

### \<prog\>_rad_a

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N |             |
|   3 | eventdate                   | timestamp tz |      |        Y |             |
|   4 | reachcode                   | varchar      |   14 |        Y |             |
|   5 | reachsmdate                 | timestamp tz |      |        Y |             |
|   6 | reachresolution             | varchar      |    2 |        Y |             |
|   7 | feature_permanent_identifier| varchar      |   40 |        Y |             |
|   8 | featureclassref             | integer      |      |        Y |             |
|   9 | source_originator           | varchar      |  130 |        N |             |
|  10 | source_featureid            | varchar      |  100 |        N |             |
|  11 | source_featureid2           | varchar      |  100 |        Y |             |
|  12 | source_datadesc             | varchar      |  100 |        Y |             |
|  13 | source_series               | varchar      |  100 |        Y |             |
|  14 | source_subdivision          | varchar      |  100 |        Y |             |
|  15 | source_joinkey              | varchar      |   40 |        N |             |
|  16 | permid_joinkey              | varchar      |   40 |        N |             |
|  17 | start_date                  | timestamp tz |      |        Y |             |
|  18 | end_date                    | timestamp tz |      |        Y |             |
|  19 | featuredetailurl            | varchar      |  255 |        Y |             |
|  20 | eventtype                   | integer      |      |        Y |             |
|  21 | event_areasqkm              | numeric      |      |        Y |             |
|  22 | geogstate                   | varchar      |    2 |        Y |             |
|  23 | xwalk_huc12                 | varchar      |   12 |        Y |             |
|  24 | xwalk_method                | varchar      |   16 |        Y |             |
|  25 | xwalk_huc12_version         | varchar      |   16 |        Y |             |
|  26 | globalid                    | varchar      |   40 |        N | vendor guid key    |
|  27 | shape                       | geometry     |      |        Y |             |

### \<prog\>_rad_evt2meta

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | permanent_identifier        | varchar      |   40 |        N |             |
|   3 | meta_processid              | varchar      |   40 |        N |             |
|   4 | reachresolution             | varchar      |    2 |        Y |             |
|   5 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_rad_metadata

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | meta_processid              | varchar      |   40 |        N |             |
|   3 | processdescription          | varchar      | 4000 |        Y |             |
|   4 | processdate                 | timestamp tz |      |        Y |             |
|   5 | attributeaccuracyreport     | varchar      | 1800 |        Y |             |
|   6 | logicalconsistencyreport    | varchar      | 1000 |        Y |             |
|   7 | completenessreport          | varchar      | 2400 |        Y |             |
|   8 | horizpositionalaccuracyrepor| varchar      | 3100 |        Y |             |
|   9 | vertpositionalaccuracyreport| varchar      | 3100 |        Y |             |
|  10 | metadatastandardname        | varchar      |  100 |        Y |             |
|  11 | metadatastandardversion     | varchar      |   40 |        Y |             |
|  12 | metadatadate                | timestamp tz |      |        Y |             |
|  13 | datasetcredit               | varchar      | 4000 |        Y |             |
|  14 | contactorganization         | varchar      |  100 |        Y |             |
|  15 | addresstype                 | varchar      |   40 |        Y |             |
|  16 | address                     | varchar      |  100 |        Y |             |
|  17 | city                        | varchar      |   40 |        Y |             |
|  18 | stateorprovince             | varchar      |   30 |        Y |             |
|  19 | postalcode                  | varchar      |   20 |        Y |             |
|  20 | contactvoicetelephone       | varchar      |   40 |        Y |             |
|  21 | contactinstructions         | varchar      |  120 |        Y |             |
|  22 | contactemailaddress         | varchar      |   40 |        Y |             |
|  23 | globalid                    | varchar      |   40 |        N | vendor guid key    |

### \<prog\>_rad_srccit

|     | column name                 | datatype     |  len | nullable | description        |
|----:|:----------------------------|:-------------|-----:|:--------:|:-------------------|
|   1 | objectid                    | integer      |      |        N | vendor integer key |
|   2 | title                       | varchar      |  255 |        Y |             |
|   3 | source_datasetid            | varchar      |   40 |        N |             |
|   4 | sourcecitationabbreviation  | varchar      |  255 |        Y |             |
|   5 | originator                  | varchar      |  400 |        Y |             |
|   6 | publicationdate             | timestamp tz |      |        Y |             |
|   7 | beginningdate               | timestamp tz |      |        Y |             |
|   8 | endingdate                  | timestamp tz |      |        Y |             |
|   9 | sourcecontribution          | varchar      |  255 |        Y |             |
|  10 | sourcescaledenominator      | numeric      |      |        Y |             |
|  11 | typeofsourcemedia           | varchar      |  255 |        Y |             |
|  12 | calendardate                | timestamp tz |      |        Y |             |
|  13 | sourcecurrentnessreference  | varchar      |  255 |        Y |             |
|  14 | meta_processid              | varchar      |   40 |        N |             |
|  15 | field                       | integer      |      |        Y |             |
|  16 | globalid                    | varchar      |   40 |        N | vendor guid key    |

