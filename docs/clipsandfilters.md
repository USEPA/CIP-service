# Clipping and Filtering 

The CIP-service engine provides two complementary ways to limit results by one or more state-equivalent or tribal boundaries.  **Clipping** refers to the reduction of __indexing features__ to the boundaries of a given political entity.  Such reductions will be evident in the results.  Clipping may take place before or after indexing.  **Filtering** refers to the restriction of output catchments to those belonging to or associated with a given political entity.  The concepts are very similar and almost always work in concert to constrain features and catchments to a given area of interest.  However for flexibility there is the option to mix and match these parameters if a user's requirements so demand. 

## Clipping

The CIP-service engine allows indexing features to be clipped by one or more state-equivalent or tribal political boundaries.  At this time the list includes US states and state-equivalents and the list of tribes, native areas and Alaskan homelands recognized by the US Census Bureau.  Tribes may be referenced using EPA, US Census or US Bureau of Indian Affairs codes.  

When utilizing simpler indexing methods the most straightforward approach is to clip features before any indexing takes place - less to index always means better performance.  However, for more complex indexing methods such as line_levelpath or area_artpath the clip may affect results when the entirety of a catchment, reach or levelpath should be considered against the entirety of the feature as part of the indexing logic.  To allow for the greatest flexibility the **geometry_clip_stage** parameter governs whether to perform clipping BEFORE or AFTER indexing.  

In general the recommendation would be:

* Clip BEFORE for point_simple, line_simple and area_simple indexing methods
* Clip AFTER for line_levelpath, area_centroid and area_artpath indexing methods

## Filtering

The CIP-service engine allows catchment indexing results to be filtered down to catchments assigned to one or more state-equivalent entities and/or a __tribal__ condition.  Unlike Clipping which can trim input features to specific tribal geography, filtering only allows the results to be limited to a broader definition of tribal-associated catchment.  
