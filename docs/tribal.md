# Catchment Tribal Status

Each NHDPlus catchment included with CIP-service provide a tribal status via two fields:

* **istribal** - flag with values "F" for fully tribal, "P" for partially tribal or "N" for not tribal.
* **istribal_areasqkm** - value in sqkm of the tribal lands overlapping the catchment.  Fully tribal catchments will show a value equal to the areasqkm of the catchment whilst partially tribal a lesser value.

Overlap is determined using the [EPA Office of Finance and Administration (OFA) Tribal Boundaries, Areas and Names Resources](https://www.epa.gov/geospatial/guidance-using-tribal-boundaries-areas-and-names-resources) guidance dataset for [American Indian Reservations](https://catalog.data.gov/dataset/epa-tribes-3-of-6-american-indian-reservations9). The layer is available for inspection on the [EPA ArcGIS Online Geoplatform](https://epa.maps.arcgis.com/home/item.html?id=b04b580b3e5e40cc8152b14583814073).

Note tribal status in this context is derived from the [American Indian Reservations layer alone](https://www.epa.gov/geospatial/guidance-using-tribal-boundaries-areas-and-names-resources#:~:text=State%20Office.-,American%20Indian%20Reservations,-According%20to%20USCB). As stated none of the datasets provided by EPA OFA are intended to constitute a determination of legal tribal or Indian country boundaries, nor of any entity’s jurisdictional authority or rights of ownership or entitlement.
