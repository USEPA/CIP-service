# Watershed Boundary Datasets

The NRCS and USGS Watershed Boundary Dataset (WBD) project began in 2009 as an undertaking to delineate a seamless national coverage of hydrologic units.  

> "Hydrologic units in the WBD provide a standardized base for water-resources organizations to locate, store, retrieve, and exchange hydrologic data; to index and inventory hydrologic data and information; to catalog water-data acquisition activities; and to use in a variety of other applications."
>
> <cite>[USGS](https://pubs.usgs.gov/tm/11/a3/pdf/tm11-a3_5ed.pdf)</cite>

The WBD HUC12 watershed system has become a standard unit of identification and classfication in the years since.  However, important to emphasize is that WBD units are consensus-based and underwent substantial revision between 2009 and 2025.  NHDPlus and other hydrological frameworks tied to WBD are perhaps better described as tied to specific WBD versions or vintages.

CIP-service provides multiple version of NHDPlus (MR and HR resolutions) with catchments each tied to multiple versions of WBD HUC12s.

* **NP21** - WBD associated with NHDPlus v2.1</br>
Each version of NHDPlus is constructed over several years utilizing the then most current version of WBD available.  As NHDPlus vector processing units (VPUs) are constructed generally in numeric order, VPU 01 uses a much earlier vintage of WBD than VPU 18 or 23.  Thus the WBD dataset provided in the NHDPLus v2.1 final downloads (circa 2016) represent the WBD in a variety of vintages from 2012 to 2015. The release notes for each VPU needs to be consulted and even then only a general range of the true vintage is possible.  Furthe complicating the matter, Alaska was not part of NHDPlus v2.1.  To fill this gap in coverage, the EPA Office of Water inserted WBD for Alaska from 2016.  Whilst a bit of a patchwork, this **NP21** WBD has been successfully used by many EPA programs for now over a decade.  Of the three datasets listed here, it is by far the one most likely to be encountered.<br/>
EPA's Office of Water provides an online service of the **NP21** WBD here: https://watersgeo.epa.gov/arcgis/rest/services/Support/hydrologicunits_np21/MapServer

* **NPHR** WBD associated with NHDPlus HR</br>
As with v2.1, the HR releases of NHDPlus also contain a running vintage of WBD data.  Harvesting dates by the many HR VPUs is provided in the accompanying xml metadata files - ranging from 2016 to 2020.  Notably VPUs in region 01 lack any offshore WBD coverage indicating the very early vintage of the extract.  For users seeking a modern WBD the **NPHR** can appear very similar to the **NP21** version.  CIP-service provides the **NPHR** WBD for completeness but does not recommend its usage.<br/>
EPA's Office of Water provides an online service of the **NPHR** WBD here: https://watersgeo.epa.gov/arcgis/rest/services/Support/hydrologicunits_nphr/MapServer

* **F3** Final 20250107 WBD</br>
The very last revision to the Watershed Boundary Dataset was [published by USGS](https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/WBD/National/GDB/) on January 7th, 2025.  No further changes are anticipated this decade.  Water programs at EPA and other federal and state agencies are expected to migrate to this WBD over the next coming years.  This WBD solves the coverage gaps and ever-shifting vintages of the NP21 and NPHR datasets.<br/>
EPA's Office of Water provides an online service of the **F3** WBD here: https://watersgeo.epa.gov/arcgis/rest/services/Support/hydrologicunits_f3/MapServer

