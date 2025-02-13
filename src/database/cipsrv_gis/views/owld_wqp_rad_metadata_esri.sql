DROP VIEW IF EXISTS cipsrv_gis.owld_wqp_rad_metadata_esri;

CREATE OR REPLACE VIEW cipsrv_gis.owld_wqp_rad_metadata_esri
AS
SELECT
 a.objectid
,a.meta_processid
,a.processdescription
,a.processdate
,a.attributeaccuracyreport
,a.logicalconsistencyreport
,a.completenessreport
,a.horizpositionalaccuracyreport
,a.vertpositionalaccuracyreport
,a.metadatastandardname
,a.metadatastandardversion
,a.metadatadate
,a.datasetcredit
,a.contactorganization
,a.addresstype
,a.address
,a.city
,a.stateorprovince
,a.postalcode
,a.contactvoicetelephone
,a.contactinstructions
,a.contactemailaddress
,a.globalid
FROM
cipsrv_owld.wqp_rad_metadata a;

ALTER TABLE cipsrv_gis.owld_wqp_rad_metadata_esri OWNER TO cipsrv_gis;
GRANT SELECT ON cipsrv_gis.owld_wqp_rad_metadata_esri TO public;
