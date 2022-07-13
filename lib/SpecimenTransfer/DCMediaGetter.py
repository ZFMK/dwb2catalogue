
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCMediaGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 1000
		
		self.temptable = "#MediaTempTable"
		self.createMediaTempTable()
		self.setMaxPage()
	
	
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [media_url],
		[media_creator], [license], [media_type], [media_title]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createMediaTempTable(self):
		# csi.DataWithholdingReason was hard coded here, so I let it in
		if self.respect_withhold is True:
			withholdclause = """
			AND (
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				csi.CollectionSpecimenID,
				iu.IdentificationUnitID,
				csi.[URI] AS [media_url],
				ISNULL(csi.[LicenseHolder], 'Zoological Research Museum Alexander Koenig') AS [media_creator],
				ISNULL(csi.[LicenseNotes], ISNULL(csi.LicenseType, 'Creative Commons Attribution-ShareAlike 4.0')) AS [license],
				CASE WHEN csi.ImageType IS NULL THEN 
					CASE WHEN CHARINDEX('Label', Title)>0 THEN 'label' ELSE 'image' END
					ELSE RTRIM(LTRIM(REPLACE(csi.ImageType, 'photograph', 'image'))) END AS [media_type],
				csi.Title as [media_title]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				 -- INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimenImage csi on csi.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
			WHERE (csi.DataWithholdingReason IS NULL OR csi.DataWithholdingReason='') {3}
			GROUP BY csi.CollectionSpecimenID, iu.IdentificationUnitID, csi.[URI], csi.[LicenseHolder], csi.[LicenseNotes], csi.[LicenseType], csi.[ImageType], csi.[Title]
			ORDER BY csi.CollectionSpecimenID""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nCollectionSpecimenImage:\n\t%s" % query)
		
		
		self.cur.execute(query)
		self.con.commit()
		
	
