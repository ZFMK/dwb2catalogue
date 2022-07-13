
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCCollectionProjectGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 10000
		
		self.temptable = "#CollectionProjectTempTable"
		self.createCollectionProjectTempTable()
		self.setMaxPage()
		
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [ProjectID]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createCollectionProjectTempTable(self):
		
		'''
		# not needed because restriction is set during creation of self.transfer_ids_temptable
		if self.respect_withhold is True:
			withholdclause = """
			WHERE (
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		'''
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				iu.CollectionSpecimenID,
				iu.IdentificationUnitID,
				p.ProjectID
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, p.ProjectID
			ORDER BY iu.CollectionSpecimenID,
				iu.IdentificationUnitID
			;""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable)
		log_query.info("\nCollection_Projects:\n\t%s" % query)
		
		
		self.cur.execute(query)
		self.con.commit()
		
		
		
	
	
	
	
	
	
