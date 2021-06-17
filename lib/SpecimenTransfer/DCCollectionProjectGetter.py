
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCCollectionProjectGetter(DCGetter):
	def __init__(self, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, data_source_name, globalconfig)
		self.datasourceid = datasourceid
		
		self.pagesize = 1000
		
		self.temptable = "#ProjectTempTable"
		self.createProjectTempTable()
		self.setMaxPage()
		
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [ProjectID]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createProjectTempTable(self):
		
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
				iu.CollectionSpecimenID,
				iu.IdentificationUnitID,
				p.ProjectID
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
			WHERE {2} {3}
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, p.ProjectID
			ORDER BY iu.CollectionSpecimenID,
				iu.IdentificationUnitID
			;""".format(self.datasourceid, self.temptable, self.project_id_string, withholdclause)
		log_query.info("\nCollection_Projects:\n\t%s" % query)
		
		
		self.cur.execute(query)
		self.con.commit()
		
		
		
	
	
	
	
	
	
