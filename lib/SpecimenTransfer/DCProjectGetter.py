
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCProjectGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 10000
		
		self.temptable = "#ProjectTempTable"
		self.createProjectTempTable()
		self.setMaxPage()
		
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [ProjectID], [Project], [ProjectURI]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createProjectTempTable(self):
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				pp.ProjectID,
				pp.Project,
				pp.ProjectURI
			INTO [{1}]
			FROM ProjectProxy pp
				INNER JOIN CollectionProject p on pp.ProjectID=p.ProjectID
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = p.CollectionSpecimenID)
			GROUP BY DatasourceID, pp.ProjectID, pp.Project, pp.ProjectURI
			ORDER BY pp.ProjectID
			;""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable)
		log_query.info("\nProjects:\n\t%s" % query)
		
		#log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		
	
	
	
	
	
	
