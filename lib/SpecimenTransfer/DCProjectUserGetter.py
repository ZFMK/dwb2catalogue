
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCProjectUserGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 1000
		
		self.temptable = "#ProjectUserTempTable"
		self.createProjectUserTempTable()
		self.setMaxPage()
		
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [LoginName], [ProjectID]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createProjectUserTempTable(self):
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				pu.LoginName,
				pu.ProjectID
			INTO [{1}]
			FROM ProjectUser pu
				INNER JOIN CollectionProject p on pu.ProjectID=p.ProjectID
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = p.CollectionSpecimenID)
			GROUP BY DatasourceID, pu.LoginName, pu.ProjectID
			ORDER BY pu.ProjectID
			;""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable)
		log_query.info("\nProjectUsers:\n\t%s" % query)
		
		#log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		
	
	
	
	
	
	
