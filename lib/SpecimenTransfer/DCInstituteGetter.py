
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCInstituteGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 1000
		
		self.temptable = "#InstituteTempTable"
		self.createInstituteTempTable()
		self.setMaxPage()
		
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID], [ExternalDatasourceID], [ProjectInstitute], [ProjectName], [InstituteShort], [InstituteName]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createInstituteTempTable(self):
		# create temporary table with identity column from ExternalDataSource
		query = """SELECT DISTINCT
			IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
			{0} AS DatasourceID,
			cs.CollectionSpecimenID,
			iu.IdentificationUnitID,
			ced.ExternalDatasourceID,
			ced.ExternalDatasourceName as ProjectInstitute,
			ced.ExternalDatasourceAuthors as ProjectName,
			ced.ExternalDatasourceInstitution as InstituteShort,
			ced.InternalNotes as InstituteName
		INTO [{1}]
		FROM CollectionExternalDatasource ced
		INNER JOIN CollectionSpecimen cs ON (ced.ExternalDatasourceID = cs.ExternalDatasourceID)
		 -- INNER JOIN CollectionProject p on p.CollectionSpecimenID=cs.CollectionSpecimenID
		INNER JOIN IdentificationUnit iu ON (iu.CollectionSpecimenID = cs.CollectionSpecimenID)
		INNER JOIN [{2}] ids_temp
			ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
		ORDER BY cs.CollectionSpecimenID, iu.IdentificationUnitID
		;""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable)
		
		log_query.info("\nInstitutes:\n\t%s" % query)
		self.cur.execute(query)
		self.con.commit()
		

	
	
	
	
	
	
