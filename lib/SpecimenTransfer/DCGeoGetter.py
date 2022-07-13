
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCGeoGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		
		self.pagesize = 10000
		
		self.temptable = "#GeoTempTable"
		self.createGeoTempTable()
		self.setMaxPage()
	
	
	def getPageQuery(self):
		query = """
		SELECT [DatasourceID], [CollectionSpecimenID], [IdentificationUnitID],
		[Latitude], [Longitude]
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	

	def createGeoTempTable(self):
		if self.respect_withhold is True:
			withholdclause = """
			WHERE (
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (c.[DataWithholdingReason] IS NULL OR c.[DataWithholdingReason] = '')
			AND (e.[DataWithholdingReason] IS NULL OR e.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				iu.CollectionSpecimenID,
				iu.IdentificationUnitID,
				l3.Location2 as Latitude,
				l3.Location1 as Longitude -- changed the DC turnover of lat and long here, so do not change in insert queries
				-- , l3.LocationAccuracy as Accuracy -- is not used, change InsertSpecimenQueries.ins_specimen_geo too, when it should be activated
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				LEFT JOIN CollectionSpecimen c on c.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionEvent e ON (e.CollectionEventID = c.CollectionEventID)
				INNER JOIN CollectionEventLocalisation l3 on (l3.CollectionEventID=c.CollectionEventID and l3.LocalisationSystemID=8)
				 -- LEFT JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
			{3}
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, l3.Location2,
				l3.Location1
				-- , l3.LocationAccuracy""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nGeo Coords:\n\t%s" % query)
		
		
		self.cur.execute(query)
		self.con.commit()
		
		
	


