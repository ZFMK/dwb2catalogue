
import pudb

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')



from .DCGetter import DCGetter


class DCSpecimenDataGetter(DCGetter):
	def __init__(self, dc_db, data_source_name, globalconfig, table, datasourceid):
		DCGetter.__init__(self, dc_db, data_source_name, globalconfig, datasourceid)
		self.table = table
		
		# will be overwritten in create[..]TempTable methods, just a dummy here to ensure that a name is available that produces a temporary table
		self.temptable = "#temptable"
		
		self.pagesize = 10000
		self.setTempTable()
		self.setMaxPage()
		
		#logger.info("[%s] Get Specimen entries...\n\tNo. specimen found: %i" % (self.data_source_name, self.record_num))
	

	def setTempTable(self):
		if self.table == "specimendata":
			self.createSpecimenDataTempTable()
		elif self.table == "biology":
			self.createBiologyTempTable()
		elif self.table == "event":
			self.createEventTempTable()
		elif self.table == "agent":
			self.createAgentTempTable()
		elif self.table == "part":
			self.createPartTempTable()
		elif self.table == "collection":
			self.createCollectionTempTable()
		else:
			raise ValueError('no table with name {0}'.format(self.table))
	
	
	def getPageQuery(self):
		query = """
		SELECT *
		FROM [{0}] WHERE [rownumber] BETWEEN ? AND ?""".format(self.temptable)
		return query
	
	
	# overwrite the method from DCGetter
	def getDataPage(self, page):
		if page % 10 == 0:
			logger.info("DCGetter get data page {0}".format(page))
		startrow = ((page-1)*self.pagesize)+1
		lastrow = startrow + self.pagesize-1
		
		parameters = [startrow, lastrow]
		query = self.getPageQuery()
		
		self.cur.execute(query, parameters)
		datarows = self.cur.fetchall()
		
		specimendata = []
		if len(datarows) > 0:
			specimendata = self.convertData(datarows)
		return specimendata
		
	
	
	def convertData(self, datarows):
		specimendata = []
		datadict = {}
		
		for row in datarows:
			datadict = {t[0]: value for (t, value) in zip(self.cur.description, row) if t[0] not in ('rownumber', 'DatasourceID', 'CollectionSpecimenID', 'IdentificationUnitID', 'RowGUID')}
			for fieldid, value in datadict.items():
				specimendata.append([row[1], row[2], row[3], value, fieldid])
		
		return specimendata
	
	
	def createBiologyTempTable(self):
		self.temptable = "#BiologyTempTable"
		
		if self.respect_withhold is True:
			withholdclause = """
			WHERE (
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		
		query = """
			SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				iu.CollectionSpecimenID,
				iu.IdentificationUnitID,
				iu.NumberOfUnits AS [15],
				iu.LifeStage AS [12],
				iu.Gender AS [17]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				 -- INNER JOIN CollectionProject p ON p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
			{3}
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, iu.IdentificationUnitID,
				iu.NumberOfUnits, iu.LifeStage,
				iu.Gender""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nIdentification Unit:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
		
	
	def createSpecimenDataTempTable(self):
		self.temptable = "#SpecimenDataTempTable"
		
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
		 		s.CollectionSpecimenID,
				iu.IdentificationUnitID,
				s.AccessionNumber AS [1],
				CAST(s.DepositorsName AS varchar(255)) AS [9],
				CAST(left(convert(varchar, s.LogCreatedWhen, 120),25) as VARCHAR(59)) AS [2],
				CAST(i.ResponsibleName AS varchar(255)) AS [97],
				CAST(left(convert(varchar, i.IdentificationDate, 120),25) as VARCHAR(59)) AS [98],
				i.TypeStatus AS [96],
				s.LabelTitle AS [99]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				INNER JOIN IdentificationSequenceMax im on (im.IdentificationUnitID=iu.IdentificationUnitID
					and iu.CollectionSpecimenID=im.CollectionSpecimenID)
				INNER JOIN Identification i on (im.IdentificationUnitID=i.IdentificationUnitID
					and im.CollectionSpecimenID=i.CollectionSpecimenID
					and im.IdentificationSequenceMax=i.IdentificationSequence)
				INNER JOIN CollectionSpecimen s on s.CollectionSpecimenID=iu.CollectionSpecimenID
				 -- INNER JOIN CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
			WHERE i.TaxonomicName IS NOT NULL {3}
			GROUP BY s.CollectionSpecimenID, iu.IdentificationUnitID, i.TypeStatus, s.LabelTitle,
				s.AccessionNumber, s.DepositorsName, s.LogCreatedWhen,
				i.ResponsibleName,i.IdentificationDate,
				s.RowGUID""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nSpecimen data:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
	
	
	def createEventTempTable(self):
		self.temptable = "#EventTempTable"
		
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
				case when e.CollectionDate is null then
					case when e.CollectionDay IS NOT NULL and e.CollectionMonth IS NOT NULL and e.CollectionYear IS NOT NULL THEN
						CAST(e.CollectionDay as varchar)+'.'+ CAST(e.CollectionMonth as varchar)+'.'+ CAST(e.CollectionYear as varchar)
					when e.CollectionYear IS NOT NULL THEN
						CAST(e.CollectionYear as varchar)
					else
						case when e.CollectionDateSupplement IS NOT NULL THEN e.CollectionDateSupplement else '' end
					end
				else
					CAST(CONVERT(VARCHAR(10), e.CollectionDate, 104) AS VARCHAR)
				end AS [4],
				CAST(e.CollectorsEventNumber AS VARCHAR(50)) AS [10],
				CAST(e.CountryCache AS VARCHAR(50)) AS [7],
				CAST(e.LocalityDescription AS VARCHAR(max)) as [13],
				CAST(e.HabitatDescription AS VARCHAR(max)) as [11],
				CAST(e.CollectingMethod AS VARCHAR(max)) as [3],
				CAST(l2.Location1 AS VARCHAR(255)) as [14],
				CAST(l3.Location1 AS VARCHAR(255)) as [26]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				LEFT JOIN CollectionSpecimen c on c.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionEvent e on c.CollectionEventID=e.CollectionEventID
				LEFT JOIN CollectionEventLocalisation l2 on (l2.CollectionEventID=e.CollectionEventID and l2.LocalisationSystemID=7)
				LEFT JOIN CollectionEventLocalisation l3 on (l3.CollectionEventID=e.CollectionEventID and l3.LocalisationSystemID=13)
				 -- LEFT JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
			{3}
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, e.CollectionDate, e.CollectionDay,
				e.CollectionMonth, e.CollectionYear, e.CollectionDateSupplement, e.CollectorsEventNumber,
				e.CountryCache, e.LocalityDescription, e.HabitatDescription, e.CollectingMethod,
				l2.Location1, l3.Location1""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nEvent:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
	
	
	def createAgentTempTable(self):
		self.temptable = "#AgentTempTable"
		
		#if self.respect_withhold is True:
		# keep agent withold fixed?
		withholdclause = """
		WHERE (
		(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
		AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
		AND (ca.[DataWithholdingReason] IS NULL OR ca.[DataWithholdingReason]='')
		)
		"""
		#else:
		#	withholdclause = ""
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				ca.CollectionSpecimenID,
				iu.IdentificationUnitID,
				CAST(ca.CollectorsName AS VARCHAR(max)) AS [6],
				ca.CollectorsNumber AS [18],
				CAST(ca.Notes AS VARCHAR(max)) AS [5]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				 -- INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionAgent ca on ca.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
			{3}
			GROUP BY ca.CollectionSpecimenID, iu.IdentificationUnitID, ca.CollectorsName,
				ca.CollectorsNumber, ca.Notes, ca.CollectorsSequence
			ORDER BY ca.CollectorsSequence""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nCollection_Agents:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
		
		
	
	
	def createPartTempTable(self):
		self.temptable = "#PartTempTable"
		
		if self.respect_withhold is True:
			withholdclause = """
			AND (
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			AND (cp.[DataWithholdingReason] IS NULL OR cp.[DataWithholdingReason] = '')
			)
			"""
		else:
			withholdclause = ""
		
		query = """SELECT
				IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
				{0} AS DatasourceID,
				iu.CollectionSpecimenID,
				iu.IdentificationUnitID,
				CAST(cp.PreparationMethod AS VARCHAR(max)) AS [16]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				INNER JOIN IdentificationUnitInPart iup on (iup.CollectionSpecimenID=iu.CollectionSpecimenID
					AND iu.IdentificationUnitID=iup.IdentificationUnitID)
				INNER JOIN CollectionSpecimenPart cp on (cp.SpecimenPartID=iup.SpecimenPartID
					AND cp.CollectionSpecimenID=iup.CollectionSpecimenID
					AND cp.DerivedFromSpecimenPartID IS NULL)
				LEFT JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
			WHERE cp.PreparationMethod IS NOT NULL {3}
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID, cp.PreparationMethod
			""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nSpecimen Parts:\n\t%s" % query)
		
		self.cur.execute(query)
		self.con.commit()
		
		return
		
	
	
	
	def createCollectionTempTable(self):
		self.temptable = "#CollectionTempTable"
		
		if self.respect_withhold is True:
			withholdclause = """
			WHERE (
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
				c.CollectionName AS [23]
			INTO [{1}]
			FROM IdentificationUnit iu
				INNER JOIN [{2}] ids_temp
					ON (ids_temp.CollectionSpecimenID = iu.CollectionSpecimenID AND ids_temp.IdentificationUnitID = iu.IdentificationUnitID)
				 -- INNER JOIN CollectionProject p ON p.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN CollectionSpecimen s ON s.CollectionSpecimenID=iu.CollectionSpecimenID
				INNER JOIN IdentificationUnitInPart iup on (iup.CollectionSpecimenID=iu.CollectionSpecimenID
					AND iu.IdentificationUnitID=iup.IdentificationUnitID)
				INNER JOIN CollectionSpecimenPart csp on (csp.SpecimenPartID=iup.SpecimenPartID
					AND csp.CollectionSpecimenID=iup.CollectionSpecimenID
					AND csp.DerivedFromSpecimenPartID IS NULL)
				INNER JOIN 
				(
					SELECT c2.CollectionName AS CollectionName, 
					c2.CollectionID AS CollectionID_l2,
					c3.CollectionID AS CollectionID_l3,
					c4.CollectionID AS CollectionID_l4
					FROM Collection c1 LEFT JOIN Collection c2 ON(c1.CollectionID = c2.CollectionParentID AND c1.CollectionID != c2.CollectionID)
					LEFT JOIN Collection c3 ON(c2.CollectionID = c3.CollectionParentID AND c2.CollectionID != c3.CollectionID)
					LEFT JOIN Collection c4 ON(c3.CollectionID = c4.CollectionParentID AND c3.CollectionID != c4.CollectionID)
					WHERE c1.CollectionName = 'ZFMK'
				) AS
				c ON (c.CollectionID_l2 = csp.CollectionID OR c.CollectionID_l3 = csp.CollectionID OR c.CollectionID_l4 = csp.CollectionID)
			{3}
			GROUP BY iu.CollectionSpecimenID, iu.IdentificationUnitID,
				c.CollectionName""".format(self.datasourceid, self.temptable, self.transfer_ids_temptable, withholdclause)
		log_query.info("\nCollection:\n\t%s" % query)
		
		
		self.cur.execute(query)
		self.con.commit()
		
		return
	
	
	
