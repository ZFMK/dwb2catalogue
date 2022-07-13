
import pudb

from datetime import datetime
import time

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')


class DCGetter():
	def __init__(self, dc_db, data_source_name, globalconfig, datasourceid):
		
		self.config = globalconfig
		self.data_source_name = data_source_name
		self.datasource = self.config.getDataSourceConfig(self.data_source_name)
		self.datasourceid = datasourceid
		
		self.dc_db = dc_db
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		
		self.update_date_span = []
		self.respect_withhold = self.datasource.respect_withhold
		self.mark_specimen_withhold = self.datasource.mark_specimen_withhold
		self.project_id_string = self.datasource.project_id_string
		self.collection_id_string = self.datasource.collection_id_string
		self.analysis_id_combined_string = self.datasource.analysis_id_combined_string
		self.analysis_id_method_string = self.datasource.analysis_id_method_string
		self.analysis_id_string = self.datasource.analysis_id_string
		currenttime = self.getCurrentTime()
		self.update_date_span = [None, currenttime]
		
		self.pagesize = 10000
		self.page = 1
		self.rownum = 0
		self.maxpage = 0
		self.pagingstarted = False
		
		# self.temptable will be overwritten
		self.temptable = "#DCGetterTempTable"
		self.transfer_ids_temptable = '#TransferIDsTempTable'
		
		self.data = []


	def setDateSpan(self, date_span = [None, ]):
		self.update_date_span = date_span
	
	def getData(self):
		return self.data


	def getCurrentTime(self, formatter="%Y-%m-%d %H:%M:%S"):
		return datetime.now().strftime(formatter)


	# paging
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM [dbo].[{0}]
		;
		""".format(self.temptable)
		return query


	def setRowNum(self):
		query = self.getCountQuery()
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is not None:
			self.rownum = row[0]
		else:
			self.rownum = 0
	
	def getRowNum(self):
		self.setRowNum()
		return self.rownum


	def setMaxPage(self):
		rownum = self.getRowNum()
		if rownum > 0:
			self.maxpage = int(rownum / self.pagesize + 1)
		else:
			self.maxpage = 0


	def initPaging(self):
		self.currentpage = 1
		self.pagingstarted = True


	def getNextDataPage(self):
		if self.pagingstarted == False:
			self.initPaging()
		if self.currentpage <= self.maxpage:
			datarows = self.getDataPage(self.currentpage)
			self.currentpage = self.currentpage + 1
			#return datarows
			if len(datarows) > 0:
				return datarows
			else:
				# this should never happen as self.maxpage should be 0 when there are no results
				#pudb.set_trace()
				# this can happen with names that are not accepted names and not synonyms and therefore are not be found by pageQuery in TNTSynonymsGetter 
				self.pagingstarted = False
				return None
		else:
			self.pagingstarted = False
			return None


	def getDataPage(self, page):
		if page % 10 == 0:
			logger.info("DCGetter get data page {0}".format(page))
		startrow = ((page-1)*self.pagesize)+1
		lastrow = startrow + self.pagesize-1
		
		parameters = [startrow, lastrow]
		query = self.getPageQuery()
		
		self.cur.execute(query, parameters)
		datarows = self.cur.fetchall()
		return datarows


	def create_transfer_ids_temptable(self):
		"""Get ids of all IdentificationUnits that should be transfered"""
		
		whereclauses = []
		if self.update_date_span[0]:
			whereclauses.append("(i.LogUpdatedWhen > CONVERT(DATETIME, '{0}', 102) AND i.LogUpdatedWhen <= CONVERT(DATETIME, '{1}', 102))".format(self.update_date_span[0], self.update_date_span[1]))
		
		if self.collection_id_string is not None:
			whereclauses.append(self.collection_id_string)
		elif self.project_id_string is not None:
			whereclauses.append(self.project_id_string)
		
		if self.respect_withhold is True:
			withholdclause = """(
			(iu.[DataWithholdingReason] IS NULL OR iu.[DataWithholdingReason] = '')
			AND (s.[DataWithholdingReason] IS NULL OR s.[DataWithholdingReason] = '')
			)
			"""
			whereclauses.append(withholdclause)
		else:
			pass
		
		if len(whereclauses) > 0:
			whereclause = 'WHERE ' + ' AND '.join(whereclauses)
		else:
			whereclause = ''
		
		if self.project_id_string is not None:
			project_join_string = "INNER JOIN CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID"
		else:
			project_join_string = ''
		
		query = """SELECT DISTINCT
			IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
			CollectionSpecimenID, IdentificationUnitID,
			{0} AS [DatasourceID]
			INTO [{1}]
			FROM
			(SELECT i_m.CollectionSpecimenID AS CollectionSpecimenID,
				i_m.IdentificationUnitID
				FROM IdentificationSequenceMax i_m
					INNER JOIN Identification i on (i_m.IdentificationSequenceMax=i.IdentificationSequence
						and i_m.IdentificationUnitID=i.IdentificationUnitID
						and i_m.CollectionSpecimenID=i.CollectionSpecimenID)
					INNER JOIN IdentificationUnit iu on (i.IdentificationUnitID=iu.IdentificationUnitID
						and i.CollectionSpecimenID=iu.CollectionSpecimenID)
					{2}
					INNER JOIN CollectionSpecimen s on iu.CollectionSpecimenID = s.CollectionSpecimenID
					{3}
				GROUP BY i_m.CollectionSpecimenID, i_m.IdentificationUnitID
			) AS t
			 -- WHERE LEN(t.NameID)>0 AND t.FamilyCache IS NOT NULL
			ORDER BY CollectionSpecimenID, IdentificationUnitID, DatasourceID
			;""".format(self.datasourceid, self.transfer_ids_temptable, project_join_string, whereclause)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		return self.transfer_ids_temptable



