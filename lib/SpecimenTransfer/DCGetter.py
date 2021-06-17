
import pudb

from datetime import datetime
import time

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_query = logging.getLogger('query')


from ..MSSQLConnector import MSSQLConnector


class DCGetter():
	def __init__(self, data_source_name, globalconfig):
		
		self.config = globalconfig
		self.data_source_name = data_source_name
		self.datasource = self.config.getDataSourceConfig(self.data_source_name)
		self.dc_db = MSSQLConnector(connectionstring=self.datasource.source_database)
		self.con = self.dc_db.getConnection()
		self.cur = self.dc_db.getCursor()
		
		self.update_date_span = []
		self.respect_withhold = self.datasource.respect_withhold
		self.project_id_string = self.datasource.project_id_string
		self.analysis_id_combined_string = self.datasource.analysis_id_combined_string
		self.analysis_id_method_string = self.datasource.analysis_id_method_string
		self.analysis_id_string = self.datasource.analysis_id_string
		currenttime = self.getCurrentTime()
		self.update_date_span = [None, currenttime]
		
		self.institute_id = self.datasource.institute_id
		
		
		self.pagesize = 10000
		self.page = 1
		self.rownum = 0
		self.maxpage = 0
		self.pagingstarted = False
		
		# self.temptable will be overwritten
		self.temptable = "#DCGetterTempTable"
		
		self.data = []
		#self.pagesize = 100
	
	
	# todo: implement a paging thing for the getter side too
	# currently there is one on the import side
	#def setPageSize(self, pagesize):
	#	if int(pagesize) > 0:
	#		self.pagesize = int(pagesize)
	
	
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
	
	
	
	
