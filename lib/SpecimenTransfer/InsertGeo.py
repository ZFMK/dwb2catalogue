
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertGeo(DBInsert):
	def __init__(self, globalconfig, geogetter):
		DBInsert.__init__(self, globalconfig)
		self.geogetter = geogetter
		self.insertGeo()
	
	
	def insertGeo(self):
		dataslice = self.geogetter.getNextDataPage()
		
		self.setTempTableQueries()
		self.setDataQuery()
		
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			
			self.setTempTableFillQuery()
			
			self.cur.execute(self.createtemptable)
			self.con.commit()
			
			self.cur.execute(self.filltemptable, self.values)
			self.con.commit()
			
			self.cur.execute(self.dataquery)
			self.con.commit()
			
			self.cur.execute(self.droptemptable)
			self.con.commit()
			
			dataslice = self.geogetter.getNextDataPage()
	
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_Geo_Temp`
			SELECT s.`DatasourceID`, s.`CollectionSpecimenID`, s.`IdentificationUnitID`, g.`lat`, g.`lon` FROM `{0}_Specimen` s, `{0}_Geo` g LIMIT 0
		;""".format(self.db_suffix)
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_Geo_Temp`
		;""".format(self.db_suffix)
		
	
	def setTempTableFillQuery(self):
		self.filltemptable = """
		INSERT INTO `{0}_Geo_Temp` (`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `lat`, `lon`) VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
		
	
	def setDataQuery(self):
		self.dataquery = """INSERT INTO `{0}_Geo` (`specimen_id`, `lat`,`lon`) SELECT s.`id`, gt.`lat`, gt.`lon`
			FROM `{0}_Geo_Temp` gt
			INNER JOIN `{0}_Specimen` s ON ((gt.DatasourceID = s.DatasourceID) AND (gt.`CollectionSpecimenID` = s.`CollectionSpecimenID`) AND (gt.`IdentificationUnitID` = s.`IdentificationUnitID`))
		;""".format(self.db_suffix)
	
	

