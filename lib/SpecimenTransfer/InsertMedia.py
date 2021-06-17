
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertMedia(DBInsert):
	def __init__(self, globalconfig, mediagetter):
		DBInsert.__init__(self, globalconfig)
		self.mediagetter = mediagetter
		self.insertMedia()
	
	
	def insertMedia(self):
		dataslice = self.mediagetter.getNextDataPage()
		
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
			
			dataslice = self.mediagetter.getNextDataPage()
	
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_Media_Temp`
			SELECT s.DatasourceID, s.`CollectionSpecimenID`, s.`IdentificationUnitID`, m.`media_url`, m.`media_creator`, m.`license`, m.`media_type`, m.`media_title`
			FROM `{0}_Specimen` s, `{0}_Media` m LIMIT 0
		;""".format(self.db_suffix)
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_Media_Temp`
		;""".format(self.db_suffix)
		
	
	def setTempTableFillQuery(self):
		self.filltemptable = """INSERT INTO `{0}_Media_Temp` (`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `media_url`, `media_creator`, `license`, `media_type`, `media_title`) VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
		
	
	def setDataQuery(self):
		self.dataquery = """INSERT INTO `{0}_Media` (`id`, `specimen_id`, `media_url`, `media_creator`, `license`, `media_type`, `media_title`)
			SELECT NULL, s.`id`, mt.`media_url`, mt.`media_creator`, mt.`license`, mt.`media_type`, mt.`media_title` FROM `{0}_Media_Temp` mt
			INNER JOIN `{0}_Specimen` s ON ((mt.DatasourceID = s.DatasourceID) AND (mt.`CollectionSpecimenID` = s.`CollectionSpecimenID`) AND (mt.`IdentificationUnitID` = s.`IdentificationUnitID`))
		;""".format(self.db_suffix)
	
	

