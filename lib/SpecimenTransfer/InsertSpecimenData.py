
import pudb
import re
import pymysql  # -- for MySQl Errors

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertSpecimenData(DBInsert):
	def __init__(self, globalconfig, datagetter):
		DBInsert.__init__(self, globalconfig)
		
		self.datagetter = datagetter
		self.insertData()
	
	
	
	def insertData(self):
		dataslice = self.datagetter.getNextDataPage()
		
		self.setTempTableQueries()
		self.setDataQuery()
		self.setData2SpecimenQuery()
		
		
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			
			self.setTempTableFillQuery()
			
			self.cur.execute(self.createtemptable)
			self.con.commit()
			
			self.cur.execute(self.filltemptable, self.values)
			self.con.commit()
			
			self.cur.execute(self.deletefromtemptable)
			self.con.commit()
			
			self.cur.execute(self.dataquery)
			self.con.commit()
			
			self.cur.execute(self.data2specimenquery)
			self.con.commit()
			
			self.cur.execute(self.droptemptable)
			self.con.commit()
			
			dataslice = self.datagetter.getNextDataPage()
	
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_Data_Temp`
		(DatasourceID int(10), `specimen_id` int(10) unsigned, `unit_id` int(10) unsigned, `term` varchar(3000), `field_id` int(10) unsigned -- ,
		 -- KEY (DatasourceID),
		 -- KEY (`specimen_id`),
		 -- KEY (`unit_id`),
		 -- KEY (`term`),
		 -- KEY (`field_id`)
		)
		;""".format(self.db_suffix)
		
		# what was the reason for this?
		self.deletefromtemptable = """DELETE dt FROM `{0}_Data_Temp` dt LEFT JOIN `{0}_Specimen` s ON ((dt.`DatasourceID` = s.DatasourceID) and (dt.`specimen_id` = s.`CollectionSpecimenID`) AND (dt.`unit_id` = s.`IdentificationUnitID`))
		WHERE s.DatasourceID IS NULL AND s.`CollectionSpecimenID` IS NULL AND s.`IdentificationUnitID` IS NULL
		;""".format(self.db_suffix)
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_Data_Temp`
		;""".format(self.db_suffix)
		
	
	def setTempTableFillQuery(self):
		self.filltemptable = """INSERT INTO `{0}_Data_Temp` (`DatasourceID`, `specimen_id`, `unit_id`, `term`, `field_id`) VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
		
	
	def setDataQuery(self):
		self.dataquery = """INSERT INTO `{0}_Data` (`id`, `term`, `field_id`)
			SELECT DISTINCT
			NULL, dt.`term`, dt.`field_id` FROM `{0}_Data_Temp` dt
			LEFT JOIN `{0}_Data` d2 ON ((dt.`term` = d2.`term`) AND (dt.field_id = d2.field_id))
			WHERE (d2.id IS NULL)
		;""".format(self.db_suffix)
	
	def setData2SpecimenQuery(self):
		self.data2specimenquery = """INSERT INTO `{0}_Data2Specimen` (id, data_id, specimen_id)
		SELECT NULL, d.`id`, s.`id` FROM `{0}_Data_Temp` dt
		INNER JOIN `{0}_Data` d ON ((dt.`term` = d.`term`) AND (dt.field_id = d.field_id))
		INNER JOIN `{0}_Specimen` s ON ((s.DatasourceID = dt.DatasourceID) AND (s.`CollectionSpecimenID` = dt.`specimen_id`) AND (s.`IdentificationUnitID` = dt.`unit_id`))
		;""".format(self.db_suffix)
		
		
		
		
		
		
		
		
	
	
	

