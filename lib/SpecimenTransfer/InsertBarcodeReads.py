
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertBarcodeReads(DBInsert):
	def __init__(self, globalconfig, readsgetter):
		DBInsert.__init__(self, globalconfig)
		self.readsgetter = readsgetter
		self.insertBarcodeReads()
	
	
	def insertBarcodeReads(self):
		dataslice = self.readsgetter.getNextDataPage()
		
		self.setTempTableQueries()
		self.setDataQuery()
		
		while dataslice is not None:
			self.setPlaceholderString(dataslice)
			self.setValuesFromLists(dataslice)
			
			self.setTempTableFillQuery()
			
			self.cur.execute(self.createtemptable)
			self.con.commit()
			
			self.cur.execute(self.addspecimenkey)
			self.con.commit()
			
			self.cur.execute(self.addunitkey)
			self.con.commit()
			
			self.cur.execute(self.addsourcekey)
			self.con.commit()
			
			self.cur.execute(self.addanalysisidkey)
			self.con.commit()
			
			self.cur.execute(self.addanalysisnumberkey)
			self.con.commit()
			
			#logger.info('filling temporary table with barcode reads')
			self.cur.execute(self.filltemptable, self.values)
			self.con.commit()
			
			#logger.info('fill table with barcode reads')
			self.cur.execute(self.dataquery)
			self.con.commit()
			
			self.cur.execute(self.droptemptable)
			self.con.commit()
			
			dataslice = self.readsgetter.getNextDataPage()
	
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_Barcode_Reads_Temp`
			SELECT s.`DatasourceID`, s.`CollectionSpecimenID`, s.`IdentificationUnitID`, b.`analysis_id`, b.`analysis_number`, br.`read_id`, br.`term`, br.`field_id`
			FROM `{0}_Specimen` s, `{0}_Barcode` b, `{0}_Barcode_Reads` br LIMIT 0
		;""".format(self.db_suffix)
		
		self.addspecimenkey = """ALTER TABLE `{0}_Barcode_Reads_Temp` ADD KEY bct_specimen_idx (`CollectionSpecimenID`)
		;""".format(self.db_suffix)
		
		self.addunitkey = """ALTER TABLE `{0}_Barcode_Reads_Temp` ADD KEY bct_unit_idx (`IdentificationUnitID`)
		;""".format(self.db_suffix)
		
		self.addsourcekey = """ALTER TABLE `{0}_Barcode_Reads_Temp` ADD KEY bct_source_idx (`DatasourceID`)
		;""".format(self.db_suffix)
		
		self.addanalysisidkey = """ALTER TABLE `{0}_Barcode_Reads_Temp` ADD KEY bct_analysisid_idx (`analysis_id`)
		;""".format(self.db_suffix)
		
		self.addanalysisnumberkey = """ALTER TABLE `{0}_Barcode_Reads_Temp` ADD KEY bct_analysisnumber_idx (`analysis_number`)
		;""".format(self.db_suffix)
		
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_Barcode_Reads_Temp`
		;""".format(self.db_suffix)
		
	
	def setTempTableFillQuery(self):
		self.filltemptable = """
		INSERT INTO `{0}_Barcode_Reads_Temp` 
		(`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `analysis_id`, `analysis_number`, `read_id`, `term`, `field_id`)
		VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
		
	
	def setDataQuery(self):
		self.dataquery = """INSERT INTO `{0}_Barcode_Reads` (`id`, `barcode_id`, `read_id`, `term`, `field_id`)
			SELECT NULL, b.`id`, bt.`read_id`, bt.`term`, bt.`field_id`
			FROM `{0}_Barcode` b
			INNER JOIN `{0}_Specimen` s ON (b.`specimen_id` = s.`id`)
			INNER JOIN `{0}_Barcode_Reads_Temp` bt 
			ON (
				(bt.`DatasourceID` = s.`DatasourceID`)
				AND (bt.`CollectionSpecimenID` = s.`CollectionSpecimenID`)
				AND (bt.`IdentificationUnitID` = s.`IdentificationUnitID`)
				AND (bt.`analysis_id` = b.`analysis_id`) 
				AND (bt.`analysis_number` = b.`analysis_number`))
		;""".format(self.db_suffix)
	
	

