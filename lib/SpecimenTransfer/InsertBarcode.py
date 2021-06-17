
import pudb
import re

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')


from .DBInsert import DBInsert


class InsertBarcode(DBInsert):
	def __init__(self, globalconfig, barcodegetter):
		DBInsert.__init__(self, globalconfig)
		self.barcodegetter = barcodegetter
		self.insertBarcodes()
	
	
	def insertBarcodes(self):
		dataslice = self.barcodegetter.getNextDataPage()
		
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
			
			self.cur.execute(self.filltemptable, self.values)
			self.con.commit()
			
			self.cur.execute(self.dataquery)
			self.con.commit()
			
			self.cur.execute(self.droptemptable)
			self.con.commit()
			
			dataslice = self.barcodegetter.getNextDataPage()
	
	
	
	def setTempTableQueries(self):
		self.createtemptable ="""CREATE TEMPORARY TABLE `{0}_Barcode_Temp`
			SELECT s.`DatasourceID`, s.`CollectionSpecimenID`, s.`IdentificationUnitID`, b.`analysis_id`, b.`analysis_number`, 
			b.`region`, b.`sequence`, b.`sequencing_date`, b.`responsible`, b.`withhold`
			FROM `{0}_Specimen` s, `{0}_Barcode` b LIMIT 0
		;""".format(self.db_suffix)
		
		self.addspecimenkey = """ALTER TABLE `{0}_Barcode_Temp` ADD KEY bct_specimen_idx (`CollectionSpecimenID`)
		;""".format(self.db_suffix)
		
		self.addunitkey = """ALTER TABLE `{0}_Barcode_Temp` ADD KEY bct_unit_idx (`IdentificationUnitID`)
		;""".format(self.db_suffix)
		
		
		self.droptemptable = """DROP TEMPORARY Table `{0}_Barcode_Temp`
		;""".format(self.db_suffix)
		
	
	def setTempTableFillQuery(self):
		self.filltemptable = """
		INSERT INTO `{0}_Barcode_Temp` 
		(`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`, `analysis_id`, `analysis_number`, `region`, `sequence`, `sequencing_date`, `responsible`, `withhold`)
		VALUES {1}
		;""".format(self.db_suffix, self.placeholderstring)
		
	
	def setDataQuery(self):
		self.dataquery = """
		INSERT INTO `{0}_Barcode` (`id`, `specimen_id`, `analysis_id`, `analysis_number`, `region`, `sequence`, `sequencing_date`, `responsible`, `withhold`)
			SELECT NULL, s.`id`, bt.`analysis_id`, bt.`analysis_number`, bt.`region`, bt.`sequence`, bt.`sequencing_date`, bt.`responsible`, bt.`withhold`
			FROM `{0}_Barcode_Temp` bt
			INNER JOIN `{0}_Specimen` s ON ((bt.`DatasourceID` = s.`DatasourceID`) AND (bt.`CollectionSpecimenID` = s.`CollectionSpecimenID`) AND (bt.`IdentificationUnitID` = s.`IdentificationUnitID`));
		;""".format(self.db_suffix)
	
	

