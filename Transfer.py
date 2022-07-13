#!/usr/bin/env python
# -*- coding: utf-8 -*-



import pudb
import datetime

import time

from taxamerger.TaxonomiesMerger import TaxonomiesMerger
from CopyResultDB import CopyResultDB
from runSolrIndexer import runSolrIndexer


from GlobalConfig import ConfigReader
from TempDB import TempDB
from lib.logTransfer import TransferLog

from DBConnectors.MSSQLConnector import MSSQLConnector

from lib.Datasources import Datasources

from lib.SpecimenTransfer.DCGetter import DCGetter
from lib.SpecimenTransfer.DCSpecimenGetter import DCSpecimenGetter
from lib.SpecimenTransfer.InsertSpecimen import InsertSpecimen
from lib.SpecimenTransfer.DCProjectGetter import DCProjectGetter
from lib.SpecimenTransfer.InsertProject import InsertProject
from lib.SpecimenTransfer.DCProjectUserGetter import DCProjectUserGetter
from lib.SpecimenTransfer.InsertProjectUser import InsertProjectUser
from lib.SpecimenTransfer.DCCollectionProjectGetter import DCCollectionProjectGetter
from lib.SpecimenTransfer.InsertCollectionProject import InsertCollectionProject
from lib.SpecimenTransfer.DCInstituteGetter import DCInstituteGetter
from lib.SpecimenTransfer.InsertInstitute import InsertInstitute
from lib.SpecimenTransfer.DCSpecimenDataGetter import DCSpecimenDataGetter
from lib.SpecimenTransfer.InsertSpecimenData import InsertSpecimenData
from lib.SpecimenTransfer.DCMediaGetter import DCMediaGetter
from lib.SpecimenTransfer.InsertMedia import InsertMedia
from lib.SpecimenTransfer.DCGeoGetter import DCGeoGetter
from lib.SpecimenTransfer.InsertGeo import InsertGeo
from lib.SpecimenTransfer.DCBarcodeGetter import DCBarcodeGetter
from lib.SpecimenTransfer.InsertBarcode import InsertBarcode
from lib.SpecimenTransfer.DCBarcodeReadsGetter import DCBarcodeReadsGetter
from lib.SpecimenTransfer.InsertBarcodeReads import InsertBarcodeReads

from lib.TaxaMatcher.TaxaMatcher import TaxaMatcher

from lib.CopyMatchedTaxa.CopyMatchedTaxa import CopyMatchedTaxa
from lib.CopyMatchedTaxa.CopyCommonNamesToTargetTable import CopyCommonNamesToTargetTable
from lib.CopyMatchedTaxa.CopyRedListToTargetTable import CopyRedListToTargetTable
from lib.CopyMatchedTaxa.CopyTaxonomySources import CopyTaxonomySources
from lib.CopyMatchedTaxa.CopyTaxonomicRanksEnum import CopyTaxonomicRanksEnum

from lib.CopyMatchedTaxa.NestedSetGenerator import NestedSetGenerator
from lib.CopyMatchedTaxa.FlatTaxaTable import FlatTaxaTable


from lib.Statistics.CalculateStatistics import CalculateStatistics
from lib.Statistics.SpecimenCompleteness import SpecimenCompleteness


import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


import logging, logging.config
if __name__ == "__main__":
	logging.config.fileConfig('config.ini', defaults={'logfilename': 'sync_dwb2portal.log'}, disable_existing_loggers=False)
logger = logging.getLogger('sync_webportal')



if __name__ == "__main__":
	logger.info("\n\n======= S t a r t - {:%Y-%m-%d %H:%M:%S} ======".format(datetime.datetime.now()))
	
	globalconfig = ConfigReader(config)
	
	taxonomiesmerger = TaxonomiesMerger(globalconfig)
	taxonomiesmerger.mergeTaxonomies()
	
	
	tempdb = TempDB(globalconfig)
	transfer_log = TransferLog(globalconfig)
	transfer_log.start()
	
	for datasource in globalconfig.data_sources:
		
		#time.sleep(60)
		
		datasourcename = datasource.data_source_name
		dstable = Datasources(globalconfig)
		dstable.addDatasource(datasourcename)
		datasourceid = dstable.getDatasourceID(datasourcename)
		
		dc_db = MSSQLConnector(connectionstring=datasource.source_database)
		
		dcgetter = DCGetter(dc_db, datasourcename, globalconfig, datasourceid)
		dcgetter.create_transfer_ids_temptable()
		
		logger.info("Transfer Specimens {0}".format(datasourcename.upper()))
		sp_getter = DCSpecimenGetter(dc_db, datasourcename, globalconfig, datasourceid)
		sp_importer = InsertSpecimen(globalconfig, sp_getter)
		
		logger.info("Transfer institutes {0}".format(datasourcename.upper()))
		inst_getter = DCInstituteGetter(dc_db, datasourcename, globalconfig, datasourceid)
		inst_importer = InsertInstitute(globalconfig, inst_getter)
		
		logger.info("Transfer Projects {0}".format(datasourcename.upper()))
		p_getter = DCProjectGetter(dc_db, datasourcename, globalconfig, datasourceid)
		p_importer = InsertProject(globalconfig, p_getter)
		
		logger.info("Transfer ProjectUser {0}".format(datasourcename.upper()))
		pu_getter = DCProjectUserGetter(dc_db, datasourcename, globalconfig, datasourceid)
		pu_importer = InsertProjectUser(globalconfig, pu_getter)
		
		logger.info("Transfer Collection Projects {0}".format(datasourcename.upper()))
		cp_getter = DCCollectionProjectGetter(dc_db, datasourcename, globalconfig, datasourceid)
		cp_importer = InsertCollectionProject(globalconfig, cp_getter)
		
		# import specimen data
		tables = ['specimendata', 'biology', 'event', 'agent', 'part', 'collection']
		for table in tables:
			logger.info("Transfer Specimen Data {0} {1}".format(datasourcename.upper(), table))
			d_getter = DCSpecimenDataGetter(dc_db, datasourcename, globalconfig, table, datasourceid)
			d_importer = InsertSpecimenData(globalconfig, d_getter)
		
		
		logger.info("Transfer Media {0}".format(datasourcename.upper()))
		md_getter = DCMediaGetter(dc_db, datasourcename, globalconfig, datasourceid)
		md_importer = InsertMedia(globalconfig, md_getter)
		
		logger.info("Transfer Coordinates {0}".format(datasourcename.upper()))
		g_getter = DCGeoGetter(dc_db, datasourcename, globalconfig, datasourceid)
		g_importer = InsertGeo(globalconfig, g_getter)
		
		logger.info("Transfer Barcodes {0}".format(datasourcename.upper()))
		b_getter = DCBarcodeGetter(dc_db, datasourcename, globalconfig, datasourceid)
		b_importer = InsertBarcode(globalconfig, b_getter)
		
		logger.info("Transfer Barcode Reads {0}".format(datasourcename.upper()))
		br_getter = DCBarcodeReadsGetter(dc_db, datasourcename, globalconfig, datasourceid)
		br_importer = InsertBarcodeReads(globalconfig, br_getter)
		
		transfer_log.update(datasourcename)
		dc_db.closeConnection()
	
	transfer_log.finished()
	
	taxamatcher = TaxaMatcher(globalconfig)
	
	copytaxa = CopyMatchedTaxa(globalconfig)
	
	copycommonnames = CopyCommonNamesToTargetTable(globalconfig)
	
	copyredlist = CopyRedListToTargetTable(globalconfig)
	
	copytaxonomysources = CopyTaxonomySources(globalconfig)
	
	copytaxranks = CopyTaxonomicRanksEnum(globalconfig)
	
	nestedgen = NestedSetGenerator(globalconfig)
	
	flattaxatable = FlatTaxaTable(globalconfig)
	
	calcstats = CalculateStatistics(globalconfig)
	
	spcompletenes = SpecimenCompleteness(globalconfig)
	
	dbcopy = CopyResultDB(globalconfig)
	dbcopy.copy_db()
	
	runSolrIndexer()



	logger.info("\n======= E N D - {:%Y-%m-%d %H:%M:%S} ======\n\n".format(datetime.datetime.now()))


