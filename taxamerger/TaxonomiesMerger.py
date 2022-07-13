#!/usr/bin/env python
# -*- coding: utf-8 -*-



import pudb
import datetime

#from GlobalConfig import ConfigReader
from taxamerger.TaxaDB import TaxaDB

from taxamerger.lib.TaxonomySources import TaxonomySources

from taxamerger.lib.TaxaTransfer.TNTTaxaGetter import TNTTaxaGetter
from taxamerger.lib.TaxaTransfer.TNTSynonymsGetter import TNTSynonymsGetter
from taxamerger.lib.TaxaTransfer.TNTCommonNamesGetter import TNTCommonNamesGetter
from taxamerger.lib.TaxaTransfer.TNTRedListGetter import TNTRedListGetter

from taxamerger.lib.TaxaTransfer.GBIFTaxaGetter import GBIFTaxaGetter
from taxamerger.lib.TaxaTransfer.GBIFSynonymsGetter import GBIFSynonymsGetter

from taxamerger.lib.TaxaTransfer.InsertTaxa import InsertTaxa
from taxamerger.lib.TaxaTransfer.InsertSynonyms import InsertSynonyms
from taxamerger.lib.TaxaTransfer.InsertCommonNames import InsertCommonNames
from taxamerger.lib.TaxaTransfer.InsertRedLists import InsertRedLists

from taxamerger.lib.TaxaTransfer.RankRenamer import RankRenamer
from taxamerger.lib.TaxaTransfer.TaxaClosureTable import TaxaClosureTable

from taxamerger.lib.TaxaTransfer.TaxaMerger import TaxaMerger
from taxamerger.lib.TaxaTransfer.SynonymsMerger import SynonymsMerger
from taxamerger.lib.TaxaTransfer.RedListMerger import RedListMerger

import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


import logging, logging.config

logging.config.fileConfig('config.ini', defaults={'logfilename': 'taxamerger.log'}, disable_existing_loggers=False)
logger = logging.getLogger('sync_webportal')



class TaxonomiesMerger():
	def __init__(self, globalconfig):
		self.config = globalconfig
		
		#self.taxadb_name = 'TaxaMergerDB'
		
		self.tnt_sources = self.config.tnt_sources
		self.tstable = TaxonomySources(self.config)
		
		self.taxadb = TaxaDB(self.config)
		self.taxadb_name = self.taxadb.getDBName()


	def mergeTaxonomies(self):
		logger.info("\n\n======= S t a r t - {:%Y-%m-%d %H:%M:%S} ======".format(datetime.datetime.now()))
		for tnt_source in self.tnt_sources:
			taxsourcename = tnt_source['name']
			self.tstable.addTaxonomySource(taxsourcename)
			taxsourceid = self.tstable.getTaxonomySourceID(taxsourcename)
			for projectid in tnt_source['projectids']:
				logger.info("Transfer Taxa from TNT database {0}, project id {1}".format(tnt_source['dbname'], projectid))
				tnttaxagetter = TNTTaxaGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
				inserttaxa = InsertTaxa(tnttaxagetter, self.config)
				inserttaxa.copyTaxa()
				
				tntsynonymsgetter = TNTSynonymsGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
				insertsynonyms = InsertSynonyms(tntsynonymsgetter, self.config)
				insertsynonyms.copyTaxa()
				
				commonnamesgetter = TNTCommonNamesGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
				insertcommonnames = InsertCommonNames(commonnamesgetter, self.config)
				insertcommonnames.copyCommonNames()
				
				tntredlistgetter = TNTRedListGetter(tnt_source, tnt_source['dbname'], taxsourceid, projectid)
				insertredlist = InsertRedLists(tntredlistgetter, self.config)
				insertredlist.copyRedList()


		if self.config.use_gbif_taxa is True:
			logger.info("Transfer Taxa from GBIF database {0}".format(self.config.gbif_db))
			taxsourcename = self.config.gbif_db
			self.tstable.addTaxonomySource(taxsourcename)
			taxsourceid = self.tstable.getTaxonomySourceID(taxsourcename)
			gbiftaxagetter = GBIFTaxaGetter(self.config.gbif_db, self.config.gbif_taxa_table, taxsourceid, self.config)
			
			inserttaxa = InsertTaxa(gbiftaxagetter, self.config)
			inserttaxa.copyTaxa()
			
			gbifsynonymsgetter = GBIFSynonymsGetter(self.config.gbif_db, self.config.gbif_taxa_table, taxsourceid, self.config)
			insertsynonyms = InsertSynonyms(gbifsynonymsgetter, self.config)
			insertsynonyms.copyTaxa()
		
		#pudb.set_trace()
		rankrenamer = RankRenamer(self.config)
		
		# the closure table is needed to get and compare the path length to taxa in the different taxonomies
		# therefore, it is created here, but not updated after the taxonomies have been merged
		closuretable = TaxaClosureTable(self.config)
		
		# initialize the synonymsmerger before taxa are merged to set the familyCache in synonyms according to closuretable
		synonymsmerger = SynonymsMerger(self.config)
		synonymsmerger.setFamilyCache()
		
		redlistmerger = RedListMerger(self.config)
		redlistmerger.setFamilyCache()
		
		taxamerger = TaxaMerger(self.config)
		
		synonymsmerger.updateAcceptedTaxaIDs()
		redlistmerger.updateTaxaIDs()
		logger.info("\n======= E N D - {:%Y-%m-%d %H:%M:%S} ======\n\n".format(datetime.datetime.now()))
		return




if __name__ == "__main__":
	globalconfig = ConfigReader(config)
	taxamerger = TaxaMerger(globalconfig)
	taxamerger.mergeTaxa()
	
