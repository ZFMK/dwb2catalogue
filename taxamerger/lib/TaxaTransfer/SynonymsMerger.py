
import logging
import logging.config


logger = logging.getLogger('sync_webportal')
log_queries = logging.getLogger('query')


import pudb


from DBConnectors.MySQLConnector import MySQLConnector


class SynonymsMerger():
	def __init__(self, globalconfig):
		self.config = globalconfig
		dbconfig = self.config.getTaxaMergerDBConfig()
		self.dbcon = MySQLConnector(dbconfig)
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.synonymsmergetable = "TaxaSynonymsMergeTable"
		self.taxamergetable = "TaxaMergeTable"
		self.closuretable = "TaxaMergeRelationTable"

		
		# pudb.set_trace()
		
		self.pagesize = 10000
		self.setMaxPage()
		
	
	
	

	def setMaxPage(self):
		query = """
		SELECT MAX(id) FROM `{0}`
		;
		""".format(self.synonymsmergetable)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row[0] is not None:
			taxanum = row[0]
			self.maxpage = int(taxanum / self.pagesize + 1)
		else:
			taxanum = 0
			self.maxpage = 0
	
	
	def updateAcceptedTaxaIDs(self):
		page = 1
		while page <= self.maxpage:
			self.updateAcceptedTaxaIDsPage(page)
			page += 1
		
	
	def updateAcceptedTaxaIDsPage(self, page):
		#pudb.set_trace()
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		if page % 10 == 0:
			logger.info("TaxaSynonymsMerger update accepted taxa ids page {0}".format(page))
		query = """
		 -- set the accepted taxon id (aka syn_taxon_id) for synonyms that still have a accepted taxon
		 -- with SourceTaxonID, TaxonomySourceID and SourceProjectID
		 -- in the TaxaMergeTable
		UPDATE `{0}` sm
		INNER JOIN `{1}` mt ON (
			sm.SourceAcceptedTaxonID = mt.SourceTaxonID
			AND sm.TaxonomySourceID = mt.TaxonomySourceID
			AND sm.SourceProjectID = mt.SourceProjectID
		)
		SET sm.syn_taxon_id = mt.`id`
		WHERE sm.id BETWEEN %s AND %s
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		
		log_queries.info(query)
		
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		
		
		query = """
		 -- set the accepted taxon id (aka syn_taxon_id) for synonyms that 
		 -- did not have a accepted taxon connected via
		 -- SourceTaxonID, TaxonomySourceID and SourceProjectID
		 -- in the TaxaMergeTable
		 -- so try to update it via the taxon name and familyCache
		UPDATE `{0}` sm
		INNER JOIN `{1}` mt ON (
			sm.accepted_taxon = mt.taxon
			AND sm.familyCache = mt.familyCache
		)
		SET sm.syn_taxon_id = mt.`id`
		WHERE sm.syn_taxon_id IS NULL
		AND sm.id BETWEEN %s AND %s
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		
		log_queries.info(query)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()


	def setFamilyCache(self):
		logger.info("TaxaSynonymsMerger set familyCache")
		page = 1
		
		self.createFamilyCacheTempTable()
		
		while page <= self.maxpage:
			self.setFamilyCachePage(page)
			page += 1
		logger.info("TaxaSynonymsMerger set familyCache done")
		return


	def createFamilyCacheTempTable(self):
		query = """
		DROP TEMPORARY 
		TABLE IF EXISTS familycache_temp
		;"""
		log_queries.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TEMPORARY 
		TABLE familycache_temp (KEY(SourceTaxonID), KEY(TaxonomySourceID), KEY(sourceProjectID))
		SELECT DISTINCT
		t1.SourceTaxonID, t1.TaxonomySourceID, t1.sourceProjectID, t2.taxon as family FROM `{0}` t1
		INNER JOIN `{1}` tr ON (
		tr.DescendantID = t1.SourceTaxonID
		AND tr.TaxonomySourceID = t1.TaxonomySourceID
		AND tr.SourceProjectID = t1.sourceProjectID)
		INNER JOIN `{0}` t2
		ON(
		tr.AncestorID = t2.SourceTaxonID
		AND tr.TaxonomySourceID = t2.TaxonomySourceID
		AND tr.SourceProjectID = t2.sourceProjectID)
		WHERE t2.`rank` = 'fam.'
		;""".format(self.taxamergetable, self.closuretable)
		
		log_queries.info(query)
		self.cur.execute(query)
		self.con.commit()
		return


	def setFamilyCachePage(self, page):
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		if page % 10 == 0:
			logger.info("TaxaSynonymsMerger set familyCache page {0}".format(page))
		
		
		query = """
		UPDATE `{0}` sm
		INNER JOIN `{1}` mt ON (
			sm.SourceAcceptedTaxonID = mt.SourceTaxonID
			AND sm.TaxonomySourceID = mt.TaxonomySourceID
			AND sm.SourceProjectID = mt.SourceProjectID
		)
		SET sm.accepted_taxon = mt.taxon
		WHERE sm.id BETWEEN %s AND %s
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		
		log_queries.info(query)
		self.cur.execute(query, [startid, lastid])
		self.con.commit()
		
		
		query = """
		UPDATE `{0}` sm
		INNER JOIN `{1}` mt ON (
			sm.SourceAcceptedTaxonID = mt.SourceTaxonID
			AND sm.TaxonomySourceID = mt.TaxonomySourceID
			AND sm.SourceProjectID = mt.SourceProjectID
		)
		INNER JOIN familycache_temp fam
		ON (mt.SourceTaxonID = fam.SourceTaxonID
		AND mt.TaxonomySourceID = fam.TaxonomySourceID
		AND mt.SourceProjectID = fam.sourceProjectID)
		SET sm.familyCache = fam.family
		WHERE sm.id BETWEEN %s AND %s
		;
		""".format(self.synonymsmergetable, self.taxamergetable)
		
		log_queries.info(query)
		self.cur.execute(query, [startid, lastid])
		#log_queries.info(query)
		self.con.commit()
		
		log_queries.info('query done')







