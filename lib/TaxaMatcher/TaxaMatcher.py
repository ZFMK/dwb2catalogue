#!/usr/bin/env python
# -*- coding: utf8 -*-
import re

import logging
import logging.config
logger = logging.getLogger('sync_webportal')
log_queries = logging.getLogger('query')

import pudb

from .TaxaMatchTable import TaxaMatchTable

from DBConnectors.MySQLConnector import MySQLConnector


class TaxaMatcher():
	def __init__(self, globalconfig):
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		
		self.db_suffix = self.config.db_suffix
		
		self.specimentable = "{0}_Specimen".format(self.db_suffix)
		#self.taxamergetable = "{0}_TaxaMergeTable".format(self.db_suffix)
		self.pagesize = 10000
		self.pagingstarted = False
		
		self.removeTaxonIDs()
		
		self.setMaxPage()
		
		self.taxonpattern = re.compile('([A-Z][a-z|-]+)\s*(\([^\,]*?\))*\s*([a-z|-]+)*\s*([a-z|-]+)*\s*([\(]*[A-Z].*\,.*)*')
		#self.subgenpattern = re.compile('\(.+\)')
		
		# fix for our Ichthyiology department that added Eschmeyer chapter numbers to the family names
		self.eschmeyerpattern = re.compile(r'\s*\d+\s*$')
		
		self.matchingtable = TaxaMatchTable(self.dbcon, self.config)
		self.matchingtable.createTempTable()
		self.matchingtable.createTaxaNotMatchedTable()
		self.matchingtable.createTaxaMatchedTable()
		
		self.matchTaxa()
		#self.copyMatchedTaxa()
		
		self.deleteUnMatchedSpecimens()
	

	def setMaxPage(self):
		query = """
		SELECT MAX(id) FROM `{0}`
		;
		""".format(self.specimentable)
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row[0] is None:
			self.maxpage = 0
		elif row[0] <= 0:
			self.maxpage = 0
		else:
			self.maxpage = int(row[0] / self.pagesize + 1)
	
	def initPaging(self):
		self.currentpage = 1
		self.pagingstarted = True
	
	def getNextSpecimenPage(self):
		if self.pagingstarted is False:
			self.initPaging()
		if self.currentpage <= self.maxpage:
			specimens = self.getSpecimenPage(self.currentpage)
			self.currentpage = self.currentpage + 1
			if len(specimens) > 0:
				return specimens
			else:
				# there might be gaps in the primary key column
				#pudb.set_trace()
				while len(specimens) <= 0 and self.currentpage <= self.maxpage:
					specimens = self.getSpecimenPage(self.currentpage)
					self.currentpage = self.currentpage + 1
					if len(specimens) > 0:
						return specimens
				self.pagingstarted = False
				return None
		else:
			self.pagingstarted = False
			return None
	
	def getSpecimenPage(self, page):
		if page % 10 == 0:
			logger.info("TaxaMatcher get specimen page {0}".format(page))
			
		startrow = ((page-1)*self.pagesize)+1
		lastrow = startrow + self.pagesize-1
		
		parameters = [startrow, lastrow]
		self.specimenquery = """
		SELECT `id`, `taxon`, `FamilyCache` FROM {0}
		WHERE `id` BETWEEN %s AND %s
		;
		""".format(self.specimentable)
		
		self.cur.execute(self.specimenquery, parameters)
		specimendata = self.cur.fetchall()
		return specimendata
	
	
	def matchTaxa(self):
		self.specimendata = self.getNextSpecimenPage()
		
		while self.specimendata is not None:
			
			self.calculateNames()
			self.matchingtable.deleteTempTableData()
			self.matchingtable.insertIntoTempTable(self.placeholderstring, self.values)
			self.matchingtable.matchTaxa()
			self.matchingtable.updateTaxonIDsInSpecimens()
			
			#this is a hack to be compatible with former implementation where taxon and author is replaced in Specimen table, too
			#it is necessary, because the portal queries taxon and author from the Specimen table instead of the Taxa table, while solr uses the Taxa table
			# TODO: fix it in the portal and remove the following method from TaxaMatchTable
			self.matchingtable.updateTaxonAndAuthorInSpecimens()
			
			self.matchingtable.markMatchedTaxaInMergeTable()
			
			self.specimendata = self.getNextSpecimenPage()
			
			#if self.currentpage >= 70:
			#	pudb.set_trace()
			
		return
	
	
	
	def removeTaxonIDs(self):
		query = """
		UPDATE {0}
		set `taxon_id` = NULL
		WHERE `taxon_id` IS NOT NULL
		;""".format(self.specimentable)
		self.cur.execute(query)
		self.con.commit()
		
	
	
	
	
	def calculateNames(self):
		self.values = []
		self.placeholderstrings = []
		for specimenrow in self.specimendata:
			specimen_id = specimenrow[0]
			taxonstring = specimenrow[1]
			# copy the name as it is for comparison
			scientific_name = str(specimenrow[1])
			familycache = specimenrow[2]
			genus_name = None
			
			# test for synonyms
			#if taxonstring.startswith('Polemon lipar'):
			#	pudb.set_trace()
			
			#if taxonstring.startswith('Microcelis'):
			#	pudb.set_trace()
			
			# check that there is something in the taxonstring
			# if taxonstring is None put an empty string into it to allow the use of split()
			# should never happen?
			if (taxonstring is None):
				taxonstring = ''
			
			
			# if there is an empty string in taxonstring try to insert the data from FamiliyCache 
			if (taxonstring == ''):
				if (familycache is not None) and (familycache != ''):
					taxonstring = familycache
				
			taxon_name_parts = []
			m = self.taxonpattern.search(taxonstring)
			
			if m is None:
				taxon_name = taxonstring
			else:
				taxon_name_parts.append(m.groups()[0])
				# m.groups()[1] is the subgenus so i skip it
				# species epithet
				if m.groups()[2] is not None:
					taxon_name_parts.append(m.groups()[2])
					genus_name = m.groups()[0]
				# subspecies
				if m.groups()[3] is not None:
					taxon_name_parts.append(m.groups()[3])
			
			if len(taxon_name_parts) <= 0:
				# pudb.set_trace()
				taxon_name = None
			
			else:
				taxon_name = ' '.join(taxon_name_parts).strip()
			
			# add family if there is one
			family_name = familycache
			if family_name is not None:
				# clean families for our Ichthyology department (familyname + Eschmeyer chapter number)
				family_name = self.eschmeyerpattern.sub('', family_name)
			
			row = [specimen_id, scientific_name, taxon_name, genus_name, family_name]
			self.values.extend(row)
			self.placeholderstrings.append("(%s, %s, %s, %s, %s)")
		self.placeholderstring = ", ".join(self.placeholderstrings)
	
	
	
	def deleteUnMatchedSpecimens(self):
		# this deletes from specimen table and the connected data
		
		query = """
		DELETE br FROM `{0}_Barcode_Reads` br
		INNER JOIN `{0}_Barcode` b ON(br.`barcode_id` = b.`id`)
		INNER JOIN `{0}_Specimen` s ON(s.`id` = b.`specimen_id`)
		WHERE s.`taxon_id` IS NULL
		;
		""".format(self.db_suffix)
		self.cur.execute(query)
		self.con.commit()
		
		tables = ["Media", "Geo", "Barcode", "Data2Specimen", "CollectionProject"]
		
		for table in tables:
			query = """
			DELETE dt FROM `{0}_{1}` dt
			INNER JOIN `{0}_Specimen` s ON(s.`id` = dt.`specimen_id`)
			WHERE s.`taxon_id` IS NULL
			;
			""".format(self.db_suffix, table)
			self.cur.execute(query)
			self.con.commit()
		
		query = """
		DELETE pu FROM `{0}_ProjectUser` pu
		LEFT JOIN `{0}_CollectionProject` cp
		ON cp.ProjectID = pu.ProjectID AND cp.DatasourceID = pu.DatasourceID
		WHERE cp.ProjectID IS NULL
		;""".format(self.db_suffix)
		
		#log_queries.info(query)
		#pudb.set_trace()
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE p FROM `{0}_Project` p
		LEFT JOIN `{0}_CollectionProject` cp
		ON cp.ProjectID = p.ProjectID AND cp.DatasourceID = p.DatasourceID
		WHERE cp.ProjectID IS NULL
		;""".format(self.db_suffix)
		
		#log_queries.info(query)
		#pudb.set_trace()
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		DELETE s FROM `{0}_Specimen` s
		WHERE s.`taxon_id` IS NULL
		;
		""".format(self.db_suffix)
		self.cur.execute(query)
		self.con.commit()
		
		
	
	
	
