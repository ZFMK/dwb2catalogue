#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
Transfer the data from CacheDB to web portal
Create a temporary MySQL Database that has the same structure as the web portals database
method to transfer the data to webportals database?
drop temporary database
"""

import pudb

from datetime import datetime
from collections import OrderedDict

from DBConnectors.MySQLConnector import MySQLConnector

class TempDB():
	def __init__(self, globalconfig):
		# connect to MySQL target database and get a connection and cursor object
		self.temp_db = MySQLConnector(globalconfig.production_db_parameters)
		self.cur = self.temp_db.getCursor()
		self.con = self.temp_db.getConnection()
		self.execute = self.temp_db.cur.execute #getExecuteMethod()
		# take over configuration object as it is needed later in method create_temp_scheme
		self.config = globalconfig
		self.db_suffix = globalconfig.db_suffix

		self.temp_db_name = self.get_name(newdb=True)
		# store the name in global config
		self.config.setTempDBName(self.temp_db_name)

		self.prod_db = globalconfig.production_db_name
		self.create_queries = CreateTableQueries(globalconfig)

		self.drop_temp_dbs()
		self.create_temp_scheme()

		# new connect to temp database to get the right database name
		self.temp_db.closeConnection()

		dbconfig = self.config.temp_db_parameters
		self.temp_db = MySQLConnector(dbconfig)


	def __getCurrentTime(self, formatter="%Y%m%d%H%M%S"):
		return datetime.now().strftime(formatter)


	def getDBName(self):
		return self.temp_db_name


	def get_name(self, newdb=False):
		if newdb:  # -- first call, get a free name for temporary database
			counter = 1
			existing_dbs = []
			currenttime = self.__getCurrentTime()
			self.temp_db_name = """Transfer2Catalog_{0}_{1}""".format(currenttime, counter)
			self.execute("SHOW DATABASES")
			rows = self.cur.fetchall()
			for row in rows:
				existing_dbs.append(row[0])
			while (self.temp_db_name in existing_dbs) and (counter < 1000):
				counter += 1
				self.temp_db_name = """Transfer2Catalog_{0}_{1}""".format(currenttime, counter)
			if counter >= 1000:
				raise ValueError('TempDB.get_name(): no free name found for temporary database, something is wrong')
			return self.temp_db_name
		# return the name of the temporary database that was set up before
		# or get the last used database name when an old transfer should be continued (parameter globalconfig.use_last_transfer is True)
		try:
			ret = self.temp_db_name
		except AttributeError:
			q1 = """SELECT `transfer_table` FROM `Sync_Transfer_Log` WHERE id=(SELECT MAX(id) FROM `Sync_Transfer_Log`)"""
			self.execute(q1)
			rows = self.cur.fetchone()
			for r in rows:
				self.temp_db_name = r
		return self.temp_db_name

	def drop_temp_dbs(self):
		""" Cleanup previous and leftover databases """

		q8 = 'show databases where `database` like "Transfer2Catalog_%"'
		self.execute(q8)
		rows = self.cur.fetchall()
		# keep the last three databases
		# for backup when using cronjob to do the transfer
		for r in rows[:-3]:
			q = """drop database `%s`""" % r
			self.execute(q)

	def create_temp_scheme(self):
		""" Original in biocase_media/Transfer2/transfer_sql.py """
		q2 = """create database `%s` CHARACTER SET utf8mb4;""" % self.temp_db_name
		q7 = """use `{0}`""".format(self.temp_db_name)

		self.execute(q2)
		self.execute(q7)

		# create tables
		for (table, query) in self.create_queries.iter_tables_query():
			self.execute(query())
		# self.execute(self.create_queries.Transfer_ID_Mapping())

		# fill Data_Category and Data_fields with standard data
		for (table_name, query) in self.create_queries.fill_category_field():
			self.execute(query)



class CreateTableQueries():
	def __init__(self, globalconfig):
		# the only needed config parameter is db_suffix here, so copy it for easier use
		self.db_suffix = globalconfig.db_suffix

	def column_field_mapping(self):
		return """SELECT id, field_name FROM `{0}_Data_Fields` WHERE lang='prg'""".format(self.db_suffix)


	def iter_tables_query(self):
		'''
		returns the SQL Statements to create tables in temporary database
		sets methods as function objects, thus they can be called with variable parameters later
		'''
		res_a = OrderedDict()
		res_a['Datasources'] = self.Datasources
		res_a['TaxonomySources'] = self.TaxonomySources
		res_a['Institutes'] = self.Institutes
		res_a['Specimen'] = self.Specimen
		res_a['Data_Category'] = self.Data_Category
		res_a['Data_Fields'] = self.Data_Fields
		res_a['Data'] = self.Data
		res_a['Data2Specimen'] = self.Data2Specimen
		res_a['Geo'] = self.Geo
		res_a['Project'] = self.Project
		res_a['CollectionProject'] = self.CollectionProject
		res_a['ProjectUser'] = self.ProjectUser
		res_a['Barcode'] = self.Barcode
		res_a['Barcode_Reads'] = self.Barcode_Reads
		res_a['Taxa'] = self.Taxa
		res_a['TaxaSynonyms'] = self.TaxaSynonyms
		res_a['TaxonomicRanksEnum'] = self.TaxonomicRanksEnum
		res_a['TaxaCommonNames'] = self.TaxaCommonNames
		res_a['TaxaPropertyTerms'] = self.TaxaPropertyTerms
		#res_a['TaxaPropertyTermsFillFixedValues'] = self.TaxaPropertyTermsFillFixedValues
		res_a['TaxaRedList'] = self.TaxaRedList
		res_a['TaxaFlat'] = self.TaxaFlat
		res_a['Media'] = self.Media
		res_a['SpecimenCompleteness'] = self.SpecimenCompleteness
		res_a['taxa_matched'] = self.taxa_matched
		res_a['taxa_not_matched'] = self.taxa_not_matched
		for table_name, query in iter(res_a.items()):
			yield (table_name, query)


	def Datasources(self):
		q = """CREATE TABLE `{0}_Datasources` (
			`DatasourceID` INT(10) NOT NULL AUTO_INCREMENT,
			`data_source_name` varchar(255),
			PRIMARY KEY (`DatasourceID`),
			UNIQUE KEY (`data_source_name`)
			) DEFAULT CHARSET=utf8mb4""".format(self.db_suffix)
		return q

	def Data(self):
		q = """CREATE TABLE `{0}_Data` (
				`id` INT NOT NULL AUTO_INCREMENT,
				`term` varchar(3000) DEFAULT NULL,
				`field_id` INT NOT NULL COMMENT 'references {0}_Data_Fields.id',
				PRIMARY KEY (`id`),
				KEY  `idx_term` USING BTREE (`term`(255) ASC),
				KEY `idx_field_id` USING BTREE (`field_id` ASC),
				FOREIGN KEY (field_id) REFERENCES {0}_Data_Fields(`id`)
			) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4""".format(self.db_suffix)
		return q

	def Data2Specimen(self):
		return """CREATE TABLE `{0}_Data2Specimen` (
			`id` INT NOT NULL AUTO_INCREMENT,
			`data_id` INT DEFAULT NULL,
			`specimen_id` INT DEFAULT NULL,
			PRIMARY KEY (`id`),
			KEY (`data_id`),
			KEY (`specimen_id`),
			FOREIGN KEY (`data_id`) REFERENCES `{0}_Data` (`id`),
			FOREIGN KEY (`specimen_id`) REFERENCES `{0}_Specimen` (`id`)
			) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COMMENT='n:m mapping der Einträge in _Data auf die Specimen in GB'""".format(self.db_suffix)

	def Data_Category(self):
		return """CREATE TABLE `{0}_Data_Category` (
		`id` varchar(4) NOT NULL,
		`category` varchar(45) DEFAULT NULL,
		PRIMARY KEY (`id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Data categories'""".format(self.db_suffix)

	def Data_Fields(self):
		return """CREATE TABLE {0}_Data_Fields (
		  `id` INT NOT NULL,
		  `lang` enum('prg','de','en') NOT NULL DEFAULT 'en' COMMENT 'Language. prg for internal use only!',
		  `field_name` varchar(255) DEFAULT NULL COMMENT 'The field name from the table in DiversityCollection, i.e. `AccessionNumber`',
		  `category` varchar(4) DEFAULT NULL COMMENT 'id zu _Data_Category',
		  `restricted` tinyint(3) unsigned DEFAULT '0' COMMENT 'wenn 1: Daten können nur von eingeloggten Benutzern eingesehen werden',
		  `order` tinyint(3) unsigned DEFAULT NULL,
		  `solr_type` varchar(45) DEFAULT NULL,
		  `solr_facet` varchar(45) DEFAULT NULL,
		  PRIMARY KEY (`id`,`lang`),
		  KEY (`category`),
		  FOREIGN KEY (`category`) REFERENCES {0}_Data_Category(id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Fieldnames (columns names) for the data'""".format(self.db_suffix)

	def Barcode(self):
		return """CREATE TABLE `{0}_Barcode` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `specimen_id` INT NOT NULL,
		  `analysis_id` int(10) COMMENT 'the id for a specific analysis type',
		  `analysis_number` varchar(50) DEFAULT NULL COMMENT 'Differentiate several barcodes of the organisme and the same type',
		  `region` varchar(50) DEFAULT NULL,
		  `sequence` text DEFAULT NULL,
		  `sequencing_date` varchar(50) DEFAULT NULL,
		  `responsible` varchar(255) DEFAULT NULL,
		  `withhold` varchar(255) DEFAULT NULL COMMENT 'if 1: do not publish!!!',
		  PRIMARY KEY (`id`),
		  KEY `idx_specimen_id` (`specimen_id`),
		  KEY (`analysis_id`),
		  KEY (`analysis_number`),
		  FOREIGN KEY (`specimen_id`) REFERENCES `{0}_Specimen` (`id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""".format(self.db_suffix)

	def Barcode_Reads(self):
		return """CREATE TABLE `{0}_Barcode_Reads` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `barcode_id` INT NOT NULL,
		  `read_id` INT DEFAULT NULL,
		  `term` mediumtext DEFAULT NULL,
		  `field_id` INT NOT NULL COMMENT 'references _Data_Fields.id',
		  PRIMARY KEY (`id`),
		  KEY `idx_term` (`term`(255)) USING BTREE,
		  KEY `idx_field_id` (`field_id`) USING BTREE,
		  KEY `idx_Barcode_Reads` (`barcode_id`) USING BTREE,
		  FOREIGN KEY (`barcode_id`) REFERENCES `{0}_Barcode` (`id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Reads from barcode sequencing';
		""".format(self.db_suffix)

	def Geo(self):
		return """CREATE TABLE `{0}_Geo` (
		`id` INT NOT NULL auto_increment,
		`specimen_id` INT NOT NULL,
		`lat` varchar(45) DEFAULT NULL,
		`lon` varchar(45) DEFAULT NULL,
		`center_x` varchar(45) DEFAULT NULL,
		`center_y` varchar(45) DEFAULT NULL,
		`radius` varchar(45) DEFAULT NULL,
		PRIMARY KEY (`id`),
		FOREIGN KEY (`specimen_id`) REFERENCES `{0}_Specimen` (`id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".format(self.db_suffix)

	def Specimen(self):
		return """CREATE TABLE `{0}_Specimen` (
		`id` INT NOT NULL auto_increment,
		`DatasourceID` int(10) NOT NULL,
		`CollectionSpecimenID` int(10) NOT NULL,
		`IdentificationUnitID` int(10) NOT NULL,
		`taxon_id` INT DEFAULT NULL COMMENT 'TNT TaxonID',
		`taxon` varchar(255) DEFAULT NULL,
		`FamilyCache` varchar(255), 
		`author` varchar(255) DEFAULT NULL,
		`barcode` BOOLEAN NOT NULL DEFAULT 0 COMMENT 'Specimen has barcode',
		`institute_id` INT DEFAULT NULL,
		`withhold` varchar(50) DEFAULT NULL,
		`AccDate` DATETIME DEFAULT NULL,
		`AccessionNumber` varchar(255) COMMENT 'copied here for easier access, otherwise it is in _Data table',
		`withhold_flagg` BOOLEAN,
		PRIMARY KEY (`id`),
		UNIQUE KEY `origin` (`DatasourceID`, `CollectionSpecimenID`, `IdentificationUnitID`),
		KEY `taxon_id` (`taxon_id`),
		KEY `taxon` (`taxon`),
		KEY `FamilyCache` (`FamilyCache`),
		FOREIGN KEY (`institute_id`) REFERENCES `{0}_Institutes` (`institute_id`),
		KEY `AccessionNumber` (`AccessionNumber`),
		FOREIGN KEY (`DatasourceID`) REFERENCES `{0}_Datasources` (`DatasourceID`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='All Specimen from CollectionSpecimen'""".format(self.db_suffix)

	def TaxonomySources(self):
		q = """CREATE TABLE `{0}_TaxonomySources` (
			`TaxonomySourceID` INT(10) NOT NULL AUTO_INCREMENT,
			`taxonomy_source_name` varchar(255) NOT NULL,
			PRIMARY KEY (`TaxonomySourceID`),
			UNIQUE KEY (`taxonomy_source_name`)
			) DEFAULT CHARSET=utf8mb4""".format(self.db_suffix)
		return q

	def Taxa(self):
		return """CREATE TABLE `{0}_Taxa` (
		`id` int(10) NOT NULL AUTO_INCREMENT,
		`taxon` varchar(255) NOT NULL,
		`author` varchar(255) DEFAULT NULL,
		`rank` varchar(25) NOT NULL,
		`parent_id` INT DEFAULT NULL,
		`lft` INT DEFAULT NULL,
		`rgt` INT DEFAULT NULL,
		`known` INT NOT NULL DEFAULT '0' COMMENT 'number of species',
		`collected` INT NOT NULL DEFAULT '0' COMMENT 'number of collected species',
		`barcode` INT NOT NULL DEFAULT '0' COMMENT 'number of species with barcodes',
		`collected_individuals` INT NOT NULL DEFAULT '0',
		`barcode_individuals` INT NOT NULL DEFAULT '0',
		`rank_code` int(10) NOT NULL DEFAULT '0',
		`scientificName` varchar(255),
		`matched_in_specimens` BOOLEAN DEFAULT 0,
		PRIMARY KEY (`id`),
		KEY `left` (`lft`),
		KEY `right` (`rgt`),
		KEY `parent_id` (`parent_id`),
		KEY `taxon` (`taxon`),
		key `idx_rank_code` (`rank_code` ASC),
		KEY `scientificName` (`scientificName`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='All taxa from tnt.diversityworkbench.de'""".format(self.db_suffix)

	def TaxonomicRanksEnum(self):
		return """CREATE TABLE `{0}_TaxonomicRanksEnum` (
		`rank` varchar(255),
		`rank_code` int(10) NOT NULL,
		UNIQUE KEY `rank` (`rank`),
		PRIMARY KEY (`rank_code`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;""".format(self.db_suffix)

	def TaxaFlat(self):
		return """CREATE TABLE `{0}_TaxaFlat` (
		`taxon_id` INT NOT NULL,
		`tax_species` varchar(100) DEFAULT NULL,
		`tax_species_parent_id` INT DEFAULT NULL,
		`tax_species_author` varchar(200) DEFAULT NULL,
		`tax_subgenus` varchar(100) DEFAULT NULL,
		`tax_subgenus_parent_id` INT DEFAULT NULL,
		`tax_genus` varchar(100) DEFAULT NULL,
		`tax_genus_parent_id` INT DEFAULT NULL,
		`tax_infratribe` varchar(100) DEFAULT NULL,
		`tax_infratribe_parent_id` INT DEFAULT NULL,
		`tax_subtribe` varchar(100) DEFAULT NULL,
		`tax_subtribe_parent_id` INT DEFAULT NULL,
		`tax_tribe` varchar(100) DEFAULT NULL,
		`tax_tribe_parent_id` INT DEFAULT NULL,
		`tax_supertribe` varchar(100) DEFAULT NULL,
		`tax_supertribe_parent_id` INT DEFAULT NULL,
		`tax_infrafamily` varchar(100) DEFAULT NULL,
		`tax_infrafamily_parent_id` INT DEFAULT NULL,
		`tax_subfamily` varchar(100) DEFAULT NULL,
		`tax_subfamily_parent_id` INT DEFAULT NULL,
		`tax_family` varchar(100) DEFAULT NULL,
		`tax_family_parent_id` INT DEFAULT NULL,
		`tax_superfamily` varchar(100) DEFAULT NULL,
		`tax_superfamily_parent_id` INT DEFAULT NULL,
		`tax_infraorder` varchar(100) DEFAULT NULL,
		`tax_infraorder_parent_id` INT DEFAULT NULL,
		`tax_suborder` varchar(100) DEFAULT NULL,
		`tax_suborder_parent_id` INT DEFAULT NULL,
		`tax_order` varchar(100) DEFAULT NULL,
		`tax_order_parent_id` INT DEFAULT NULL,
		`tax_superorder` varchar(100) DEFAULT NULL,
		`tax_superorder_parent_id` INT DEFAULT NULL,
		`tax_infraclass` varchar(100) DEFAULT NULL,
		`tax_infraclass_parent_id` INT DEFAULT NULL,
		`tax_subclass` varchar(100) DEFAULT NULL,
		`tax_subclass_parent_id` INT DEFAULT NULL,
		`tax_class` varchar(100) DEFAULT NULL,
		`tax_class_parent_id` INT DEFAULT NULL,
		`tax_superclass` varchar(100) DEFAULT NULL,
		`tax_superclass_parent_id` INT DEFAULT NULL,
		`tax_infraphylum` varchar(100) DEFAULT NULL,
		`tax_infraphylum_parent_id` INT DEFAULT NULL,
		`tax_subphylum` varchar(100) DEFAULT NULL,
		`tax_subphylum_parent_id` INT DEFAULT NULL,
		`tax_phylum` varchar(100) DEFAULT NULL,
		`tax_phylum_parent_id` INT DEFAULT NULL,
		`tax_superphylum` varchar(100) DEFAULT NULL,
		`tax_superphylum_parent_id` INT DEFAULT NULL,
		`tax_infrakingdom` varchar(100) DEFAULT NULL,
		`tax_infrakingdom_parent_id` INT DEFAULT NULL,
		`tax_subkingdom` varchar(100) DEFAULT NULL,
		`tax_subkingdom_parent_id` INT DEFAULT NULL,
		`tax_kingdom` varchar(100) DEFAULT NULL,
		`tax_kingdom_parent_id` INT DEFAULT NULL,
		`synonyms` varchar(300) DEFAULT NULL,
		`vernacular` varchar(100) DEFAULT NULL,
		PRIMARY KEY (`taxon_id`)
		) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COMMENT='Flat data representation of {0}_Taxa'""".format(self.db_suffix)

	def TaxaSynonyms(self):
		return """CREATE TABLE `{0}_TaxaSynonyms` (
		`id` INT NOT NULL AUTO_INCREMENT,
		`taxon_id` INT NOT NULL COMMENT 'the taxon-id of the synonym',
		`syn_taxon_id` INT NOT NULL COMMENT 'the taxon-id of the accepted name',
		`taxon` varchar(255) NOT NULL,
		`author` varchar(255) DEFAULT NULL,
		`rank_code` int(10) NOT NULL DEFAULT '0',
		PRIMARY KEY (`id`),
		KEY `taxon_id` (`taxon_id`),
		KEY `syn_taxon_id` (`syn_taxon_id`),
		KEY `name` (`taxon`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Synonyms from tnt.diversityworkbench.de'""".format(self.db_suffix)

	def TaxaCommonNames(self):
		return """CREATE TABLE `{0}_TaxaCommonNames` (
		`id` INT NOT NULL AUTO_INCREMENT,
		`taxon_id` INT NOT NULL,
		`name` varchar(255) NOT NULL,
		`code` varchar(2) NOT NULL DEFAULT 'de',
		`db_name` varchar(50) NOT NULL,
		PRIMARY KEY (`id`),
		KEY `taxon_id` (`taxon_id`),
		KEY `name` (`name`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Common taxon names from tnt.diversityworkbench.de'""".format(self.db_suffix)

	def TaxaPropertyTerms(self):
		return """CREATE TABLE `{0}_TaxaPropertyTerms` (
		`id` INT NOT NULL AUTO_INCREMENT,
		`term` varchar(800) NOT NULL,
		`category` enum('rl_category','rl_reference') NOT NULL,
		`lang` varchar(10) NOT NULL DEFAULT 'de',
		PRIMARY KEY (`id`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Terms for taxon properties'""".format(self.db_suffix)

	'''
	def TaxaPropertyTermsFillFixedValues(self):
		#fixed values because of portal software
		#this must be reworked
		return """INSERT INTO `{0}_TaxaPropertyTerms`
		(`id`, `term`, `category`, `lang`)
		 -- fixed values here because portal software uses ids that depend on TNT SQL queries
		 -- must be reworked
		VALUES 
		(27, 'AktuelleBestandssituation', 'rl_category', 'de'),
		(28, 'KurzfristigerBestandstrend', 'rl_category', 'de'),
		(29, 'LangfristigerBestandstrend', 'rl_category', 'de'),
		(30, 'LetzterNachweis', 'rl_category', 'de'),
		(31, 'Neobiota', 'rl_category', 'de'),
		(32, 'Risikofaktoren', 'rl_category', 'de'),
		(33, 'RL-Kategorie', 'rl_category', 'de'),
		(34, 'Sonderfälle', 'rl_category', 'de'),
		(35, 'Verantwortlichkeit', 'rl_category', 'de')
		;
		""".format(self.db_suffix)
	'''

	def TaxaRedList(self):
		return """CREATE TABLE `{0}_TaxaRedLists` (
		`id` INT NOT NULL AUTO_INCREMENT,
		`taxon_id` INT NOT NULL,
		`value` varchar(50) NOT NULL,
		`category_id` INT NOT NULL,
		`reference_id` INT NOT NULL,
		PRIMARY KEY (`id`),
		KEY `taxon_id` (`taxon_id`),
		KEY `category_id` (`category_id`),
		KEY `reference_id` (`reference_id`)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='Red list values from TNT Analysis'""".format(self.db_suffix)

	def Institutes(self):
		return """CREATE TABLE `{0}_Institutes` (
		`institute_id` INT NOT NULL auto_increment,
		`ExternalDatasourceID` int(10) NOT NULL,
		`DatasourceID` int(10) NOT NULL,
		`project_institute` varchar(255) NOT NULL,
		`project_name` varchar(255) NULL,
		`institute_short` varchar(80) NULL,
		`institute_name` varchar(255) NULL,
		PRIMARY KEY (`institute_id`),
		KEY (`ExternalDatasourceID`),
		KEY (`DatasourceID`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Institutes from CollectionExternalDatasource'""".format(self.db_suffix)

	def Project(self):
		query = """
		CREATE TABLE `{0}_Project` (
		`id` INT(10) NOT NULL AUTO_INCREMENT,
		`ProjectID` INT(10) NOT NULL,
		`DatasourceID` INT(10) NOT NULL,
		`Project` VARCHAR(50),
		`ProjectURI` VARCHAR(255),
		PRIMARY KEY(`id`),
		KEY(`ProjectID`, `DatasourceID`), 
		KEY(`ProjectID`),
		KEY(`DatasourceID`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;""".format(self.db_suffix)
		return query

	def ProjectUser(self):
		query = """
		 -- project_id is the new assigned id for project from the different Datasources, referencing to id in ZFMK_Coll_Project, ProjectID is the original ProjectID within the Datasource
		CREATE TABLE `{0}_ProjectUser` (
		`project_id` INT(10),
		`LoginName` VARCHAR(255),
		`ProjectID` INT(10) NOT NULL,
		`DatasourceID` INT(10) NOT NULL,
		ReadOnly BOOLEAN,
		PRIMARY KEY(project_id, LoginName),
		KEY(`project_id`),
		KEY(LoginName),
		KEY (ProjectID),
		KEY (DatasourceID),
		FOREIGN KEY (ProjectID, DatasourceID) REFERENCES `{0}_Project` (`ProjectID`, DatasourceID)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
		;""".format(self.db_suffix)
		return query

	def CollectionProject(self):
		return """CREATE TABLE `{0}_CollectionProject` (
		`specimen_id` INT NOT NULL,
		`ProjectID` int(10) NOT NULL,
		`DatasourceID` INT(10) NOT NULL,
		KEY (`specimen_id`),
		KEY (`ProjectID`),
		KEY (`DatasourceID`),
		FOREIGN KEY (`specimen_id`) REFERENCES `{0}_Specimen` (`id`),
		FOREIGN KEY (`ProjectID`, DatasourceID) REFERENCES `{0}_Project` (`ProjectID`, DatasourceID)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".format(self.db_suffix)

	def Media(self):
		return """CREATE TABLE `{0}_Media` (
		`id` INT NOT NULL auto_increment,
		`specimen_id` INT NOT NULL,
		`media_url` varchar(3000),
		`media_creator` varchar(3000),
		`license` varchar(3000),
		`media_type` varchar(50), -- ENUM('video', 'audio', 'image', 'image_stack', 'website'),
		`media_title` varchar(500),
		PRIMARY KEY (`id`),
		FOREIGN KEY (`specimen_id`) REFERENCES {0}_Specimen(`id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Media connected to a specific specimen, currently images are the only media used'""".format(self.db_suffix)

	def SpecimenCompleteness(self):
		return """CREATE TABLE `{0}_SpecimenCompleteness` (
		`id` INT NOT NULL auto_increment,
		`specimen_id` INT NOT NULL,
		`filled_terms` INT(10),
		`grouped_percent` INT(3),
		PRIMARY KEY (`id`),
		FOREIGN KEY (`specimen_id`) REFERENCES {0}_Specimen(`id`),
		KEY filled_terms(`filled_terms`),
		KEY grouped_percent(`grouped_percent`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='count of the number of terms filled for each specimens, used for a label showing the quality of the specimens data'
		;
		""".format(self.db_suffix)

	def taxa_matched(self):
		query = """
		CREATE TABLE IF NOT EXISTS taxa_matched
		(
		`specimen_id` INT(10),
		`taxon_id` INT(10),
		`taxon_name` varchar(255),
		KEY `specimen_id` (`specimen_id`),
		KEY `taxon_id` (taxon_id),
		KEY `taxon_name` (`taxon_name`)
		)
		;"""
		return query

	def taxa_not_matched(self):
		query = """
		CREATE TABLE IF NOT EXISTS taxa_not_matched
		(
		`specimen_id` INT(10),
		`scientificName` varchar(255),
		`taxon_name` varchar(255),
		KEY `specimen_id` (`specimen_id`),
		KEY `scientificName` (`scientificName`),
		KEY `taxon_name` (`taxon_name`)
		)
		;"""
		return query

	def fill_category_field(self):
		"""
		'prg': ColumnNames from DiversityCollection!
		Get infos for Barcodes from DiversityCollection_ZFMK.Parameter:
		SELECT [ParameterID] AS id, 'prg' AS lang, REPLACE([DisplayText], '_', ' ') AS field_name, cast([MethodID] AS varchar)+'DE' AS category, 1 as restricted,
			0 AS [order]
			FROM [DiversityCollection_SMNK].[dbo].[Parameter]
			WHERE ParameterID>=27
		UNION
			SELECT [ParameterID], 'de', REPLACE([DisplayText], '_', ' ') AS field_name, cast([MethodID] AS varchar)+'DE', 1,
			RANK() OVER (PARTITION BY [MethodID] ORDER BY [ParameterID] ASC)+23 AS [order]
			FROM [DiversityCollection_SMNK].[dbo].[Parameter]
			WHERE ParameterID>=27
		UNION
			SELECT [ParameterID], 'en', REPLACE([DisplayText], '_', ' ') AS field_name, cast([MethodID] AS varchar)+'EN', 1,
			RANK() OVER (PARTITION BY [MethodID] ORDER BY [ParameterID] ASC)+23 AS [order]
			FROM [DiversityCollection_SMNK].[dbo].[Parameter]
			WHERE ParameterID>27
		"""
		queries = {'Data_Category': """INSERT INTO `{0}_Data_Category` (`id`, `category`) 
				VALUES ('1DE', 'Sammlungsnummer'), ('1EN', 'Catalogue number'), 
				('2DE', 'Person'), ('2EN', 'Person name'), 
				('3DE', 'Biologie'), ('3EN', 'Biology'), 
				('4DE', 'Fundort'), ('4EN', 'Location'), 
				('5DE', 'Datum'), ('5EN', 'Date'), 
				('6DE', 'Habitat'), ('6EN', 'Habitat'), 
				('7DE', 'Methode'), ('7EN', 'Method'), 
				('8DE', 'Bemerkung'), ('8EN', 'Note'), 
				('9DE', 'Zahl (Anzahl, Höhe)'), ('9EN', 'Number'),
				('10DE', 'Taxon Name'), ('10EN', 'Taxon Name'), 
				('11DE', 'Institut'), ('11EN', 'Institute'),
				('12DE', 'Sequenz'), ('12EN', 'Sequence'),
				('13DE', 'Sammlung'), ('13EN', 'Collection'),
				('16DE', 'Sequenz-Rohdaten'), ('16EN', 'Sequence-Rawdata'),
				('17DE', 'Specimen Eigenschaften'), ('17EN', 'Specimen Properties')
				""".format(self.db_suffix),
			'Data_fields': """INSERT INTO `{0}_Data_Fields` (id,lang,field_name,category,restricted,`order`,solr_type,solr_facet)
				VALUES (1,'prg','AccessionNumber','1DE',1,0,'string',NULL),
				(1,'de','Katalognummer','1DE',0,2,NULL,NULL),
				(1,'en',"Catalogue number",'1EN',0,2,NULL,NULL),
				(2,'prg','Changed','1DE',1,0,'tdate',NULL),
				(2,'de',"Geändert am",'5DE',0,22,NULL,NULL),
				(2,'en',"Last changed",'5EN',0,22,NULL,NULL),
				(3,'prg','CollectingMethod','1DE',1,0,'string',NULL),
				(3,'de','Sammelmethode','7DE',0,5,NULL,NULL),
				(3,'en',"Collecting method",'7EN',0,5,NULL,NULL),
				(4,'prg','CollectingDate','1DE',1,0,'string',NULL),
				(4,'de','Sammeldatum','5DE',0,6,NULL,NULL),
				(4,'en',"Collecting date",'5EN',0,6,NULL,NULL),
				(5,'prg','CollectingNotes','1DE',1,0,'text_ws',NULL),
				(5,'de','Sammelbemerkung','8DE',1,8,NULL,NULL),
				(5,'en',"Collecting note",'8EN',1,8,NULL,NULL),
				(6,'prg','CollectorsName','2DE',1,0,'string',NULL),
				(6,'de','Sammlername','2DE',1,7,NULL,NULL),
				(6,'en',"Collectors name",'2EN',1,7,NULL,NULL),
				(7,'prg','Country','1DE',1,0,'string',NULL),
				(7,'de','Land','4DE',0,10,NULL,NULL),
				(7,'en','Country','4EN',0,10,NULL,NULL),
				(8,'prg','DepositorsAccessionNumber','1DE',1,0,'string',NULL),
				(8,'de',"Katalognummer des Einsenders",'1DE',0,21,NULL,NULL),
				(8,'en',"Depositors catalogue number",'1EN',0,21,NULL,NULL),
				(9,'prg','DepositorsName','1DE',1,0,'string','depositors_name_facet:-60'),
				(9,'de',"Name des Einsenders",'2DE',1,20,NULL,NULL),
				(9,'en',"Depositors name",'2EN',1,20,NULL,NULL),
				(10,'prg','FieldNumber','1DE',1,0,'string',NULL),
				(10,'de','Feldnummer','1DE',1,15,NULL,NULL),
				(10,'en',"Field number",'1EN',1,15,NULL,NULL),
				(11,'prg','HabitatDescription','1DE',1,0,'text_ws',NULL),
				(11,'de',"Habitat Beschreibung",'6DE',0,14,NULL,NULL),
				(11,'en',"Habitat description",'6EN',0,14,NULL,NULL),
				(12,'prg','LifeStage','1DE',1,0,'string',NULL),
				(12,'de','Alter','3DE',0,17,NULL,NULL),
				(12,'en',"Life stage",'3EN',0,17,NULL,NULL),
				(13,'prg','LocalityDescription','1DE',1,0,'text_ws',NULL),
				(13,'de','Fundortbeschreibung','4DE',0,13,NULL,NULL),
				(13,'en',"Locality description",'4EN',0,13,NULL,NULL),
				(14,'prg','Location','1DE',1,0,'string',NULL),
				(14,'de','Fundort','4DE',0,12,NULL,NULL),
				(14,'en','Location','4EN',0,12,NULL,NULL),
				(15,'prg','NumberOfUnits','1DE',1,0,'int',NULL),
				(15,'de',"Anzahl Individuen",'9DE',0,16,NULL,NULL),
				(15,'en',"Number of specimen",'9EN',0,16,NULL,NULL),
				(16,'prg','PreparationMethod','1DE',1,0,'text_ws',NULL),
				(16,'de','Präparations-Methode','7DE',0,19,NULL,NULL),
				(16,'en',"Preparation method",'7EN',0,19,NULL,NULL),
				(17,'prg','Sex','1DE',1,0,'string',NULL),
				(17,'de','Sex','3DE',0,18,NULL,NULL),
				(17,'en','Sex','3EN',0,18,NULL,NULL),
				(18,'prg','CollectorsNumber','1DE',1,0,'string',NULL),
				(18,'de','Sammlernummer','1DE',1,9,NULL,NULL),
				(18,'en',"Collectors number",'1EN',1,9,NULL,NULL),
				(19,'de',"Taxon Name",'10DE',0,3,NULL,NULL),
				(19,'en',"Taxon Name",'10EN',0,3,NULL,NULL),
				(20,'de','Barcode','1DE',0,24,NULL,NULL),
				(20,'en','Barcode','1EN',0,24,NULL,NULL),
				(21,'prg','LabName','1DE',1,0,'string',NULL),
				(22,'prg','Institute','1DE',0,0,NULL,'institute_facet:-90'),
				(22,'de','Institut','11DE',0,23,NULL,NULL),
				(22,'en','Institute','11EN',0,23,NULL,NULL),
				(23,'prg','Collection','13DE',0,0,'string','collection_facet:-90'),
				(23,'de','Sammlung','13DE',0,31,NULL,NULL),
				(23,'en','Collection','13EN',0,31,NULL,NULL),
				(26,'prg','State','4DE',0,0,'string','state_facet:-100'),
				(26,'de','Bundesland','4DE',0,11,NULL,NULL),
				(26,'en','State','4EN',0,11,NULL,NULL),
				(27,'de','Art','10DE',0,0,NULL,NULL),
				(27,'en','Species','10EN',0,0,NULL,NULL),
				(28,'de','Artname','10DE',0,1,NULL,NULL),
				(28,'en',"Common name",'10EN',0,1,NULL,NULL),
				(30,'prg','project','12DE',1,0,'string',NULL),
				(30,'de','project','12DE',1,47,NULL,NULL),
				(30,'en','project','12EN',1,47,NULL,NULL),
				(31,'prg','failure','12DE',1,0,'string',NULL),
				(31,'de','failure','12DE',1,54,NULL,NULL),
				(31,'en','failure','12EN',1,54,NULL,NULL),
				(32,'prg','failure_detail','12DE',1,0,'string',NULL),
				(32,'de',"failure detail",'12DE',1,55,NULL,NULL),
				(32,'en',"failure detail",'12EN',1,55,NULL,NULL),
				(65,'prg','marker','12DE',1,0,'string',NULL),
				(65,'de',"Genetischer MArker",'12DE',1,49,NULL,NULL),
				(65,'en',"Genetic marker",'12EN',1,49,NULL,NULL),
				(66,'prg','pcr_primer_forward_name','16DE',1,0,'string',NULL),
				(66,'de',"PCR Primer vorwärts Name",'16DE',1,25,NULL,NULL),
				(66,'en',"PCR primer forward name",'16EN',1,25,NULL,NULL),
				(67,'prg','pcr_primer_forward_sequence','16DE',1,0,'text_noTokens',NULL),
				(67,'de',"PCR Primer vorwärts Sequenz",'16DE',1,26,NULL,NULL),
				(67,'en',"PCR primer forward Sequence",'16EN',1,26,NULL,NULL),
				(68,'prg','pcr_primer_reverse_name','16DE',1,0,'string',NULL),
				(68,'de',"PCR Primer rückwärts Name",'16DE',1,27,NULL,NULL),
				(68,'en',"PCR primer reverse name",'16EN',1,27,NULL,NULL),
				(69,'prg','pcr_primer_reverse_sequence','16DE',1,0,'text_noTokens',NULL),
				(69,'de',"PCR Primer rückwärts Sequenz",'16DE',1,28,NULL,NULL),
				(69,'en',"PCR primer reverse Sequence",'16EN',1,28,NULL,NULL),
				(74,'prg','sequencing_timestamp','16DE',1,0,'tdate',NULL),
				(74,'de','Sequenzierungsdatum','16DE',1,33,NULL,NULL),
				(74,'en',"Sequencing timestamp",'16EN',1,33,NULL,NULL),
				(75,'prg','sequencing_labor','16DE',1,0,'string',NULL),
				(75,'de','Sequenzierlabor','16DE',1,34,NULL,NULL),
				(75,'en',"Sequencing lab",'16EN',1,34,NULL,NULL),
				(76,'prg','direction','16DE',1,0,'string',NULL),
				(76,'de','Richtung','16DE',1,35,NULL,NULL),
				(76,'en','Direction','16EN',1,35,NULL,NULL),
				(77,'prg','trace_filename','16DE',1,0,'string',NULL),
				(77,'de',"Trace Dateiname",'16DE',1,36,NULL,NULL),
				(77,'en',"Trace filename",'16EN',1,36,NULL,NULL),
				(78,'prg','trace_file_url','16DE',1,0,NULL,NULL),
				(78,'de',"Trace Datei URL",'16DE',1,37,NULL,NULL),
				(78,'en',"Trace file url",'16EN',1,37,NULL,NULL),
				(79,'prg','trace_file_org_length','16DE',1,0,NULL,NULL),
				(79,'de',"Trace Datei: Länge der Originaldatei",'16DE',1,38,NULL,NULL),
				(79,'en',"Trace file org length",'16EN',1,38,NULL,NULL),
				(80,'prg','trace_file_org_md5','16DE',1,0,NULL,NULL),
				(80,'de',"Trace Datei: MD5 Summe der Originaldatei",'16DE',1,39,NULL,NULL),
				(80,'en',"Trace file org md5",'16EN',1,39,NULL,NULL),
				(81,'prg','trace_file_encoded','16DE',1,0,NULL,NULL),
				(81,'de',"Trace Datei kodiert",'16DE',1,40,NULL,NULL),
				(81,'en',"Trace file encoded",'16EN',1,40,NULL,NULL),
				(82,'prg','trace_file_encoding','16DE',1,0,NULL,NULL),
				(82,'de',"Trace Datei Kodierung",'16DE',1,41,NULL,NULL),
				(82,'en',"Trace file encoding",'16EN',1,41,NULL,NULL),
				(83,'prg','trace_file_encoding','16DE',1,0,NULL,NULL),
				(83,'de',"Trace Datei: Länge der kodierten Datei)",'16DE',1,42,NULL,NULL),
				(83,'en',"Trace file enc length",'16EN',1,42,NULL,NULL),
				(84,'prg','trace_file_encoding','16DE',1,0,NULL,NULL),
				(84,'de',"Trace Dateiformat",'16DE',1,43,NULL,NULL),
				(84,'en',"Trace file format",'16EN',1,43,NULL,NULL),
				(86,'prg','trace_id','16DE',1,0,NULL,NULL),
				(86,'de','Tracenummer','16DE',1,44,NULL,NULL),
				(86,'en',"Trace id",'16EN',1,44,NULL,NULL),
				(87,'prg','sequence_id','16DE',1,0,NULL,NULL),
				(87,'de','Sequenznummer','16DE',1,45,NULL,NULL),
				(87,'en',"Sequence id",'16EN',1,45,NULL,NULL),
				(88,'prg','well','16DE',1,0,NULL,NULL),
				(88,'de','PLattenposition','16DE',1,46,NULL,NULL),
				(88,'en','Well','16EN',1,46,NULL,NULL),
				(89,'prg','sequence_length','12DE',1,0,NULL,NULL),
				(89,'de','Sequenzlänge','12DE',1,51,NULL,NULL),
				(89,'en',"Sequence length",'12EN',1,51,NULL,NULL),
				(90,'prg','trace_count','12DE',1,0,NULL,NULL),
				(90,'de',"Trace Anzahl",'12DE',1,52,NULL,NULL),
				(90,'en',"Trace count",'12EN',1,52,NULL,NULL),
				(91,'prg','barcode_compliant','12DE',1,0,NULL,NULL),
				(91,'de',"Barcode konform",'12DE',1,53,NULL,NULL),
				(91,'en',"Barcode compliant",'12EN',1,53,NULL,NULL),
				(93,'prg','sequence','12DE',1,50,'text_noTokens',NULL),
				(93,'de','Barcode','12DE',1,50,NULL,NULL),
				(93,'en','Barcode','12EN',1,50,NULL,NULL),
				(94,'prg','sequencing_primer_name','16DE',1,0,'string',NULL),
				(94,'de',"Sequenzierungs Primer Name",'16DE',1,29,NULL,NULL),
				(94,'en',"Sequencing primer name",'16EN',1,29,NULL,NULL),
				(95,'prg','sequencing_primer_sequence','16DE',1,0,'text_noTokens',NULL),
				(95,'de',"Sequenzierungs Primer Sequenz",'16DE',1,30,NULL,NULL),
				(95,'en',"Sequencing primer Sequence",'16EN',1,30,NULL,NULL),
				(96,'prg','typestatus','17DE',0,0,NULL,NULL),
				(96,'de',"Typenstatus",'17DE',0,60,NULL,NULL),
				(96,'en',"Type status",'17EN',0,60,NULL,NULL),
				(97,'prg','DeterminedBy','2DE',1,0,NULL,NULL),
				(97,'de',"Bestimmt durch",'2DE',1,4,NULL,NULL),
				(97,'en',"Determined by",'2EN',1,4,NULL,NULL),
				(98,'prg','DeterminationDate','5DE',1,0,NULL,NULL),
				(98,'de',"Bestimmungsdatum",'5DE',1,65,NULL,NULL),
				(98,'en',"Date of Determination",'5EN',1,65,NULL,NULL),
				(99,'prg','LabelTitle','1DE',1,0,'string',NULL),
				(99,'de',"Etikett",'1DE',1,65,NULL,NULL),
				(99,'en',"Label on Specimen",'1EN',1,65,NULL,NULL)
				""".format(self.db_suffix)}
		for (table_name, query) in iter(queries.items()):
			yield (table_name, query) #.replace('\n',' ').replace('\t',''))

