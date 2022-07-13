#!/usr/bin/env python
# -*- coding: utf8 -*-
from configparser import ConfigParser, NoOptionError
import re
import pudb

"""
Transfer the data from CacheDB to web portal
Read config parameters and prepare them for other classes

This is mainly needed for translation of some more advanced configuration settings. 
It provides a single object where all other classes can access the configuration parameters
"""


class ConfigReader():

	def __init__(self, config = None):
		self.config = config
		if not (isinstance(config, ConfigParser)):
			raise ValueError ('ConfigReader.__init__(): parameter config must be instance of ConfigParser')

		self.readStaticParameters()
		self.readTargetDBParameters()
		self.readDataSourceParameters()
		self.readTaxaMergerDBParameters()
		self.readTNTSourceParameters()


	def readStaticParameters(self):
		'''
		assign all necessary config values to attributes of ConfigReader object
		The attributes should be in the most useful format to be digested by the other classes
		'''
		
		# the production_db_name is set by the parameters in the section of the project selected here, thus the option production_db_name is useless
		# self.production_db_name = self.config.get('option', 'production_db_name')
		
		# this is the name of the webportal database to transfer to, the parameter project in options define which 
		# database connection to be used from config file in the project sections

		# self.project = self.config.get('option', 'project') # never used?
		self.project_name = self.config.get('option', 'project')
		
		if self.config.has_option('option', 'db_suffix'):
			self.db_suffix = self.config.get('option', 'db_suffix')
		
		if self.config.has_option('option', 'use_gbif_taxa'):
			self.use_gbif_taxa = self.config.getboolean('option', 'use_gbif_taxa')
			if self.use_gbif_taxa is True:
				self.gbif_db = self.config.get('GBIF_DB', 'db')
				self.gbif_taxa_table = self.config.get('GBIF_DB', 'table')
		else:
			self.use_gbif_taxa = False
		return


	def readTargetDBParameters(self):
		# target db is selected indirectly via [option] project section in config,
		# thus the choosen target depends on wich project is set there.
		# the corresponding parameter is the db parameter within the project
		# The project has been read before by readStaticParameters() method
		
		# get db_suffix for project section if any
		# else get db_suffix given in option section
		
		# self.project_name was set by readStaticParameters() before
		if self.config.has_option(self.project_name, 'db_suffix'):
			self.db_suffix = self.config.get(self.project_name, 'db_suffix')
		else:
			# has it been set in readStaticParameters?
			try:
				self.db_suffix
			except AttributeError:
				raise ValueError("No DB suffix given in config file")
		self.production_db_name = self.config.get(self.project_name, 'db')
		self.temp_db_name = ''
		
		# read the complete config section of the project into a dict
		self.my_db_parameters = self.config[self.project_name]
		self.production_db_parameters = {}
		for key in self.my_db_parameters:
			# use str function to get a copy, dirty
			self.production_db_parameters[key] = str(self.my_db_parameters[key])
		
		
		self.temp_db_parameters = {}
		
		for key in self.my_db_parameters:
			# use str function to get a copy
			self.temp_db_parameters[key] = str(self.my_db_parameters[key])
		self.temp_db_parameters['db'] = self.temp_db_name


	def readDataSourceParameters(self):
		'''
		assign all necessary config values depending on a data source
		'''
		self.data_sources = []
		sections = self.config.sections()
		for section in sections:
			if section[:12]=='data_source_' and section!='data_source_test':
				data_source = DataSourceDefinition(section, self.project_name)
				data_source.getConfig(self.config)
				self.data_sources.append(data_source)


	def setTempDBName(self, temp_db_name):
		self.temp_db_name = temp_db_name
		self.temp_db_parameters['db'] = self.temp_db_name
	
	def getTempDBName(self):
		return self.temp_db_parameters['db']

	def getProjectName(self):
		return self.project_name

	def getDataSourceConfig(self, data_source_name):
		for config in self.data_sources:
			if config.data_source_name == data_source_name:
				return config
		return 


	def readTaxaMergerDBParameters(self):
		self.taxadb_config = {
			'db': self.config.get('taxamergerdb', 'db'),
			'user': self.config.get('taxamergerdb', 'user'),
			'passwd': self.config.get('taxamergerdb', 'passwd')
			}
		
		try:
			self.taxadb_config['host'] = self.config.get('taxamergerdb', 'host')
		except NoOptionError:
			self.taxadb_config['host'] = 'localhost'
		
		try:
			self.taxadb_config['port'] = self.config.get('taxamergerdb', 'port')
		except NoOptionError:
			self.taxadb_config['port'] = 3306
		
		try:
			self.taxadb_config['charset'] = self.config.get('taxamergerdb', 'charset')
		except NoOptionError:
			self.taxadb_config['charset'] = 'utf8'
		
		self.taxadb_name = self.taxadb_config['db']


	def getTaxaMergerDBName(self):
		return self.taxadb_name


	def getTaxaMergerDBConfig(self):
		return self.taxadb_config


	def readTNTSourceParameters(self):
		'''
		assign all necessary config values depending on a data source
		'''
		self.tnt_sources = []
		sections = self.config.sections()
		for section in sections:
			if section[:4]=='tnt_' and section!='tnt_test':
				tnt_source = {}
				tnt_source['name'] = section
				tnt_source['connection'] = self.config.get(section, 'connection')
				tnt_source['dbname'] = self.config.get(section, 'dbname')
				tnt_source['projectids'] = [projectid.strip() for projectid in self.config.get(section, 'projectids').split(',')]
				self.tnt_sources.append(tnt_source)
		return


class DataSourceDefinition():
	'''
	read the needed parameters for each datasource and collect them in a DataSourceDefinition object.
	The objects are stored in ConfigReader.data_sources list above
	Thus they can be obtained one by one from the ConfigReader-object
	'''
	
	def __init__(self, data_source_name, project_name):
		self.data_source_name = data_source_name
		self.project_name = project_name
		
		self.project_id_string = None
		self.analysis_id_combined_string = None
		self.analysis_id_string = None
		self.analysis_id_method_string = None

		
	def getConfig(self, config):
		
		self.source_database = config.get(self.data_source_name, 'connection')
		if config.has_option(self.data_source_name, 'institute_id'):
			self.institute_id = config.get(self.data_source_name, 'institute_id')
		else:
			self.institute_id = None
		
		self.collection_id_string = None
		self.project_id_string = None
		
		if config.has_option(self.data_source_name, 'collection_id'):
			self.collection_id_string = self.__parse2sql(config.get(self.data_source_name, 'collection_id'))
			self.collection_id_string = self.collection_id_string.format('s.CollectionID')
		
		else:
			self.project_id_string = self.__parse2sql(config.get(self.data_source_name, 'project_id'))
			self.project_id_string = self.project_id_string.format('p.ProjectID')
		
		self.combined_analysis = []
		if config.has_option(self.data_source_name, 'analysis_id_tools'):
			self.combined_analysis.append(config.get(self.data_source_name, 'analysis_id_tools'))
			self.analysis_id_string = self.__parse2sql(config.get(self.data_source_name, 'analysis_id_tools')) #self.combined_analysis[0])
			self.analysis_id_string = self.analysis_id_string.format('AnalysisID')

		if config.has_option(self.data_source_name, 'analysis_id_methods'):
			self.combined_analysis.append(config.get(self.data_source_name, 'analysis_id_methods'))
			self.analysis_id_method_string = self.__parse2sql(config.get(self.data_source_name, 'analysis_id_methods')) #self.combined_analysis[-1])
			self.analysis_id_method_string = self.analysis_id_method_string.format('AnalysisID')

		if len(self.combined_analysis)>0:
			self.analysis_id_combined_string = self.__parse2sql(';'.join(self.combined_analysis))
			self.analysis_id_combined_string = self.analysis_id_combined_string.format('AnalysisID')
		
		if config.has_option(self.data_source_name, 'respect_withhold'):
			self.respect_withhold = config.getboolean(self.data_source_name, 'respect_withhold')
		else: 
			self.respect_withhold = True
		if config.has_option(self.data_source_name, 'mark_specimen_withhold'):
			self.mark_specimen_withhold = config.getboolean(self.data_source_name, 'mark_specimen_withhold')
		else: 
			self.mark_specimen_withhold = True


	def __parse2sql(self, c):
		"""
		generates WHERE clauses from the project ids and analysis ids given in config file
		';': separate different groups
		'-': from - to
		'|': or
		',': in (...,...)
		TODO: Throw error if syntax error!
		"""
		command = []
		or_token = []
		and_token = []
		re_digit = re.compile('^\d+?$')
		re_from_to = re.compile('^(\d+?)\-(\d+?)$')

		for token in c.split(';'):
			proceed=False
			if token.find('-') > -1:
				if re.match(re_from_to, token):
					from_to = token.split('-')
					command.append('{{0}} BETWEEN {0} AND {1}'.format(*from_to))
					proceed=True
			for s in [',', '|']:
				if not proceed and token.find(s) > -1:
					for o in token.split(s):
						if re.match(re_digit, o):
							or_token.append(o)
							proceed=True
			if not proceed and token.find('&') > -1:
				for o in token.split('&'):
					if re.match(re_digit, o):
						and_token.append(o)
			else:
				if re.match(re_digit, token):
					or_token.append(token)
		if len(or_token) > 0:
			command.append('{{0}} IN ({0})'.format(','.join(or_token)))
		if len(and_token) > 0:
			command.append('{0}=%s' % ' AND {0}='.join(and_token))

		if len(command)>0:
			return "(" + "(%s)" % ") OR (".join(command) +")"
		return ''
