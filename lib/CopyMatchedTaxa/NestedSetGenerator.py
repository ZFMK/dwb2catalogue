import pudb

import logging
import logging.config

logger = logging.getLogger('sync_webportal')
log_missing_taxa = logging.getLogger('missing_taxa')

import pudb

from DBConnectors.MySQLConnector import MySQLConnector

class NestedSetGenerator():
	def __init__(self, globalconfig):
		self.config = globalconfig
		
		dbconfig = self.config.temp_db_parameters
		self.dbcon = MySQLConnector(dbconfig)
		
		self.cur = self.dbcon.getCursor()
		self.con = self.dbcon.getConnection()
		self.db_suffix = self.config.db_suffix
		self.queries = NestedSetQueries(self.config)
		
		self.generate_nested_set()
		
		

		
		
		
	def generate_nested_set(self):
		logger.info('Nested set start')
		counter = 0
		start_id = 1
		start_parent_id = 1
		counter = self.nested_set_walktree(start_id, start_parent_id, counter, 'root')
		self.cur.execute(self.queries.set_right(start_id, counter))
		self.con.commit()
		logger.info('Generation of nested set end')

	def nested_set_walktree(self, taxon_id, parent_id, counter, rank):
		# set left of the first child to the given counter
		self.cur.execute(self.queries.set_left(taxon_id, counter))
		self.con.commit()
		counter+=1

		# get all childrens (taxa that have current taxon_id as parent_id)
		self.cur.execute(self.queries.get_children(taxon_id))
		#debugvar2 = self.ph_queries.get_children(taxon_id)
		rows = self.cur.fetchall()

		# step into the childrens
		for a in rows:
			counter = self.nested_set_walktree(int(a[0]),taxon_id, counter, a[1])

		# counter is incremented with each call of self.__nested_set_walktree, thus it contains the next free counter value
		self.cur.execute(self.queries.set_right(taxon_id, counter))
		self.con.commit()
		counter+= 1
		return counter





class NestedSetQueries():
	def __init__(self, config):
		self.db_suffix = config.db_suffix
		pass
		
		
	def set_left(self,taxon_id,left_id):
		return """UPDATE `{2}_Taxa` SET `lft`='{1}' WHERE `id`='{0}'""".format(taxon_id, left_id, self.db_suffix)

	def set_right(self,taxon_id,right_id):
		return """UPDATE `{2}_Taxa` SET rgt='{1}' WHERE `id`='{0}'""".format(taxon_id, right_id, self.db_suffix)

	def get_children(self,parent_id):
		return """SELECT `id`, `rank` FROM `{1}_Taxa` WHERE `parent_id`='{0}'""".format(parent_id, self.db_suffix)


	# not used
	'''
	def get_leaves(self):
		return """SELECT `id` FROM `{0}_Taxa` WHERE `lft`=rgt-1""".format(self.db_suffix)
	'''

