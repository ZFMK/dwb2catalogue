import logging
import logging.config

logger = logging.getLogger('sync_webportal')

import pudb


from .InsertTaxa import InsertTaxa


class InsertSynonyms(InsertTaxa):
	def __init__(self, taxagetter, globalconfig):
		
		InsertTaxa.__init__(self, taxagetter, globalconfig)
	
	def copyTaxa(self):
		self.taxa = self.taxagetter.getNextTaxaPage()
		
		while self.taxa is not None:
			self.setPlaceholderString(self.taxa)
			self.setValuesFromLists(self.taxa)
			
			query = """
			INSERT INTO `TaxaSynonymsMergeTable`
			(`TaxonomySourceID`, `SourceTaxonID`, `SourceProjectID`, `SourceAcceptedTaxonID`, `taxon`, `author`, `rank`)
			VALUES {0}
			;""".format(self.placeholderstring)
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.taxa = self.taxagetter.getNextTaxaPage()
			
	




