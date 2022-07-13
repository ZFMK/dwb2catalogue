import time
import requests

import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

import logging, logging.config
logger = logging.getLogger('sync_webportal')


def runSolrIndexer():
	#pudb.set_trace()
	time.sleep(60)
	logger.info('####### Start solr indexer')
	
	solr_url = config.get('solr', 'url')
	solr_user = config.get('solr', 'user')
	solr_passwd = config.get('solr', 'passwd')
	solr_core = config.get('solr', 'solr_core')
	
	url = """{0}{1}/dataimport?command=full-import""".format(solr_url, solr_core)
	try:
		response = requests.get(url, auth=(solr_user,solr_passwd), verify = False)
		if response.status_code != 200:
			return (False, response.text)
		else:
			pass
	except:
		return (False, response.text)
