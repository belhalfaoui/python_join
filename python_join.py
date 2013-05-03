"""
    python_join

This library allows you to get data from various SQL databases, "join" them together within
your Pyhon script, and write the result into your own database.

Usage :
	tb = TableBuilder(
		main_db = 'platform',
		main_query = "SELECT ...",
		destination_table = 'Your_table',
		verbose = True
	)
	tb.build()
	tb.add_source('my_source1', 'ecom', "SELECT id, ...", join_on=2, outer_join=True, keep_key_column=True)
	tb.add_source('my_source2', 'platform', "SELECT id, ...", join_on=6, outer_join=True, keep_key_column=True)
	tb.join()
	tb.write()
	tb.reporting()

In this example, we will :
- download the mail data using 'main_query',
- for each of those rows, join :
	- the first column of 'my_source1' on the 2nd column of 'main_query'
	- the first column of 'my_source2' on the 6nd column of 'main_query'
(keeping the columns 2 and 6 of 'main_query' in the final result)
- write the result in 'Your_table'
"""


from connection import DB
from datetime import datetime

class SourceAlreadyExistsError(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return "Error while adding source '%s' : source already exists" % self.value

		
class TableBuilder(object):
	def __init__(self, main_db, main_query, create_query, destination_table, verbose=False, destination_db='datawarehouse'):
		self.sources = {}
		self.sources_ordered = []
		self.destination_table = destination_table
		self.create_query = create_query
		self.verbose = verbose
		self.result = []
		self.destination_db = destination_db
		self.start_time = start_time = datetime.now()
		
		self.main_source = {
			'db':main_db,
			'query':main_query,
			'data':[]
		}
		self._get_data(None)
	
	def build(self):
		with DB(self.destination_db) as dw:
			query1 = "DROP TABLE IF EXISTS `"+self.destination_table+"`"
			query2 = self.create_query
			dw.execute(query1)
			dw.execute(query2)
	
	def add_source(self, name, db, query, join_on, outer_join=False, keep_key_column=False):
		if name in self.sources or name=='main':
			raise SourceAlreadyExistsError(name)
		else :
			self.sources[name] = {
				'db':db,
				'query':query,
				'join_on':join_on,
				'outer_join':outer_join,
				'keep_key_column':keep_key_column,
				'errors_count':0,
				'matches_count':0,
				'errors':[],
				'data':None
			}
			
		self._get_data(name)
		
	def _get_data(self, source_name):
		if self.verbose:
			print "... Getting data from '%s' source ..." % (source_name or 'main')
		
		if source_name == None:
			source = self.main_source
		else :
			source = self.sources[source_name]
		
		with DB(source['db']) as db:
			data = db.execute(source['query'])
			
			if source_name == None:
				self.main_source['data'] = data
			else :
				self.sources[source_name]['data'] = self._get_dictionary(data)
	
	def _get_dictionary(self, data):
		dict = {}
		for e in data:
			dict[e[0]]=e[1:]
		return dict
	
	def _get_sources_order(self):
		fields = {self.sources[s]['join_on']:s for s in self.sources}
		self.sources_ordered = [fields[e] for e in sorted(fields)]
	
	def _append_result_row(self, row, matches):
		result_row = []
		curs = 0
		
		if all([matches[m][0]!=None or self.sources[m]['outer_join'] for m in matches]):
		
			for s_name in self.sources_ordered:
				source = self.sources[s_name]
				result_row += row[curs:(source['join_on']+source['keep_key_column'])]
				result_row += matches[s_name]
				curs = source['join_on']+1
			
			result_row += row[curs:]
			self.result.append(result_row)
		
	def join(self):
		matches = None
		self._get_sources_order()
		
		if self.verbose:
			print "... Joining sources ..."

		for row in self.main_source['data']:
			matches = {}
			
			for s_name,s in self.sources.items():
				try:
					matches[s_name] = s['data'][row[s['join_on']]]
					s['matches_count'] += 1
				except Exception, e:
					matches[s_name] = (None,)*len(s['data'].values()[0])
					s['errors'].append(row[s['join_on']])
					s['errors_count'] += 1
			
			self._append_result_row(row, matches)
		
	def write(self):
		if self.verbose:
			print "... Writing the data into the datawarehouse ..."
			
		with DB(self.destination_db) as dw:
			query = "REPLACE INTO "+self.destination_table+" VALUES (" + ",".join(["%s"] * len(self.result[0])) + ")"
			dw.execute(query, values=self.result, many=True)
	
	def reporting(self):
		print "- Main source (from '%s') :	%s rows" % (self.main_source['db'], len(self.main_source['data']))
		
		for s_name,s in self.sources.items():
			print ""
			print "- Source '%s' :		%s rows	%s matches	%s errors" % (s_name, len(s['data']), s['matches_count'], s['errors_count'])
			if self.verbose and s['errors_count']>0:
				print "		* Errors : " + ("	".join(map(str, s['errors'])))
		print ""
		print "(Execution time : %s)" % (datetime.now()-self.start_time);