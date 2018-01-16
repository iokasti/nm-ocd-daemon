from requests import get
import pandas as pd
import json
from pymongo import MongoClient, GEO2D
import os
import argparse
import sys

# downloads csv.gz file from opencellid
def downloadCsv(transactions_source_csv_gz, token):
	# open in binary mode
	with open(transactions_source_csv_gz, "wb") as file:
		print('Downloading ' + transactions_source_csv_gz + '.')
		# get request
		request = get('https://download.unwiredlabs.com/ocid/downloads?token=' + token + '&file=cell_towers.csv.gz', stream=True)
		# request = get('http://ipv4.download.thinkbroadband.com/100MB.zip', stream=True)
		total_length = request.headers.get('content-length')
		if total_length is None: # no content length header
			print('Could not download ' + transactions_source_csv_gz +  '. Maybe your access token is limited.')
			return False
		else:
			dl = 0
			total_length = int(total_length)
			for chunk in request.iter_content(chunk_size = 4096):
				dl += len(chunk)
				file.write(chunk)
				done = int(50 * dl / total_length)
				sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )    
				sys.stdout.flush()
			return True


# updates database in order to store latitude and longitude as parts of "coords"
# also ensures "coords" is a 2D index (this way we can easily find neighbour cells using MongoDB functions)
def updateLoc(dest):
	dest.update({}, 
		{'$rename':{'lon':'coords.lon', 'lat':'coords.lat'}, 
		'$set': {'coords.type':'Point'}}, 
		False, multi=True)
	dest.ensure_index([('coords', GEO2D)])


# drops the database if the "drop" option is set to true
# inserts all data from csv.gz files to the database
# "ensures" all specified indexes
def toMongo(dest, data, drop, idxs=[]):
	if (drop):
		dest.drop()
	for chunk in data:
		dest.insert(json.loads(chunk.to_json(orient='records')))
	updateLoc(dest)
	for idx in idxs:
		dest.ensure_index(idx)

# connects to the database, parses the data and stores them
def updateDatabase(db_host, db_name, db_port, chunk_size, drop, transactions_collection, transactions_source_csv_gz):
	# establish connection:
	mongoClient = MongoClient(db_host, db_port)
	mongoDb = mongoClient[db_name]
	print()
	print('Updating ' + db_host + ":" + str(db_port) + ":" + db_name + ":" + transactions_collection + ' using dataset ' + transactions_source_csv_gz + '.')
	# load data:
	transactions = pd.read_csv(transactions_source_csv_gz, compression='gzip', chunksize=chunk_size)
	# insert data:
	toMongo(mongoDb[transactions_collection], transactions, drop, ['mcc','net','area','cell'])
	# close connection:
	mongoClient.close()

def main():
	parser = argparse.ArgumentParser(description='Download open cell id dataset and store it to a MongoDB database.')
	# open cell id tokens: 9d9a0a15012fc1, 92af25912f69f6
	parser.add_argument('-tk', '--token', default='92af25912f69f6', help='Access token for unwiredlabs open cell id.')
	parser.add_argument('-ht', '--db_host', default='localhost', help='MongoDB host address.')
	parser.add_argument('-n', '--db_name', default='open_cell_id', help='MongoDB name.')
	parser.add_argument('-p', '--db_port', type=int, default=27017, help='MongoDB port.')
	parser.add_argument('-s', '--chunk_size', type=int, default=1000000, help='Insertion chunk size. Requires more RAM if set to higher values.')
	parser.add_argument('-d', '--drop', type=bool, default=False, help='Drops MongoDB collection if set to True.')
	parser.add_argument('-tc', '--transactions_collection', default='cell_towers', help='Name of transactions collection.')
	parser.add_argument('-ts', '--transactions_source_csv_gz', default='cell_towers.csv.gz', help='Name of transactions source.')
	parser.add_argument('-dl', '--download_transactions_source_csv_gz', type=bool, default=False, help='If set to True transactions_source_csv_gz is downloaded even if it already exists.')
	parser.add_argument('-r', '--remove_transactions_source_csv_gz', type=bool, default=False, help='Removes transactions_source_csv_gz if set to True.')

	args = parser.parse_args()
	

	if args.download_transactions_source_csv_gz:
		if downloadCsv(args.transactions_source_csv_gz, args.token):
			updateDatabase(args.db_host, args.db_name, args.db_port, args.chunk_size, args.drop, args.transactions_collection, args.transactions_source_csv_gz)
	elif os.path.isfile(args.transactions_source_csv_gz):
		updateDatabase(args.db_host, args.db_name, args.db_port, args.chunk_size, args.drop, args.transactions_collection, args.transactions_source_csv_gz)
	else:
		print(args.transactions_source_csv_gz + ' file not found.')
	if args.remove_transactions_source_csv_gz:
		os.remove(args.transactions_source_csv_gz)

if __name__ == '__main__':
	main()		

