from requests import get
import pandas as pd
import json
from pymongo import MongoClient, GEO2D
import os
import argparse

# downloads csv.gz file from opencellid
def downloadCsv(transactions_source_csv_gz, token):
	# open in binary mode
	with open(transactions_source_csv_gz, "wb") as file:
		# get request
		request = get('https://download.unwiredlabs.com/ocid/downloads?token=' + token + '&file=cell_towers.csv.gz', stream=True)
		for chunk in request.iter_content(chunk_size = 1024):
			if chunk:
				file.write(chunk)

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
		print(chunk)
		dest.insert(json.loads(chunk.to_json(orient='records')))
	updateLoc(dest)
	for idx in idxs:
		dest.ensure_index(idx)

# connects to the database, parses the data and stores them
def updateDatabase(db_host, db_name, db_port, chunk_size, drop, transactions_collection, transactions_source_csv_gz):
	# establish connection:
	mongoClient = MongoClient(db_host, db_port)
	mongoDb = mongoClient[db_name]

	try:
		# load data:
		transactions = pd.read_csv(
			transactions_source_csv_gz,
			compression='gzip',
			chunksize=chunk_size)
		# insert data:
		toMongo(mongoDb[transactions_collection],
				 transactions,
				 drop,
				 ['mcc','net','area','cell'])
		# close connection:
		mongoClient.close()
	except:
		print("Unexpected error. Maybe your access token is limited.")

def main():
	parser = argparse.ArgumentParser(description='Download open cell id dataset and store it to a MongoDB database.')
	parser.add_argument('-tk', '--token', default='9d9a0a15012fc1', help='Access token for unwiredlabs open cell id.')
	parser.add_argument('-ht', '--db_host', default='localhost', help='MongoDB host address.')
	parser.add_argument('-n', '--db_name', default='open_cell_id', help='MongoDB name.')
	parser.add_argument('-p', '--db_port', type=int, default=27017, help='MongoDB port.')
	parser.add_argument('-s', '--chunk_size', type=int, default=1000000, help='Insertion chunk size. Requires more RAM if set to higher values.')
	parser.add_argument('-d', '--drop', type=bool, default=False, help='Drops MongoDB collection if set to True.')
	parser.add_argument('-tc', '--transactions_collection', default='cell_towers', help='Name of transactions collection.')
	parser.add_argument('-ts', '--transactions_source_csv_gz', default='cell_towers.csv.gz', help='Name of transactions source.')

	args = parser.parse_args()

	print('Downloading data...')
	downloadCsv(args.transactions_source_csv_gz, args.token)
	print('Updating database...')
	updateDatabase(args.db_host, args.db_name, args.db_port, args.chunk_size, args.drop, args.transactions_collection, args.transactions_source_csv_gz)
	os.remove(args.transactions_source_csv_gz)

if __name__ == '__main__':
	main()		

