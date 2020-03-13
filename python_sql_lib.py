#python3

import pandas as pd #pip3 install Pandas
import mysql.connector #pip3 install mysql-connector-python
from passwords import * 

def executeQuery_returnDF(dbconnection, sqlquery):
	dbcursor = dbconnection.cursor(dictionary=True)
	dbcursor.execute(sqlquery)
	results = dbcursor.fetchall()
	dbcursor.close()
	df = pd.DataFrame(results)
	return df


def excuteGenericFileQueryExportResults(dbconnection, sql_file, output_file):
	#This excutes a generic query from a SQL file and exports to a csv
	sqlStr = open(sql_file).read()
	dbcursor = dbconnection.cursor(dictionary=True)
	dbcursor.execute(sqlStr)
	results = dbcursor.fetchall()
	dbcursor.close()
	df = pd.DataFrame(results)
	df.to_csv(output_file)
	

def createCircuitMegaPandasGeneric(county, sql_file, output_file, begin_year, end_year, num_date_queries=1):

	CIRCUIT = circuit_db[county]
	dbconnection = mysql.connector.connect( host=hostname, user=username, passwd=password, db=CIRCUIT)
	
	begin = datetime.datetime(begin_year,1,1)
	end = datetime.datetime(end_year, 12, 31)
	print("Excuting making circuit mega Pandas for", output_file)

	output_path = "../NewData/"
	output_file = output_path + output_file

	sqlStr = open(sql_file).read()
	print("Opened SQL file", sql_file)
	#dbcursor = myCircuitConnection.cursor(dictionary=True)
	dbcursor = dbconnection.cursor(dictionary=True)
	
	#Just going to do this ugly
	if (num_date_queries==1):
		dbcursor.execute(sqlStr, (begin, end))
	elif (num_date_queries==2):
		dbcursor.execute(sqlStr, (begin, end, begin, end))
	elif (num_date_queries==3):
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end))
	elif (num_date_queries==4):
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end, begin, end))
	else:
		#assume 5 is limit!
		dbcursor.execute(sqlStr, (begin, end, begin, end, begin, end, begin, end, begin, end))
	print("Executed SQL")
	results = dbcursor.fetchall()
	print("Fetched all")
	dbcursor.close()
	df = pd.DataFrame(results)
	print("Converted to dataframe")
	df.to_csv(output_file)

	dbconnection.close()


def openIDB_connection():
	dbconnection = mysql.connector.connect( host=fjc_host, user=fjc_colin, passwd=fjc_colin_password, db=fjc_db)
	return dbconnection

def old_main():
	dbconnection = mysql.connector.connect( host=fjc_host, user=fjc_colin, passwd=fjc_colin_password, db=fjc_db)
#	sql_file = "select_child_exploitation_cases.sql"
#	output_file = "RESULTS/child_exploit_results_compare.csv"
#	excuteGenericFileQueryExportResults(dbconnection, sql_file, output_file)

	sql_file = "child_exploit_key_j.sql"
	sql= open(sql_file).read()
	resultDF = executeQuery_returnDF(dbconnection, sql)
	print(resultDF)
	dbconnection.close()


