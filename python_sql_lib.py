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


