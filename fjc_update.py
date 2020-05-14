#Update Functions
#Python3

import csv

my_path = "/Users/colinstarger/Downloads/LDDC_Temp/"


def makeDict(file_name):

	fullfile = my_path+file_name
	result = {}
	with open(fullfile) as csvDataFile:	
		csvReader = csv.reader(csvDataFile)
    	#Skip header row
		next(csvReader)
    	
		for row in csvReader:
			result[row[0]] = row[1]
    
	return result

def checkDicts(newDict, oldDict):

	result = {}
	
	#New will always have more
	for key, value in newDict.items():
		if not(key in oldDict):
			print("Found new entry", key, value)
			result[key]="new"
		elif (value!=oldDict[key]):
			print("Found updated entry", key, value)
			result[key]="update"

	return result

def compare_save():

#	old_file = "wirefraud_old.csv"
#	new_file = "wirefraud_new.csv"
#	save_file = "wirefraud_update.csv"

#	old_file = "bank_old.csv"
#	new_file = "bank_new.csv"
#	save_file = "bank_update.csv"

	old_file = "ce_old.csv"
	new_file = "ce_new.csv"
	save_file = "ce_update.csv"

	old_dict = makeDict(old_file)
	new_dict = makeDict(new_file)
	result= checkDicts(new_dict, old_dict)

	save_file = my_path+save_file

	with open(save_file, 'w') as csvfile:
		
		csvfile.write("%s,%s\n"%("def_key", "fisc_yr"))
		for key, value in result.items():
			csvfile.write("%s,%s\n"%(key, value))


#Call Main or whatever
compare_save()
