#python3
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from passwords import *

def getJSONfromAPI_Auth(api_url):
	r = requests.get(api_url, headers={'Authorization': 'Token '+cl_auth_token})
	return r.json()

def getJSONfromAPI(api_url):
	r=requests.get(api_url)
	j=r.json()
	return j

def convertFJC_to_Docket(fjc_office, fjc_docket):
	pacer_docket = fjc_office+":"
	year = fjc_docket[:2]
	if (year[0]!= "1"):
		#reverse
		year = year[::-1]

	pacer_docket+=year+"-cr-"
	dock_slice = fjc_docket[2:]
	extraZeros = 5-len(dock_slice)
	if (extraZeros>0):
		for x in range(extraZeros):
			dock_slice= "0"+dock_slice
	pacer_docket += dock_slice

	return pacer_docket

def getMDD_Docket(row):
	office= str(row['OFFICE']) 
	docket= str(row['DOCKET'])
	return(convertFJC_to_Docket(office, docket))

def getDocket_in_RECAP(row):
	return(checkDocket_in_RECAP(row['MDD_DOCKET']))

def getDocket_in_RECAP_Fullinfo(row):
	return(checkDocket_in_RECAP_Fullinfo(row['MDD_DOCKET']))

def convertFJC_to_PacerDocket(FJC_list):
	fjc_office = FJC_list[0]
	fjc_docket = FJC_list[1]
	return(convertFJC_to_Docket(fjc_office, fjc_docket))

def checkDocket_in_RECAP(docket):
	endpoint="https://www.courtlistener.com/api/rest/v3/dockets/?court=mdd&docket_number="+docket
	print("Looking for this endpoint", endpoint)
	myJson=getJSONfromAPI_Auth(endpoint)
	results=""
	if (myJson==None or myJson['count']==0):
		return "NO"
	else:
		return "YES"

def checkDocket_in_RECAP_Fullinfo(docket):
	endpoint="https://www.courtlistener.com/api/rest/v3/dockets/?court=mdd&docket_number="+docket
	print("Looking for this endpoint", endpoint)
	myJson=getJSONfromAPI_Auth(endpoint)
	
	if (myJson==None or myJson['count']==0):
		return ["NO", "", ""]
	else:
		recap_id=myJson['results'][0]['id']
		recap_url="https://www.courtlistener.com"+myJson['results'][0]['absolute_url']
		return ["YES", recap_id, recap_url]


def createRecapCheckList(inputfile, outputfile="recap_results.csv", slice_start=0, slice_end=0):

	inDF = pd.read_csv(inputfile)
	outDF = inDF[['OFFICE', 'DOCKET']]
	outDF = outDF.drop_duplicates()

	if (slice_end>0):
		outDF=outDF[slice_start:slice_end]

	outDF["MDD_DOCKET"]=outDF.apply(getMDD_Docket,axis=1)
	outDF["RECAP"]="NO"
	outDF["RECAP_ID"]=""
	outDF["LINK"]=""
	
	temp = outDF.apply(getDocket_in_RECAP_Fullinfo, axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)
	outDF['RECAP'] = temp2[0]
	outDF['RECAP_ID'] = temp2[1]
	outDF['LINK']=temp2[2]

	outDF.to_csv(outputfile)

def fetch_to_RECAP(docket_id, court="mdd"):
	api_url= "https://www.courtlistener.com/api/rest/v3/recap-fetch/"
	data = {'request_type':1, 'pacer_username': pacer_username, 'pacer_password':pacer_password, 'docket_number':docket_id, 'court':court, 'show_parties_and_counsel': 'True', "show_terminated_parties": "True"}
	r = requests.post(api_url, data=data, headers={'Authorization': 'Token '+cl_auth_token})
	return r.json()

def checkRow_Fetch_Missing(row):
	return(checkDocket_Fetch_Missing(row['MDD_DOCKET']))

def checkDocket_Fetch_Missing(docket):
	if (checkDocket_in_RECAP(docket)=="YES"):
		print(docket, "already in RECAP")
	else:
		print("Fetching", docket)
		fetch_to_RECAP(docket)
	return True

def checkList_Fetch_Missing(csvfile, slice_start=0, slice_end=0):
	checkDF = pd.read_csv(csvfile)

	#Take subslice if don't want to go nuts
	if (slice_end>0):
		checkDF=checkDF[slice_start:slice_end]
	checkDF.apply(checkRow_Fetch_Missing, axis=1)
	return True
		

def scrapeCharges(partyURL):

	page = requests.get(partyURL)
	soup = bs(page.content, 'html.parser')
	table = soup.find('table')
	#print(table.prettify())
	rows = table.find_all('tr')

	output_table = []

	for index, row in enumerate(rows):
		newrow=[]
		if (index==0):
			headers = row.find_all('th')
			for header in headers:
				text = header.get_text()
				if (text==''):
					text='Type'
				newrow.append(text)
		else:
			cells = row.find_all('td')
			for cell in cells:
				newrow.append(cell.get_text())
		output_table.append(newrow)

	print("RESULTS TABLE\n", output_table)
	return(output_table)



def main():

	#Proof of concept check docket
	#docket="1:05-cr-00232"
	#print(docket, "in RECAP=", checkDocket_in_RECAP(docket))

	#These create RECAP lists with dockets from a FJC list TARGET FILE to an OUTPUT FILE
	#All the target files are created by SQL Queries to FJC DB
	target_file="RESULTS/child_exploitation_results.csv"
	outputfile="RESULTS/child_recap_results.csv"
	#target_file="bank_robbery_results.csv"
	#outputfile="bank_robbery_recap_results.csv"
	#target_file="wirefraud_results.csv"
	#outputfile="wirefraud_recap_results.csv"
	createRecapCheckList(target_file, outputfile)


	#checkList_Fetch_Missing go through a checklist created using CreateRecapCheckList
	#And get the missing items from PACER
	#When done, just re-run 
	#target_file = "child_exploitation_recap.csv"
	#target_file = "bank_robbery_recap.csv"
	#target_file = "wirefraud_recap.csv"
	#checkList_Fetch_Missing(target_file)


	#SCRAPER TEST - Getting Parties
	#testURL = "https://www.courtlistener.com/docket/16901415/parties/united-states-v-koontz/"
	#testURL = "https://www.courtlistener.com/docket/4937257/parties/united-states-v-holiday/"
	#returnTable = scrapeCharges(testURL)
	#testDF = pd.DataFrame(returnTable[1:], columns=returnTable[0])
	#print(testDF)



# CALL MAIN
main()