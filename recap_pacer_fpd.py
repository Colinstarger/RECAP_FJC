	#python3
import requests
import pandas as pd
from pandas import ExcelWriter #note also need to pip3 install xlrd & openpyxl
from pandas import ExcelFile
from bs4 import BeautifulSoup as bs
import json
from passwords import *
from python_sql_lib import *


#GLOBAL VARS TO HASH FJC CODES

Disp_Code_Hash ={
	0: "Rule 20(a)/21 transfer",
	1: "Dismissed by gov[1]",
	2: "Acquitted by court/JOA",
	3: "Acquitted by jury",
	4: "Guilty plea",
	5: "Guilty nolo",
	8: "Convicted by court",
	9: "Convicted by jury",
	10: "NARA Titles I and III",
	11: "Nolle prosequi",
	12: "Pretrial diversion",
	13: "Mistrial",
	14: "Statistically closed",
	15: "Dismissed w/o prejudice",
	16: "Not guilty insanity court",
	17: "Guilty/insane court",
	18: "Not guilty insanity jury",
	19: "Guilty/insance jury",
	20: "Dismissal superseded",
	21: "Reassigned from judge to magistrate",
	-8: "Missing data"
}

Prison_Time_Hash = {
	-1: "Less than month prison",
	-2: "Guilty/No sentence",
	-3: "Sealed sentence",
	-4: "Life in prison",
	-5: "Death",
	-8: "missing data"

}

#This is to deal with bad recap data and multiple defendants. There does not seem to be a pattern to just fixing the items by hand

CP_Recap_Exception_Hash = {
	
	12102991: 0,
	4954750: 0,
	11939605: 0,
	16901278:0,
	16914462:2,
	6280873: 0,
	6287198:0,
	6306179:0
}

CP_Charge_Index_Exception = {
	
	4285619: 1
}
CP_Data_Hash = {

	41: 1,
	120: 0,
	121: 1,
	125: 3, # still bad
	126: 2,
	127: 1,
	128: 0,
	192: 0, #no good one here
	248: 0, #no good one here
	302: 4,
	303: 2,
	304: 1,
	305: 3,
	306: 0,
	351: 0, #no good one here
	416: 1,
	462: 0,
	469: 0,
	477: 1,
	478: 0,
	485: 0,
	495: 0,
	497: 0,
	498: 0
	
}

CP_E_Data_Hash = {
	
	11:1,
	14:0,
	18:0,
	19:0,
	20:1,
	21:0,
	23:1,
	24:1,
	25:1,
	26:1,
	27:1,
	29:0,
	35:2,
	36:0,
	39:1,
	40:1,
	41:0,
	42:1,
	44:1,
	45:1,
	47:1,
	48:1,
	50:1,
	51:1
}


recapDefExceptHash = {
	
	"12103018003":1,
	"16922452005":1,
	"12104771006":1,
	"12103764003":1,
	"6259405001":0,
	"16922480007":2,
	"16922491001":2,
	"12982767003":1,
	"12780294002":1,
	"4286148003":1,
	"16174830003":1,
	"5269796002":1,
	"12780307004": 1,
	"6224282001": 0,
	"4512786002":1,
	"6233798001":0,
	"13363156005":1,
	"8379589002":1,
	"12104337003":2,
	"16915004007":5,
	"17004045060":7,
	"4973666002":1,
	"16299271002":1,
	"5280879003":1,
	"15915322005":1,
	"8141915003": 1
}

pacer_docket_hash = {
	"1:08-cr-00585":0,
	"1:10-cr-00498":1,
	"8:09-cr-00213":0,
	"1:09-cr-00547":0,
	"8:10-cr-00596":0,
	"1:11-cr-00600":0,
	"8:10-cr-00761":0,
	"1:13-cr-00568":0,
	"8:12-cr-00288":0,
	"1:11-cr-00696":0,
	"1:13-cr-00619":0,
	"1:15-cr-00302":0,
	"1:15-cr-00565":0,
	"1:16-cr-00416":0,
	"8:16-cr-00225":0,
	"1:14-cr-00600":0,
	"8:16-cr-00608":0,
	"8:17-cr-00364":0,
	"8:17-cr-00064":0,
	"8:05-cr-00393":0,
	"8:10-cr-00637":0,
	"1:14-cr-00109":0,
	"8:13-cr-00047":0,
	"8:14-cr-00083":0,
	"8:14-cr-00437":0,
	"1:15-cr-00261":0,
	"1:16-cr-00087":0,
	"1:16-cr-00499":0,
	"1:16-cr-00606":0,
	"1:17-cr-00106":2,
	"1:17-cr-00145":0,
	"1:18-cr-00058":0,
	"1:18-cr-00066":0,
	"1:18-cr-00271":0,
	"8:17-cr-00342":0,
	"8:17-cr-00526":0,
	"8:17-cr-00578":0,
	"8:17-cr-00408":0,
	"8:17-cr-00579":0
}

Charge_Index_Exception = {
	"4286107001":1
}
def getJSONfromAPI_Auth(api_url):
	r = requests.get(api_url, headers={'Authorization': 'Token '+cl_auth_token})
	return r.json()

def getJSONfromAPI(api_url):
	r=requests.get(api_url)
	j=r.json()
	return j

def disagg_fjc_deflogky(deflogkey):
	#print("Whole key = ", deflogkey)
	circuit = deflogkey[0:2]
	district = deflogkey[2:4]
	office = deflogkey[4:5]
	docket = deflogkey[5:12]
	casetype = deflogkey[12:14]
	defno = deflogkey[14:17]
	reopen = deflogkey[-1:]
	returnhash = {"circuit": circuit, "district": district, "office": office, "docket": docket, "casetype": casetype, "defno": defno, "reopen": reopen}
	return returnhash

def convertFJCSQL_to_Docket(fjc_office, fjc_docket):
	pacer_docket = fjc_office+":"
	year = fjc_docket[:2]
	
	pacer_docket+=year+"-cr-"
	dock_slice = fjc_docket[2:]
	extraZeros = 5-len(dock_slice)
	if (extraZeros>0):
		for x in range(extraZeros):
			dock_slice= "0"+dock_slice
	pacer_docket += dock_slice

	return pacer_docket


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


def checkDocket_in_MDD_RECAP_Fullinfo(docket):
	endpoint="https://www.courtlistener.com/api/rest/v3/dockets/?court=mdd&docket_number="+docket
	#print("Looking for this endpoint", endpoint)
	myJson=getJSONfromAPI_Auth(endpoint)
	
	if (myJson==None or myJson['count']==0):
		return ["NO"]
	else:
		count = myJson['count']
		recap_id=myJson['results'][0]['id']
		assigned_to = myJson['results'][0]['assigned_to_str']
		case_name = myJson['results'][0]['case_name']
		#return ["YES", count, recap_id, assigned_to, case_name]
		return ["YES", count, recap_id, assigned_to, case_name, myJson]


def getDefendantInfo_RECAP_DefNo(recap_docket, defNo):

	endpoint="https://www.courtlistener.com/api/rest/v3/parties/?docket="+str(recap_docket)
	#print("Looking for this endpoint", endpoint)
	myJson=getJSONfromAPI_Auth(endpoint)
	count = myJson["count"]
	results = myJson["results"]
	def_name = "missing"
	recap_top_charge = "missing"
	recap_top_disp = "missing"
	recap_total_charges = -1
	recap_total_convictions = -1

	if count>0:
		#def_index = count -1
		def_index = count - int(defNo)

		if def_index >= 20:
			#For those rare more than 20 defendant cases
			#I am going to assume there are no more than 40 defendant cases (this is a bad assumption see 8:05-cr-00393)
			print("Funky more than 20 defendant case")
			nextEndpoint = myJson["next"]
			myJson=getJSONfromAPI_Auth(nextEndpoint)
			results = myJson["results"]
			def_index = def_index-20
			print("Going to page2 and will look at index", def_index)



		#This will be the by-hand check
		defNoExcept = str(recap_docket)+defNo
		if defNoExcept in recapDefExceptHash:
			def_index = recapDefExceptHash[defNoExcept]

		if (def_index<0):
			#The algorithm didn't work
			print("Issue with recap_docket", recap_docket, "Defno", defNo, "resetting to 0!!")
			def_index=0

		#Testing
		print("Looking at def_index", def_index, "for recap_docket",recap_docket)
		def_name = results[def_index]["name"]
		

		try:
			charge_index = 0
			if (defNoExcept in Charge_Index_Exception):
				charge_index = Charge_Index_Exception[defNoExcept]
			#charges = results[def_index]["party_types"][0]["criminal_counts"]
			charges = results[def_index]["party_types"][charge_index]["criminal_counts"]
			#print("found charges", charges)
			recap_total_charges = len(charges)
			dismissed = 0
			for charge in charges:
				if (charge['disposition'].upper() in ("DISMISSED", "DISMISSED WITHOUT PREJUDICE", 'ACQUITTAL')):
					dismissed += 1
			recap_total_convictions = recap_total_charges-dismissed
			if (recap_total_convictions == recap_total_charges or recap_total_convictions==0):
				recap_top_charge= charges[-1]["name"]
				recap_top_disp = charges[-1]["disposition"]
			else:
				for charge in reversed(charges):
					if not(charge['disposition'].upper() in ("DISMISSED", "DISMISSED WITHOUT PREJUDICE", "ACQUITTAL")):
						recap_top_charge= charge["name"]
						recap_top_disp = charge["disposition"]
						break;
		except:
			print("**\nNo charges found for ", str(recap_docket)+defNo, "at def_index", def_index,"\n**")

	defInfo = {"def_name":def_name, 
				"recap_top_charge": recap_top_charge,
				"recap_top_disp": recap_top_disp,
				"recap_total_charges": recap_total_charges,
				"recap_total_convictions": recap_total_convictions
				}
	return defInfo		


def getDefendantInfo_RECAP(recap_docket):

	endpoint="https://www.courtlistener.com/api/rest/v3/parties/?docket="+str(recap_docket)
	#print("Looking for this endpoint", endpoint)
	myJson=getJSONfromAPI_Auth(endpoint)
	count = myJson["count"]
	results = myJson["results"]
	def_name = "missing"
	recap_top_charge = "missing"
	recap_top_disp = "missing"
	recap_total_charges = -1
	recap_total_convictions = -1

	if count>0:
		def_index = count -1
		if (recap_docket in CP_Recap_Exception_Hash):
			def_index = CP_Recap_Exception_Hash[recap_docket]
		def_name = results[def_index]["name"]
		#print("Found defendant", def_name)

		try:
			charge_index = 0
			if (recap_docket in CP_Charge_Index_Exception):
				charge_index = CP_Charge_Index_Exception[recap_docket]
			#charges = results[def_index]["party_types"][0]["criminal_counts"]
			charges = results[def_index]["party_types"][charge_index]["criminal_counts"]
			#print("found charges", charges)
			recap_total_charges = len(charges)
			dismissed = 0
			for charge in charges:
				if (charge['disposition'].upper() in ("DISMISSED", "DISMISSED WITHOUT PREJUDICE", 'ACQUITTAL')):
					dismissed += 1
			recap_total_convictions = recap_total_charges-dismissed
			if (recap_total_convictions == recap_total_charges or recap_total_convictions==0):
				recap_top_charge= charges[-1]["name"]
				recap_top_disp = charges[-1]["disposition"]
			else:
				for charge in reversed(charges):
					if not(charge['disposition'].upper() in ("DISMISSED", "DISMISSED WITHOUT PREJUDICE", "ACQUITTAL")):
						recap_top_charge= charge["name"]
						recap_top_disp = charge["disposition"]
						break;
		except:
			print("No charges found")

	defInfo = {"def_name":def_name, 
				"recap_top_charge": recap_top_charge,
				"recap_top_disp": recap_top_disp,
				"recap_total_charges": recap_total_charges,
				"recap_total_convictions": recap_total_convictions
				}
	return defInfo		


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

def fetch_to_RECAP_PACERID(pacer_id, court="mdd"):
	api_url= "https://www.courtlistener.com/api/rest/v3/recap-fetch/"
	data = {'request_type':1, 'pacer_username': pacer_username, 'pacer_password':pacer_password, 'pacer_case_id':pacer_id, 'court':court, 'show_parties_and_counsel': 'True', "show_terminated_parties": "True"}
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
	#This is a totally unnecessary function now that API is in place
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

def getChild_RECAP_Attorneys_Row(row):
	return(getLeadAttorneys(row["recap_id"])) 

def getChild_RECAP_Row(row):
	#This was my first pass at this and this is ONLY FOR CHILD data
	#print ("looking at", row.name, "with key", row["def_key"])
	index = row.name
	key = row["def_key"]
	disag = disagg_fjc_deflogky(key)
	defno=disag["defno"]
	pacer_docket = convertFJCSQL_to_Docket(disag['office'], disag['docket'])
	recap_info = checkDocket_in_MDD_RECAP_Fullinfo(pacer_docket)
	if (recap_info[0]=="YES"):
		count = recap_info[1]
		defInfo="" #will get defined in the function

		if (count>1):
			#This was testing - I still think the best is to use latest but its hard
			#print("buddy - you got to CP_Data_Hash this index", index, "it has", count, "rows", pacer_docket)
			#print("FJC info - top convict", row["top_convict"], "prison_total", row["prison_total"])
			#print("scour this buddy")
			#print("https://www.courtlistener.com/api/rest/v3/dockets?court=mdd&docket_number="+pacer_docket)

			#This was for J
			#hash_results = recap_info[5]['results'][CP_Data_Hash[index]]
			hash_results = recap_info[5]['results'][CP_E_Data_Hash[index]]
			recap_id = hash_results['id']
			assigned_to = hash_results['assigned_to_str']
			case_name = hash_results['case_name']
			defInfo = getDefendantInfo_RECAP(recap_id)
			docket_link = "www.courtlistener.com"+hash_results['absolute_url']
			
		else: 
			recap_id = recap_info[2]
			assigned_to = recap_info[3]
			case_name = recap_info[4]
			defInfo = getDefendantInfo_RECAP(recap_id)
			docket_link = "www.courtlistener.com"+recap_info[5]['results'][0]['absolute_url']
						
		print("Index", index, "FJC def no", defno)
		#print(pacer_docket, recap_id, case_name, assigned_to)
		#print( "RCP Num charges", defInfo['recap_total_charges'], "Convictions", defInfo['recap_total_convictions'], defInfo['recap_top_charge'], defInfo['recap_top_disp'], "\n")

		#DISABLE DURING TESTING
		result_list =[pacer_docket, recap_id, assigned_to, case_name, defInfo['def_name'], defInfo['recap_total_charges'],  defInfo['recap_total_convictions'], defInfo['recap_top_charge'], defInfo['recap_top_disp'], docket_link]
		return result_list

	else:
		#print("Pacer docket", pacer_docket, "not found in RECAP")	

		result_list =[pacer_docket, "nir", "nir", "nir", "nir", "nir",  "nir", "nir", "nir", "nir"]
		return result_list

def getChild_RECAP_Row_Generic(row):
	#Second pass at this -- will try to make generic for wirefraud and bank robbery
	
	#Try to avoid using the index since that is too dependent on the data set
	#print ("looking at", row.name, "with key", row["def_key"])
	#index = row.name
	
	key = row["def_key"]
	disag = disagg_fjc_deflogky(key)
	defno=disag["defno"]
	pacer_docket = convertFJCSQL_to_Docket(disag['office'], disag['docket'])
	recap_info = checkDocket_in_MDD_RECAP_Fullinfo(pacer_docket)
	if (recap_info[0]=="YES"):
		count = recap_info[1]
		defInfo="" #will get defined in the function

		if (count>1):
			#Testing - will use latest for now but then must check
			print("Warning multiple results found for", pacer_docket, "with FJC info - top convict", row["top_convict"], "prison_total", row["prison_total"])
			print("https://www.courtlistener.com/api/rest/v3/dockets?court=mdd&docket_number="+pacer_docket)

			#In child used an index-based hash. Shift to pacer_docket hash
			result_index = count-1 #using this as default
			if (pacer_docket in pacer_docket_hash):
				result_index = pacer_docket_hash[pacer_docket]
			
			#hash_results = recap_info[5]['results'][CP_E_Data_Hash[index]]
			hash_results = recap_info[5]['results'][result_index]
			recap_id = hash_results['id']
			assigned_to = hash_results['assigned_to_str']
			case_name = hash_results['case_name']
			defInfo = getDefendantInfo_RECAP_DefNo(recap_id, defno)
			docket_link = "www.courtlistener.com"+hash_results['absolute_url']
			
		else:
			#This is assuming a single result 
			recap_id = recap_info[2]
			assigned_to = recap_info[3]
			case_name = recap_info[4]
			defInfo = getDefendantInfo_RECAP_DefNo(recap_id, defno)
			docket_link = "www.courtlistener.com"+recap_info[5]['results'][0]['absolute_url']
						
		print("Searched", pacer_docket, "FJC def no", defno)
		#print(pacer_docket, recap_id, case_name, assigned_to)
		#print( "RCP Num charges", defInfo['recap_total_charges'], "Convictions", defInfo['recap_total_convictions'], defInfo['recap_top_charge'], defInfo['recap_top_disp'], "\n")

		#DISABLE DURING TESTING
		result_list =[pacer_docket, recap_id, assigned_to, case_name, defInfo['def_name'], defInfo['recap_total_charges'],  defInfo['recap_total_convictions'], defInfo['recap_top_charge'], defInfo['recap_top_disp'], docket_link]
		return result_list

	else:
		#print("Pacer docket", pacer_docket, "not found in RECAP")	

		result_list =[pacer_docket, "nir", "nir", "nir", "nir", "nir",  "nir", "nir", "nir", "nir"]
		return result_list
	

def FJC_prison_hash(row):
	prison_total = row["prison_total"]
	if prison_total < 0:
		prison_total = Prison_Time_Hash[prison_total]
	return prison_total

def FJC_disp_hash(row):
	return(Disp_Code_Hash[row["top_disp"]])

def getLeadAttorneys(recap_docket):
	#first get the parties
	endpoint="https://www.courtlistener.com/api/rest/v3/parties/?docket="+str(recap_docket)
	myJson=getJSONfromAPI_Auth(endpoint)
	
	lead_defense = "not found"
	lead_prosecutor = "not found"

	if myJson==None:
		print("No Json for recap_docket", recap_docket)
		return ([lead_prosecutor, lead_defense])

	results = myJson["results"]

	#plaintiff aka prosecution first
	
	for result in results:
		if (result["party_types"][0]["name"]=="Plaintiff"):
			attorneys = result["attorneys"]
			for attorney in reversed(attorneys):
				if (attorney['role']==2):
					#found lead, now get name
					atty_endpoint = attorney["attorney"]
					lead_prosecutor = getJSONfromAPI_Auth(atty_endpoint)["name"]
					break
			break
	#print("Found lead prosecutor", lead_prosecutor)
	
	for result in results:
		if (result["party_types"][0]["name"]=="Defendant"):
			attorneys = result["attorneys"]
			for attorney in reversed(attorneys):
				if (attorney['role']==2):
					#found lead, now get name
					atty_endpoint = attorney["attorney"]
					lead_defense = getJSONfromAPI_Auth(atty_endpoint)["name"]
					break
			break
	#print("Found lead defense", lead_defense)
	if (lead_defense=="not found" and lead_prosecutor!="not found"):
		for result in results:
			if (result["party_types"][0]["name"]=="Defendant"):
				attorneys = result["attorneys"]
				for attorney in reversed(attorneys):
					if (attorney['role']==6):
					#found lead, now get name
						atty_endpoint = attorney["attorney"]
						lead_defense = getJSONfromAPI_Auth(atty_endpoint)["name"]
						break
				break

	if (lead_defense=="not found" and lead_prosecutor!="not found"):
		for result in results:
			if (result["party_types"][0]["name"]=="Defendant"):
				attorneys = result["attorneys"]
				for attorney in attorneys:
					if (attorney['role']==1):
					#found lead, now get name
						atty_endpoint = attorney["attorney"]
						lead_defense = getJSONfromAPI_Auth(atty_endpoint)["name"]
						break
				break

	print("Recap Docket ", recap_docket, "prosecutor=", lead_prosecutor, "defense_lead=", lead_defense)
	return ([lead_prosecutor, lead_defense])


def addProsecutorDefense(loadfile="child_exploit_v2.0.csv", outputfile="child_exploit_v2.1.csv"):

	resultDF = pd.read_csv(loadfile)
	#just make it a few less as I develop the logic
	start = 0
	end =  len(resultDF)
	resultDF = resultDF[start:end]

	temp = resultDF.apply(getChild_RECAP_Attorneys_Row, axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)

	resultDF['prosecutor_lead'] = temp2[0]
	resultDF['defense_lead'] = temp2[1]

	resultDF.to_csv(outputfile, index=False)


def create_child_master():

	fjc_db = openIDB_connection()
	#sql_file = "child_exploit_key_j.sql"
	sql_file = "child_exploit_key_e.sql"
	sql= open(sql_file).read()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()

	#just make it a few less as I develop the logic
	print("This many results", len(resultDF))
	start = 0
	end =  len(resultDF)
	resultDF = resultDF[start:end]
	resultDF["prison_total"] = resultDF.apply(FJC_prison_hash, axis=1)
	resultDF["top_disp"]= resultDF.apply(FJC_disp_hash, axis=1)


	#Kludgy but it works - get results back as a DF and then apply to resultsDC
	temp = resultDF.apply(getChild_RECAP_Row, axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)

	resultDF['pacer_docket'] = temp2[0]
	resultDF['recap_id'] = temp2[1]
	resultDF['assigned_to']=temp2[2]
	resultDF['case_name']=temp2[3]
	resultDF['def_name']=temp2[4]
	resultDF['r_charges_total']=temp2[5]
	resultDF['r_convictions_total']=temp2[6]
	resultDF['r_top_charge']=temp2[7]
	resultDF['r_top_disp']=temp2[8]
	resultDF['r_docket_link']=temp2[9]

	#result_list =[pacer_docket, recap_id, assigned_to, case_name, defInfo['def_name'], defInfo['recap_total_charges'],  defInfo['recap_total_convictions'], defInfo['recap_top_charge'], defInfo['recap_top_disp']]

	#Save Results
	#resultDF = reorderColumns(resultDF)
	resultDF.to_csv("child_exploit_E.v1.0.csv", index=False)


def create_master_generic(sql_file, outputfile, start=0, end=-999):
	fjc_db = openIDB_connection()

	sql= open(sql_file).read()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()

	#Option to narrow results during development
	print("*****\n*****\n")
	print("This many results", len(resultDF))
	# start = This comes in from function now
	if end ==-999:
		end =  len(resultDF) 
		#This is just a special case

	resultDF = resultDF[start:end]
	resultDF["prison_total"] = resultDF.apply(FJC_prison_hash, axis=1)
	resultDF["top_disp"]= resultDF.apply(FJC_disp_hash, axis=1)
	
	#Kludgy but it works - get results back as a DF and then apply to resultsDC
	temp = resultDF.apply(getChild_RECAP_Row_Generic, axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)

	resultDF['pacer_docket'] = temp2[0]
	resultDF['recap_id'] = temp2[1]
	resultDF['assigned_to']=temp2[2]
	resultDF['case_name']=temp2[3]
	resultDF['def_name']=temp2[4]
	resultDF['r_charges_total']=temp2[5]
	resultDF['r_convictions_total']=temp2[6]
	resultDF['r_top_charge']=temp2[7]
	resultDF['r_top_disp']=temp2[8]
	resultDF['r_docket_link']=temp2[9]
	
	#resultDF.to_csv("wirefraud.v2.0.csv", index=False)
	resultDF.to_csv(outputfile, index=False)


def create_wirefraud_master():
	sql_file = "wirefraud_key.sql"
	outputfile="wirefraud.v2.0.csv"
	#create_master_generic(sql_file, outputfile)
	#addProsecutorDefense("wirefraud.v2.0.csv", "wirefraud.v2.1.csv")
	finalDF = pd.read_csv("wirefraud.v2.1.csv")
	finalDF = reorderColumns(finalDF)
	finalDF.to_csv("wirefraud.v2.2.csv", index=False)

	
def create_bankrobbery_master():
	sql_file = "bank_robbery_key.sql"
	outputfile="bank_robbery.v2.0.csv"
	#create_master_generic(sql_file, outputfile, 700, -999)
	#create_master_generic(sql_file, outputfile)
	#addProsecutorDefense("bank_robbery.v2.0.csv", "bank_robbery.v2.1.csv")
	finalDF = pd.read_csv("bank_robbery.v2.1.csv")
	finalDF = reorderColumns(finalDF)
	finalDF.to_csv("bank_robbery.v2.2.csv", index=False)


def reorderColumns(inputDF):

	#def_key	top_charge	top_disp	top_convict	prison_total	file_date	disp_date	pacer_docket	recap_id	assigned_to	case_name	def_name	r_charges_total	r_convictions_total	r_top_charge	r_top_disp	r_docket_link	prosecutor_lead	defense_lead

	outputDF = ""
	numCols = len(inputDF.columns)

	if (numCols==19):
		outputDF=inputDF[["case_name", "def_name", "pacer_docket", "assigned_to", "prosecutor_lead", "defense_lead", "file_date", "top_charge", "r_top_charge", "r_charges_total", "r_convictions_total", "disp_date", "top_disp", "top_convict", "r_top_disp", "prison_total", "r_docket_link", "recap_id", "def_key"]]
	elif (numCols==20):
		outputDF=inputDF[["case_name", "def_name", "pacer_docket", "assigned_to", "prosecutor_lead", "defense_lead", "file_date", "top_charge", "r_top_charge", "r_charges_total", "r_convictions_total", "fjc_status", "disp_date", "top_disp", "top_convict", "r_top_disp", "prison_total", "r_docket_link", "recap_id", "def_key"]]
	else:
		print(numCols, "is not a state I can deal with")
	return (outputDF)

def test_get_child_keys():

	#This function is not used anymore -- instead using Pandas rows
	#See create_child_master() - Just keeping to remind me how to loop through in
	#non-pythonic but easy to understand way

	fjc_db = openIDB_connection()
	sql_file = "child_exploit_key_j.sql"
	sql= open(sql_file).read()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()

	#just make it a few less as I develop the logic
	print("This is how many pending cases there are", len(resultDF))
	start = 9
	end =  12 #len(resultDF)
	resultDF = resultDF[start:end]

	#This is a goofy non-Pythonic way of looping through
	#Just doing for development; will put into Pandas row function when ready

	for x in range(start, end):
		file_date = resultDF.at[x,"file_date"]
		disp_date = resultDF.at[x,"disp_date"]
		top_charge = resultDF.at[x,"top_charge"]
		top_disp = Disp_Code_Hash[resultDF.at[x,"top_disp"]]
		top_convict =  resultDF.at[x,"top_convict"]
		prison_total = resultDF.at[x,"prison_total"]
		if prison_total < 0:
			prison_total = Prison_Time_Hash[prison_total]

		key = resultDF.at[x,"def_key"]
		disag = disagg_fjc_deflogky(key)
		#print(disag)
		defno = disag['defno']
		pacer_docket = convertFJCSQL_to_Docket(disag['office'], disag['docket'])
		recap_info = checkDocket_in_MDD_RECAP_Fullinfo(pacer_docket)
		if (recap_info[0]=="YES"):
			count = recap_info[1]
			defInfo="" #will get defined in the function

			if (count>1):
				hash_results = recap_info[5]['results'][CP_Data_Hash[x]]
				recap_id = hash_results['id']
				assigned_to = hash_results['assigned_to_str']
				case_name = hash_results['case_name']
				defInfo = getDefendantInfo_RECAP(recap_id)
			else: 
				recap_id = recap_info[2]
				assigned_to = recap_info[3]
				case_name = recap_info[4]
				defInfo = getDefendantInfo_RECAP(recap_id)
						
			print("FJC def no", defno, file_date, disp_date, top_charge, top_disp, top_convict, prison_total)
			print(pacer_docket, recap_id, case_name, assigned_to)
			print( "RCP Num charges", defInfo['recap_total_charges'], "Convictions", defInfo['recap_total_convictions'], defInfo['recap_top_charge'], defInfo['recap_top_disp'], "\n")

		else:
			print("Pacer docket", pacer_docket, "not found in RECAP")	


def addNickColumns():
	#This is specific to the child data set
	#First Load up Excel Sheet
	mainDF = pd.read_excel('child_exploit_v3.0.xlsx', sheet_name='Main')
	mainDF["file_date"]=mainDF["file_date"].dt.date
	mainDF["disp_date"]=mainDF["disp_date"].dt.date
	mainDF["Minor victim"]=""
	mainDF["Num victims"]=""
	mainDF["Victim ages"]=""
	mainDF["Sexual contact"]=""
	mainDF["Plea link"]=""

	#Don't have a key in common will just do by hand - below was from when I thought I had key
	#nickDF = pd.read_excel('CHILD_EXPLOITATION.xlsx', sheet_name='DOCS')
	#subDF = nickDF[nickDF["Minor Victim (Y/N)"] == ("Y" or "N")]

	mainDF.to_excel("child_exploit_v3.0.xlsx", index=False)



def main():

	#These create RECAP lists with dockets from a FJC list TARGET FILE to an OUTPUT FILE
	#All the target files are created by SQL Queries to FJC DB
	#target_file="RESULTS/child_exploitation_results.csv"
	#outputfile="RESULTS/child_recap_results.csv"
	#target_file="bank_robbery_results.csv"
	#outputfile="bank_robbery_recap_results.csv"
	#target_file="wirefraud_results.csv"
	#outputfile="wirefraud_recap_results.csv"
	#createRecapCheckList(target_file, outputfile)

	#checkList_Fetch_Missing go through a checklist created using CreateRecapCheckList
	#And get the missing items from PACER
	#When done, just re-run 
	#target_file = "child_exploitation_recap.csv"
	#target_file = "bank_robbery_recap.csv"
	#target_file = "wirefraud_recap.csv"
	#checkList_Fetch_Missing(target_file)


	#Child Exploit - Was Round one so extra kludgey
	#create_child_master()
	#getLeadAttorneys(16914442)
	#addProsecutorDefense("child_exploit_E.v1.0.csv", "child_exploit_E.v1.1.csv")
	#myDF= pd.read_csv("child_exploit_E.v1.1.csv")
	#myDF = reorderColumns(myDF)
	#myDF.to_csv("child_exploit_E.v1.3.csv", index=False)
	#closedDF = pd.read_csv("child_exploit_v2.2.csv")
	#print("Len of closed = ", len(closedDF), "with", len(closedDF.columns), "columns")
	#closedDF["fjc_status"] = "Closed"
	#pendingDF = pd.read_csv("child_exploit_E.v1.3.csv")
	#pendingDF["fjc_status"] = "Pending"
	#pendingDF.index += 502
	#concat_list = [closedDF, pendingDF]
	#finalDF = pd.concat(concat_list)
	#print("Len of final = ", len(finalDF), "with", len(finalDF.columns), "columns")
	#finalDF = reorderColumns(finalDF)
	#finalDF.to_csv("child_exploit_v3.0.csv", index=False)
	#addNickColumns()

	# WIREFRAUD - Round 2 is cleaner
	#create_wirefraud_master()

	# Bank Robbery - Round 3 hopefully runs smoothest
	create_bankrobbery_master()

	#Use this to get Doppelgangers
	#print(fetch_to_RECAP_PACERID("405924"))

# CALL MAIN
main()