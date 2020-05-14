# Functions for interacting with FJC DB, Pacer and Recap 

#python3
import requests
import pandas as pd
from pandas import ExcelWriter #note also need to pip3 install xlrd & openpyxl
from pandas import ExcelFile
from bs4 import BeautifulSoup as bs
import json
from passwords import *
from python_sql_lib import *
import csv
import datetime


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

#MD Specific
judgeHash = {
	
	"Richard D. Bennett":"RDB",
	"Catherine C. Blake":"CCB",
	"James K. Bredar":"JKB",
	"Deborah K. Chasanow":"DKC",
	"Theodore D. Chuang":"TDC",
	"Andre M. Davis":"AMD",
	"Stephanie A. Gallagher":"SAG",
	"Marvin J. Garbis": "MJG",
	"Paul W. Grimm": "PWG",
	"George Jarrod Hazel": "GJH",
	"Ellen L. Hollander": "ELH",
	"Thomas E. Johnston": "TEJ",
	"Benson Everett Legg": "BEL",
	"Peter J. Messitte": "PJM",
	"Frederick Motz": "JFM",
	"William M. Nickerson":"WMN",
	"William D. Quarles Jr.":"WDQ",
	"George Levi Russell III":"GLR",
	"Roger W. Titus": "RWT",
	"Alexander Williams Jr.": "AW",
	"Paula Xinis": "PX"

}

#This is to deal with bad recap data and multiple defendants. There does not seem to be a pattern to just fixing the items by hand

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
	"8141915003": 1,
	"12102991001":0,
	"4954750001": 0,
	"11939605001": 0,
	"16901278001":0,
	"16914462001":2,
	"16832158002":1,
	"4286283002":1,
	"6280873001":0,
	"6287198001":0,
	"6306179001":0,
	"6252287001":0,
	"7419258002":0,
	"17074074004":0,
	"13471711001":2,
	"13471711002":3
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
	"8:17-cr-00579":0,
	"1:09-cr-00271":0,
	"1:09-cr-00512":0,
	"1:15-cr-00462":0,
	"1:18-cr-00471":0,
	"1:18-cr-00271":1,
	"8:17-cr-00578":1,
	"8:18-cr-00216":0,
	"1:19-cr-00144":1,
	"8:14-cr-00437":2,
	"8:17-cr-00267":0,
	"8:18-cr-00038":0,
	"8:13-cr-00558":0,
	"1:13-cr-00229":0
}

Charge_Index_Exception = {
	"4286107001":1,
	"4285619001":1,
	"4286294001":2
}

# RECAP API functions

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
		
def getChild_RECAP_Attorneys_Row_DefNo(row):
	return(getLeadAttorneys_defNo(row["recap_id"], row['def_key'][-2:-1])) 


def getChild_RECAP_Attorneys_Row(row):
	return(getLeadAttorneys(row["recap_id"])) 

def get_CE_FJC_Info(row):

	found = False
	top_ce_charge = "not found"
	top_ce_charge_no = "NA"
	ce_charge_disp = "NA"
	ce_charge_prison = "NA"
	ce_charge_sup_release = "NA"
	
	for t in range (1,6):

		ttitle = "TTITLE"+str(t)
		if row[ttitle] in ["18:1591*", "18:2251*", "18:2252*", "18:2422*"]:
			found = True
			top_ce_charge = row[ttitle]
			top_ce_charge_no = t
			ce_charge_disp = row["DISP"+str(t)]
			ce_charge_disp = Disp_Code_Hash[ce_charge_disp]
			ce_charge_prison = row["PRISTIM"+str(t)]
			if ce_charge_prison < 0:
				ce_charge_prison = Prison_Time_Hash[ce_charge_prison]
			ce_charge_sup_release = row["SUPVREL"+str(t)]
			if (ce_charge_sup_release==-1 or ce_charge_sup_release==999):
				ce_charge_sup_release="life"
			break

	if not(found):
		print("MAYBE SHOULDN'T INCLUDE THIS ROW")

	result_list =[top_ce_charge, top_ce_charge_no, ce_charge_disp, ce_charge_prison, ce_charge_sup_release]
	return (result_list)



def getChild_RECAP_Row_Generic(row):
	
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
			print("Warning multiple results found for", pacer_docket, "with FJC info - prison_total", row["prison_total"])
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
	
def FJC_clean_title(title):
	
	if title=="-8":
		return "-8"
	
	clean = title[:7]
	if clean[-1].isdigit():
		#Have to add a '*' because of some Pandas DF coercion
		return (str(clean)+"*")
		
	else:
		#Have to add a '*' because of some Pandas DF coercion
		return (str(clean[:-1])+"*")


def FJC_prison_hash(row):
	prison_total = row["prison_total"]
	if prison_total < 0:
		prison_total = Prison_Time_Hash[prison_total]
	return prison_total

def FJC_disp_hash(row):
	return(Disp_Code_Hash[row["top_disp"]])


def getLeadAttorneys_defNo(recap_docket, defNo):

	print("Looking for docket", recap_docket, "and defNo",defNo)
	endpoint="https://www.courtlistener.com/api/rest/v3/parties/?docket="+str(recap_docket)
	myJson=getJSONfromAPI_Auth(endpoint)
	
	lead_defense = "not found"
	lead_prosecutor = "not found"
	role_order = [2, 1, 10]

	if myJson==None:
		print("No Json for recap_docket", recap_docket)
		return ([lead_prosecutor, lead_defense])

	count = myJson["count"]
	results = myJson["results"]

	#prosecutor. First find right results
	pros_attorneys=[]
	for result in results:
		if result['name']=="USA":
			pros_attorneys=result['attorneys']
			break;

	found = False
	for role in role_order:
		for attorney in reversed(pros_attorneys):
			if attorney['role']==role:
				found= True
				atty_endpoint = attorney["attorney"]
				lead_prosecutor = getJSONfromAPI_Auth(atty_endpoint)["name"]
				break
		if found:
			break
	if not(found):
		print("Hmmm didn't find prosecutor")

	#Here I am assuming clear data - have results etc

	def_index = count - int(defNo)
	if def_index >= 20:
		#For those rare more than 20 defendant cases
		#I am going to assume there are no more than 40 defendant cases (this is a bad assumption see 8:05-cr-00393)
		print("Funky more than 20 defendant case")
		nextEndpoint = myJson["next"]
		myJson=getJSONfromAPI_Auth(nextEndpoint)
		results = myJson["results"]
		def_index = def_index-20

	defNoExcept = str(recap_docket)+defNo
	if defNoExcept in recapDefExceptHash:
		def_index = recapDefExceptHash[defNoExcept]
	def_attorneys = results[def_index]["attorneys"]
	
	
	found = False

	for role in role_order:
		for attorney in reversed(def_attorneys):
			if attorney['role']==role:
				found= True
				atty_endpoint = attorney["attorney"]
				lead_defense = getJSONfromAPI_Auth(atty_endpoint)["name"]
				break
		if found:
			break
	if not(found):
		print("Hmmm didn't find defense counsel")


	return ([lead_prosecutor, lead_defense])



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


def create_master_generic_query(resultDF, outputfile, start=0, end=-999):

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

def create_master_generic(sql_file, outputfile, start=0, end=-999):
	fjc_db = openIDB_connection()
	sql= open(sql_file).read()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()
	create_master_generic_query(resultDF, outputfile, start, end)

def create_FJC_update_master(update_file, base_sql_file, outputfile):
	append_list = "and DEFLGKY in ("
	with open(update_file) as csvDataFile:	
		csvReader = csv.reader(csvDataFile)
    	#Skip header row
		next(csvReader)
		for row in csvReader:
			append_list += "'"+row[0]+"'" + ", "
	#remove last comma and replace with closed paren
	append_list = append_list[:-2] + ")"

	sql= open(base_sql_file).read()
	sql += append_list	

	print("Executing this SQL statement\n", sql)
	
	fjc_db = openIDB_connection_New()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()

	#resultDF.to_csv(outputfile, index=False)	
	create_master_generic_query(resultDF, outputfile)



# POC First Then Try to Make Better
def create_wirefraud_update_master():

	my_path = "/Users/colinstarger/Downloads/LDDC_Temp/"
	update_file = my_path+ "wirefraud_update.csv"
	append_list = "and DEFLGKY in ("
	with open(update_file) as csvDataFile:	
		csvReader = csv.reader(csvDataFile)
    	#Skip header row
		next(csvReader)
		for row in csvReader:
			append_list += "'"+row[0]+"'" + ", "
	#remove last comma and replace with closed paren
	append_list = append_list[:-2] + ")"

	base_sql_file = "wirefraud_key.sql"
	outputfile = "wirefraud_2019_update.csv"
	sql= open(base_sql_file).read()
	sql += append_list	

	print("Executing this SQL statement\n", sql)
	
	fjc_db = openIDB_connection_New()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()

	#resultDF.to_csv(outputfile, index=False)	
	create_master_generic_query(resultDF, outputfile)

def update_recap_file(update_file):
	#update_file = "wirefraud_update.csv"
	updateDF = pd.read_csv(update_file)	

	def_key_list = updateDF['def_key'].tolist()

	for def_key in def_key_list:
		update_recap(def_key)

def update_recap(deflogkey, after_date=datetime.datetime.fromisoformat("2020-01-01T01:00:00.000000")):

	def_hash = disagg_fjc_deflogky(deflogkey)
	pacer_docket = convertFJCSQL_to_Docket(def_hash['office'], def_hash['docket'])
	
	#recap_info = checkDocket_in_MDD_RECAP_Fullinfo(pacer_docket)

	endpoint="https://www.courtlistener.com/api/rest/v3/dockets/?court=mdd&docket_number="+pacer_docket
	print("Looking for this endpoint", endpoint)
	myJson=getJSONfromAPI_Auth(endpoint)
	
	if (myJson==None or myJson['count']==0):
		#Need to get from RECAP
		print("Fetching from RECAP", pacer_docket)
		fetch_to_RECAP(pacer_docket)
	else:
		count = myJson['count']
		for i in range (count):
			print("Looking at result",i, "for", pacer_docket)
			result = myJson['results'][i]
			pacer_id = result['pacer_case_id']
			last_modified = result['date_modified'][:-1]
			lm_date = datetime.datetime.fromisoformat(last_modified)
			if lm_date < after_date:
				print("Need to update - getting again", pacer_docket)
				fetch_to_RECAP_PACERID(pacer_id)
			else:
				print("In RECAP already and up to date")
	print("***")		


def create_wirefraud_master():
	#sql_file = "wirefraud_key.sql"
	#outputfile="wirefraud.v2.0.csv"
	#create_master_generic(sql_file, outputfile)
	
	#inputfile="wirefraud.v2.0.csv"
	#outputfile2="wirefraud.v2.1.csv"
	#outputfile3="wirefraud.v2.2.csv"
	#outputfile4="wirefraud.v2.3.csv"

	inputfile="wirefraud_2019_update.csv"
	outputfile2="wirefraud_2019_update_v2.csv"
	outputfile3="wirefraud_2019_update_v3.csv"
	outputfile4="wirefraud_2019_update_v4.csv"
	outputfile5="wirefraud_2019_update_v5.csv"

	addProsecutorDefense(inputfile, outputfile2)
	finalDF = pd.read_csv(outputfile2)
	finalDF = reorderColumns(finalDF)
	finalDF.to_csv(outputfile3, index=False)

	finalDF = pd.read_csv(outputfile3)
	finalDF["Restitution"]=finalDF.apply(getRestitution,axis=1)
	finalDF.to_csv(outputfile4, index=False)

	testDF = pd.read_csv(outputfile4)
	testDF["supervised"] = testDF.apply(getSupRelease, axis=1)
	
	#MAY NEED TO USE THIS AGAIN IF I RE_RUN FROM THE TOP
	#df_len = len(testDF) #904
	#Just to deal with network issue
	#saveBase = "wirefraud.v.2.4"
	
	#i = 4
	#start = 600
	#interval = 200
	#end = start+interval
	#done = False

	#while not done:
	#	tempDF= testDF[start:end]
	#	tempDF["detained"] = tempDF.apply(checkDetained, axis=1)
	#	tempDF.to_csv(saveBase+"-"+str(i)+".csv")
	#	print("\n********Saved ", saveBase+"-"+str(i)+".csv *********\n")
	#	if end==df_len:
	#		done=True
	#	else:
	#		start = end
	#		end = start+interval
	#		if end > df_len:
	#			end=df_len
	#	i+=1 

	#stitchDF = pd.read_csv("wirefraud.v.2.4-1.csv")
	#for stitch in range (2,6):
	#	stitchfile = saveBase+"-"+str(stitch)+".csv"
	#	tempDF = pd.read_csv(stitchfile)
	#	stitchDF = stitchDF.append(tempDF, ignore_index=True)

	#stitchDF.to_csv("wirefraud.v.2.4-stitched.csv", index=False)


	testDF["detained"] = testDF.apply(checkDetained, axis=1)
	#tempDF = testDF[["r_docket_link", "detained"]].copy()
	#tempDF.to_csv("01_test_sup.csv")
	testDF.to_csv(outputfile5, index=False)

def checkDetained(row):
	recap_id = row["recap_id"]
	def_name = row["def_name"]
	last_name = def_name.split()[-1].lower()
	link = row["r_docket_link"]
	slug = link[(link.find("united-states-v-")+len("united-states-v-")):-1]
	#print("Matching last name", last_name, "with slug", slug)
	slugmatch = (slug==last_name) 


	endpoint = "https://www.courtlistener.com/api/rest/v3/docket-entries/?docket="+str(recap_id)
	myJson = getJSONfromAPI_Auth(endpoint)
	count = myJson["count"]
	print("Looking for", def_name, "with count", count)

	status = "didn't find"
	found = False
	i = 1
	while not found:
		results=[]
		try:
			results = myJson["results"]
		except:
			return("Json error")
		for result in results:
			descript = result["description"].lower()
			if ("order of detention" in descript and ((def_name.lower() in descript) or (slugmatch and "as to" not in descript))):
					status = "detained pretrial"
					found=True
					break
			elif ("order setting conditions of release" in descript and ((def_name.lower() in descript) or (slugmatch and "as to" not in descript))):
				status = "released pretrial"
				found=True
				break
			i += 1
			print(i, end=" ")
		print("finished looking at ", i, "entries")
		if (i < count and not found):
			myJson = getJSONfromAPI_Auth(myJson["next"])
		else:
			found = True
	print("result = ", status)
	return(status)


def getSupRelease(row):

	text= row["r_top_disp"]
	print("Looking at text", text)

	semi_parse=""
	try:
		semi_parse = text.split(";")
	except:
		print("no text to parse")
		return("NO TEXT")
	
	lower = [x.lower() for x in semi_parse]
	found = False
	for phrase in lower:
		if "supervised release" in phrase:
			found = phrase
			break
	
	#Get rid of commas
	if (found and found.count(",") >1):

		commas = found.split(",")
		for phrase in commas:
			if "supervised release" in phrase:
				found = phrase
				break

	if (found):
		return(found)
	else:
		return("not found")

def getRestitution(row):

	parsed_disp = str(row["r_top_disp"]).split()
	parsed_lower = [x.lower() for x in parsed_disp]
	
	restitution = "none"

	if ( "restitution" in parsed_lower) or ("restitution;" in parsed_lower) or ("restitution," in parsed_lower) or ("restituion" in parsed_lower):
	
		index=0

		if ( "restitution" in parsed_lower):
			index = parsed_lower.index("restitution")
		
		elif ("restitution;" in parsed_lower):
			index = parsed_lower.index("restitution;")

		elif ("restitution," in parsed_lower): 
		 	index = parsed_lower.index("restitution,")

		else: 
		 	#("restituion" in parsed_lower):
		 	index = parsed_lower.index("restituion")
		
		isLast = (index==len(parsed_lower)-1)
		isFirst = (index==0)

		if (not isLast):
			aft = parsed_lower[index+1]
			if (aft[0]=="$" and parsed_lower[index][-1]!=";" and parsed_lower[index][-1]!=","):
				restitution = aft
			else:
				if (aft=="of" and parsed_lower[index+2][0]=="$"):
					restitution= parsed_lower[index+2]
				else:
					if aft=="in" and parsed_lower[index+2]=="the" and parsed_lower[index+3]=="anount":
						restitution= parsed_lower[index+5]	
		
		if restitution == "none":
			bef = parsed_lower[index-1]
			restitution = bef

		if (restitution[-1]==";"):
			restitution=restitution[:-1]
		
		return(restitution)

	return("none")
	
def create_bankrobbery_master():
	sql_file = "bank_robbery_key.sql"
	outputfile="bank_robbery_2019.v1.csv"

	my_path = "/Users/colinstarger/Downloads/LDDC_Temp/"
	update_file = my_path+ "bank_update.csv"
	#update_recap_file(update_file)

	#create_FJC_update_master(update_file, sql_file, outputfile)

	#create_master_generic(sql_file, outputfile, 700, -999)
	#create_master_generic(sql_file, outputfile)

	outputfile2="bank_robbery_2019.v2.csv"	
	outputfile3="bank_robbery_2019.v3.csv"	
	addProsecutorDefense(outputfile, outputfile2)
	finalDF = pd.read_csv(outputfile2)
	finalDF = reorderColumns(finalDF)
	finalDF.to_csv(outputfile3, index=False)


def create_colin_child_results_part1():
	#This is update to "create_new_child_master" but after Nick had to leave
	sql_file = "child_exploit_key_update_j.sql"
	sql= open(sql_file).read()
	fjc_db = openIDB_connection_New()
	resultDF = executeQuery_returnDF(fjc_db, sql)
	fjc_db.close()

	outputfile="child_exploit.v5.0.csv"
	#resultDF.to_csv(outputfile, index=False)

	#Uncomment below if want to do a recap update
	#Probably should make this a separate part rather than comment out!
	#update_recap_file(outputfile)

	titlecols = ["top_charge", "TTITLE1", "TTITLE2", "TTITLE3", "TTITLE4", "TTITLE5"]
	
	for title in titlecols:
		resultDF[title]= resultDF.apply(lambda x: FJC_clean_title(x[title]), axis=1)

	#Deal with FJC Codes
	resultDF["prison_total"] = resultDF.apply(FJC_prison_hash, axis=1)
	resultDF["top_disp"]= resultDF.apply(FJC_disp_hash, axis=1)

	#Kludgy but it works - get results back as a DF and then apply to resultsDC
	temp = resultDF.apply(get_CE_FJC_Info, axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)
	resultDF["top_ce_charge"] = temp2[0]
	resultDF["top_ce_charge_no"] = temp2[1]
	resultDF["ce_charge_disp"] = temp2[2]
	resultDF["ce_charge_prison"] = temp2[3]
	resultDF["ce_charge_sup_release"] = temp2[4]
	resultDF = resultDF[resultDF["top_ce_charge_no"]!="NA"]
	dropcols = ["TTITLE1", "TTITLE2", "TTITLE3", "TTITLE4", "TTITLE5", "DISP1", "DISP2", "DISP3", "DISP4", "DISP5", "PRISTIM1", "PRISTIM2", "PRISTIM3", "PRISTIM4", "PRISTIM5", "SUPVREL1", "SUPVREL2", "SUPVREL3", "SUPVREL4", "SUPVREL5"]
	resultDF = resultDF.drop(dropcols, axis=1)

	
	#Kludgy but it works - get results back as a DF and then apply to resultsDC
	#Now going to Recap
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
	
	resultDF.to_csv(outputfile, index=False)

def create_colin_child_results_part2():

	inputfile = "child_exploit.v5.0.csv"
	outputfile = "child_exploit.v5.1.csv"

	resultDF = pd.read_csv(inputfile)

	#Remover one Rule 20a case
	resultDF = resultDF[resultDF['top_disp']!="Rule 20(a)/21 transfer"]
	
	#Remove  NIR case + sealed -- sealed
	resultDF = resultDF[resultDF['def_name']!="nir"]
	resultDF = resultDF[resultDF['prison_total']!="Sealed sentence"]

	#Coding errors in FJC or RECAP
	#This case does not run concurrent, unlike co-D
	index = resultDF[resultDF['def_key']=="041611000263CR0020"].index[0]
	resultDF.at[index, 'prison_total']=360

	#This case years not translated to months
	index = resultDF[resultDF['def_key']=="041611100152CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=12*25

	#Miscoded prison time as 0 - is actually 60 mo
	index = resultDF[resultDF['def_key']=="041681000348CR0010"].index[0]
	resultDF.at[index, 'prison_total']=60
	resultDF.at[index, 'ce_charge_prison']=60

	#Error in sup release should be 360 not 250
	index = resultDF[resultDF['def_key']=="041611100532CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=12*30

	#Miscoded prison time as 0 - is actually 180 mo
	index = resultDF[resultDF['def_key']=="041611100673CR0010"].index[0]
	resultDF.at[index, 'prison_total']=180
	resultDF.at[index, 'ce_charge_prison']=180

	#Miscoded prison time as 480 - is actually 240 mo
	index = resultDF[resultDF['def_key']=="041681000083CR0010"].index[0]
	resultDF.at[index, 'prison_total']=240
	resultDF.at[index, 'ce_charge_prison']=240

	#Miscoded 15 years - should be in months
	index = resultDF[resultDF['def_key']=="041611200588CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=12*15

	#Miscoded 150 months - should be 15 years * 12
	index = resultDF[resultDF['def_key']=="041611300530CR0050"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=12*15

	#Miscoded prison time as 36 - is actually 264 mo
	index = resultDF[resultDF['def_key']=="041681200229CR0020"].index[0]
	resultDF.at[index, 'prison_total']=264

	#Miscoded 15 year - should be 15 years * 12
	index = resultDF[resultDF['def_key']=="041681300558CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=12*15

	#Miscoded count 1, should be 60 mos + 5years sup release
	index = resultDF[resultDF['def_key']=="041611000770CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=5*12
	resultDF.at[index, 'ce_charge_prison']=60
	
	#Miscoded sup_rel should be 240 mos, not 840
	index = resultDF[resultDF['def_key']=="041611500230CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=20*12

	#Miscoded prison time should be 0, not missing
	index = resultDF[resultDF['def_key']=="041611500551CR0010"].index[0]
	resultDF.at[index, 'prison_total']=0

	#Miscoded prison time should be 0, not missing
	index = resultDF[resultDF['def_key']=="041681500639CR0010"].index[0]
	resultDF.at[index, 'prison_total']=0

	#Miscoded prison time as 36mos - is actually 18+18 years mo
	index = resultDF[resultDF['def_key']=="041611500131CR0010"].index[0]
	resultDF.at[index, 'prison_total']=36*12
	resultDF.at[index, 'ce_charge_prison']=18*12

	#Miscoded prison time should be 0, not missing
	index = resultDF[resultDF['def_key']=="041611600060CR0010"].index[0]
	resultDF.at[index, 'prison_total']=0

	#Miscoded prison time should be 0, not missing
	index = resultDF[resultDF['def_key']=="041611600255CR0010"].index[0]
	resultDF.at[index, 'prison_total']=0

	#Miscoded prison time should be 0, not missing
	index = resultDF[resultDF['def_key']=="041611700652CR0010"].index[0]
	resultDF.at[index, 'prison_total']=0

	#Miscoded ce time as 420, should be 210
	index = resultDF[resultDF['def_key']=="041611600181CR0010"].index[0]
	resultDF.at[index, 'ce_charge_prison']=210
	
	#Miscoded ce time as 288, should be 144
	index = resultDF[resultDF['def_key']=="041611700673CR0010"].index[0]
	resultDF.at[index, 'ce_charge_prison']=144

	#Miscoded prison time as 10mos - is actually 10 years
	index = resultDF[resultDF['def_key']=="041611800081CR0010"].index[0]
	resultDF.at[index, 'prison_total']=120
	resultDF.at[index, 'ce_charge_prison']=120

	#Miscoded sup_rel not 20 mos, 20 years
	index = resultDF[resultDF['def_key']=="041611800340CR0010"].index[0]
	resultDF.at[index, 'ce_charge_sup_release']=20*12

	#Miscoded ce time as 720 - is actually 360
	index = resultDF[resultDF['def_key']=="041681700195CR0010"].index[0]
	resultDF.at[index, 'ce_charge_prison']=360

	#Save results
	resultDF.to_csv(outputfile, index=False)

def create_colin_child_results_part3():

	inputfile = "child_exploit.v5.1.csv"
	outputfile = "child_exploit.v5.2.csv" 
	#addProsecutorDefense(inputfile, inputfile)

	resultDF= pd.read_csv(inputfile)
	temp = resultDF.apply(getChild_RECAP_Attorneys_Row_DefNo, axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)
	resultDF['prosecutor_lead'] = temp2[0]
	resultDF['defense_lead'] = temp2[1]

	resultDF.to_csv(outputfile, index=False)

def create_colin_child_results_part4():
	inputfile = "child_exploit.v5.2.csv"
	outputfile = "child_exploit.v5.2.csv" 

	resultDF = pd.read_csv(inputfile)

	resultDF["File year"]=resultDF.apply(getFileYear,axis=1)
	resultDF["Judge"]=resultDF.apply(getJudgeLastName, axis=1)
	#check this one
	resultDF["fpd_docket"]=resultDF.apply(getFPD_docket,axis=1)
	resultDF.to_csv(outputfile, index=False)

def create_colin_child_results_part5():
	inputfile = "child_exploit.v5.2.csv"
	outputfile = "child_exploit.v5.3.csv"
	st_file = "/Users/colinstarger/OneDrive - University of Baltimore/LDDC/FedDefender/Child Exploit/CE_ST_to_integrate.xlsx"
	cp_file = "/Users/colinstarger/OneDrive - University of Baltimore/LDDC/FedDefender/Child Exploit/Chart of CP Production cases-LDDC.xlsx"

	resultDF = pd.read_csv(inputfile)
	searchDF1 = pd.read_excel(st_file, sheet_name='Main')
	searchDF2 = pd.read_excel(cp_file, sheet_name='Main')

	resultDF['charged_conduct'] = resultDF.apply(locateChargedCoduct, args=(searchDF1, searchDF2), axis=1)
	resultDF.to_csv(outputfile, index=False)

def create_colin_child_lookup_files():
	inputfile = "child_exploit.v5.3.csv"
	outputfile = "CP_to_fill_out.xlsx"

	inputDF = pd.read_csv(inputfile)
	outputDF = inputDF[['def_name', 'top_ce_charge', 'ce_charge_disp', 'pacer_docket', 'r_docket_link', 'charged_conduct']]
	outputDF['sexual contact']=""
	outputDF["minor victim"]=""
	outputDF["num victims"]=""
	outputDF["victim ages"]=""
	outputDF["plea link"]=""

	outputDF.to_excel(outputfile)

def create_colin_child_results_part6():

	#This was autocreated
	baseFile = "child_exploit.v5.3.csv"
	baseDF = pd.read_csv("child_exploit.v5.3.csv")

	targetDF= baseDF
	targetDF['FPD_charged_conduct']=""
	targetDF['sexual contact']=""
	targetDF['minor victim']=""
	targetDF['num victims']=""
	targetDF['victim ages']=""
	targetDF['plea link']=""

	#These come from RAs and me - by hand - with range specified
	importFileList = [["~/Downloads/CP_to_fill_out_COLIN.xlsx", 0, 6],
					  ["~/Downloads/Andrew_CP_SS.xlsx", 100, 110]]

	for importFileCluster in importFileList:
		importFile = importFileCluster[0]
		start= importFileCluster[1]
		end= importFileCluster[2]
		importDF=pd.read_excel(importFile)

		for i in range(start, end+1):
			row = importDF.loc[importDF['Index']==i].iloc[0,]
			name = row['def_name']
			docket = row['pacer_docket']
			targetIndex = targetDF[(targetDF['def_name']==name) & (targetDF['pacer_docket']==docket)].index[0]

			targetCols = ['charged_conduct','FPD_charged_conduct','sexual contact','minor victim','num victims','victim ages','plea link']
			for col in targetCols:
				targetDF.loc[targetIndex,col]=row[col]

	outputfile = "child_exploit.v5.4.csv"
	targetDF.to_csv(outputfile, index=False)

def create_colin_child_results_part7():
	inputfile = "child_exploit.v5.4.csv"
	outputfile= "child_exploit.v5.5.csv"

	inputDF= pd.read_csv(inputfile)

	outDF=inputDF[['File year','top_ce_charge', 'top_ce_charge_no', 'ce_charge_disp', 'ce_charge_prison', 'ce_charge_sup_release', 'prison_total', 'assigned_to', 'fpd_docket', 'def_name', 'charged_conduct', 'FPD_charged_conduct', 'sexual contact', 'minor victim', 'num victims', 'victim ages', 'prosecutor_lead', 'defense_lead','top_charge', 'top_disp', 'r_top_disp', 'r_docket_link', 'plea link', 'r_convictions_total', 'disp_date']]
	
	"""
	['def_key', 'top_charge', 'top_disp', 'prison_total', 'file_date',
       'disp_date', 'top_ce_charge', 'top_ce_charge_no', 'ce_charge_disp',
       'ce_charge_prison', 'ce_charge_sup_release', 'pacer_docket', 'recap_id',
       'assigned_to', 'case_name', 'def_name', 'r_charges_total',
       'r_convictions_total', 'r_top_charge', 'r_top_disp', 'r_docket_link',
       'prosecutor_lead', 'defense_lead', 'File year', 'Judge', 'fpd_docket',
       'charged_conduct', 'FPD_charged_conduct', 'sexual contact',
       'minor victim', 'num victims', 'victim ages', 'plea link',
       'FPD charged conduct']
	"""

	outDF = outDF.rename(columns={"File year":"File yr", 'top_ce_charge': 'Top CE Charge', 'top_ce_charge_no': 'Cg#', 'ce_charge_disp': 'Disp', "ce_charge_prison":"Prison", "ce_charge_sup_release": "Sup Rels", 'prison_total':'Prison total', 'assigned_to': 'Judge', 'prosecutor_lead': "Prosecutor", 'defense_lead': 'Defense', 'r_convictions_total': 'Total convictions', 'r_top_disp': 'Pacer top disp', 'FPD_charged_conduct': "FPD descript"})

	#try a hyperlink fix
	#outDF["r_docket_link"] = outDF.apply(lambda x: '=HYPERLINK("'+ x['r_docket_link']+'")', axis=1)

	outDF.to_csv(outputfile, index=False)


def checkExcelFile_Overlap():
	#THIS WAS JUST A DATA CHECK
	inputfile = "child_exploit.v5.2.csv"
	st_file = "/Users/colinstarger/OneDrive - University of Baltimore/LDDC/FedDefender/Child Exploit/CE_ST_to_integrate.xlsx"
	cp_file = "/Users/colinstarger/OneDrive - University of Baltimore/LDDC/FedDefender/Child Exploit/Chart of CP Production cases-LDDC.xlsx"

	resultDF = pd.read_csv(inputfile)

	print("CP Cases")
	cpDF = pd.read_excel(cp_file, sheet_name='Main')
	id_list = cpDF['FPD_id'].tolist()
	for cpid in id_list:
		
		if (not pd.isna(cpid)) and int(cpid[4:6])>=10:
			result = resultDF.loc[resultDF['fpd_docket']==cpid, ['top_charge']]
			if len(result.index)==0:
				print("DID NOT FIND", cpid)
	print("*****")
	print("ST CASES")
	stDF = pd.read_excel(st_file, sheet_name='Main')
	id_list = stDF['FPD_id'].tolist()
	for stid in id_list:
		
		if (not pd.isna(cpid)) and int(cpid[4:6])>=10:
			result = resultDF.loc[resultDF['fpd_docket']==cpid, ['top_charge']]
			if len(result.index)==0:
				print("DID NOT FIND", cpid)
		

def locateChargedCoduct(row, searchDF1, searchDF2):

	searchTerm = row['fpd_docket']
	
	result = searchDF1.loc[searchDF1['FPD_id']==searchTerm, ['Charged Conduct']]
	if len(result.index) >= 1:
		return result.iat[0,0]
	
	result = searchDF2.loc[searchDF2['FPD_id']==searchTerm, ['Summary of Offense']]
	if len(result.index) >= 1:
		return result.iat[0,0]

	return ""


def create_new_child_master():

	sql_file = "child_exploit_key_all.sql"
	outputfile="child_exploit.v4.0.csv"

	#create_master_generic(sql_file, outputfile, 525, -999)
	#create_master_generic(sql_file, outputfile)
	#addProsecutorDefense("child_exploit.v4.0.csv", "child_exploit.v4.1.csv")
	#finalDF = pd.read_csv("child_exploit.v4.1.csv")
	#finalDF = reorderColumns(finalDF)
	#finalDF = addNickColumns(finalDF)
	#finalDF.to_csv("child_exploit.v4.2.csv", index=False)

	#Latest Innovation
	finalDF = pd.read_csv("child_exploit.v4.2.csv")
	finalDF["fpd_docket"]=finalDF.apply(getFPD_docket,axis=1)
	finalDF.to_csv("child_exploit.v4.3.csv", index=False)


def getFPD_docket(row):

	def_key = row["def_key"]
	fjc_docket = disagg_fjc_deflogky(def_key)["docket"]
	year = fjc_docket[:2]
	caseno = fjc_docket[-4:]
	if caseno[0]=="0":
		caseno=caseno[1:] #get rid of first 0
	judge = row["assigned_to"]

	initials=""
	try:
		initials = judgeHash[judge]
	except:
		#print("Couldn't find judge", judge)
		return("")
	
	fpd_docket = initials+"-"+year+"-"+caseno
	return(fpd_docket)

def reorderColumns(inputDF, goal="child"):

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


def addNickColumns(mainDF):
	#This is specific to the child data set
	
	mainDF["Minor victim"]=""
	mainDF["Num victims"]=""
	mainDF["Victim ages"]=""
	mainDF["Sexual contact"]=""
	mainDF["Plea link"]=""

	mainDF["File year"]=mainDF.apply(getFileYear,axis=1)
	mainDF["Judge"]=mainDF.apply(getJudgeLastName, axis=1)
	return mainDF

def getFileYear(row):
	year= str(row['file_date'])[-2:] 
	year = int("20"+year)
	return(year)

def getJudgeLastName(row):
	fullname= str(row["assigned_to"])
	last_name = fullname.split()[-1]

	if last_name in ("Jr.", "III"):
		last_name = fullname.split()[-2]
	return(last_name)

def updateWireAttorneys_1():

	#I really need to clean up this sprawling code
	#But for now, just need to get it done
	#copying Bank and breaking up to deal with network interruptions
	inputfile = "wirefraud_attorney_update.csv"

	inputDF = pd.read_csv(inputfile)
	inputDF['defno'] = inputDF.apply(lambda x: disagg_fjc_deflogky(x['def_key'])['defno'], axis=1)
	inputDF['year']= inputDF.apply(lambda x: "20"+disagg_fjc_deflogky(x['def_key'])['docket'][0:2], axis=1)

	#inputDF = inputDF[inputDF['year']>"2011"]
	df_len = len(inputDF)
	start = 0
	interval = 200
	end = start + interval
	done = False
	i=1

	while not done:
		sliceDF = inputDF[start:end]
		temp = sliceDF.apply(lambda x: getLeadAttorneys_defNo(x['recap_id'], x['defno']), axis=1)
		temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)
		sliceDF['prosecutor_lead'] = temp2[0]
		sliceDF['defense_lead'] = temp2[1]
		sliceDF.to_csv("wire_temp_"+str(i)+".csv", index=False)
		
		if end==df_len:
			done = True
		else:
			start=end
			end += interval
			if end > df_len:
				end = df_len
		i += 1

def updateWireAttorneys_2():
	
	stitchDF = pd.read_csv("wire_temp_1.csv")
	for stitch in range (2,6):
		stitchfile = "wire_temp_"+str(stitch)+".csv"
		tempDF = pd.read_csv(stitchfile)
		stitchDF = stitchDF.append(tempDF, ignore_index=True)

	stitchDF.to_csv("wirefraud_attorneys__filled_update.csv", index=False)



def updateBankAttorneys():

	inputfile = "bank_robbery_v4.csv"
	outputfile = "bank_attorneys_update.csv"

	inputDF = pd.read_csv(inputfile)
	inputDF['defno'] = inputDF.apply(lambda x: disagg_fjc_deflogky(x['def_key'])['defno'], axis=1)
	inputDF['year']= inputDF.apply(lambda x: "20"+disagg_fjc_deflogky(x['def_key'])['docket'][0:2], axis=1)
	inputDF = inputDF[inputDF['year']>"2011"]
	
	temp = inputDF.apply(lambda x: getLeadAttorneys_defNo(x['recap_id'], x['defno']), axis=1)
	temp2 = pd.DataFrame(temp.values.tolist(), index=temp.index)
	inputDF['prosecutor_lead'] = temp2[0]
	inputDF['defense_lead'] = temp2[1]
	
	inputDF.to_csv(outputfile, index=False)



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


	# WIREFRAUD
	#create_wirefraud_master()
	updateWireAttorneys_1()
	updateWireAttorneys_2()

	# Bank Robbery
	#create_bankrobbery_master()

	# Child Ex
	#create_new_child_master() -- This is pre-Colin taking over
	#create_colin_child_results_part1()
	#create_colin_child_results_part2()
	#create_colin_child_results_part3() #get Attorneys
	#create_colin_child_results_part4()
	#create_colin_child_results_part5()
	#create_colin_child_lookup_files()	
	#create_colin_child_results_part6() #This is input by-hand data (from coded stuff)
	#create_colin_child_results_part7() #This is re-order columns

	#Use this to get Doppelgangers
	#print(fetch_to_RECAP_PACERID("201590"))
	pass

# CALL MAIN
main()
#create_wirefraud_update_master()
#update_wirefraud_recap()

#updateAttorneys()


