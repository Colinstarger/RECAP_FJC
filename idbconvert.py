__author__ = 'Matthew'
import pymysql
import sys
from datetime import datetime
import time
import csv

def createFileWithNewHeaders(location_of_file):
    #Creates a new text file with the updated headers.
    #There is almost certainly an easier way to do this where we just delete the first line of the old file and replace it with the new headers.
    with open(location_of_file + "cr96on.txt") as f:
        lines = f.readlines()

    lines[0] = "FISCALYR	CIRCUIT	DISTRICT	OFFICE	DOCKET	DEFNO	CTDEF	NAME	REOPSEQ	TYPEREG	TYPETRN	TYPEMAG	DEFLGKY	CASLGKY	MAGDOCK	MAGDEF	STATUSCD	FUGSTAT	FGSTRTDATE	FGENDDATE	FILEDATE	PROCDATE	PROCCD	APPDATE	APPCD	FJUDGE	FCOUNSEL	FTITLE1	FOFFLVL1	FOFFCD1	D2FOFFCD1	FSEV1	FTITLE2	FOFFLVL2	FOFFCD2	D2FOFFCD2	FSEV2	FTITLE3	FOFFLVL3	FOFFCD3	D2FOFFCD3	FSEV3	FTITLE4	FOFFLVL4	FOFFCD4	D2FOFFCD4	FSEV4	FTITLE5	FOFFLVL5	FOFFCD5	D2FOFFCD5	FSEV5	COUNTY	TRANDIST	TRANOFF	TRANDOCK	TRANDEF	UPDATE_COL	DISPDATE	SENTDATE	TERMDATE	INTONE	INTTWO	INTTHREE	TERMOFF	TJUDGE	TCOUNSEL	TTITLE1	TOFFLVL1	TOFFCD1	D2TOFFCD1	TSEV1	DISP1	PRISTIM1	PRISCD1	PROBMON1	PROBCD1	SUPVREL1	FINEAMT1	TTITLE2	TOFFLVL2	TOFFCD2	D2TOFFCD2	TSEV2	DISP2	PRISTIM2	PRISCD2	PROBMON2	PROBCD2	SUPVREL2	FINEAMT2	TTITLE3	TOFFLVL3	TOFFCD3	D2TOFFCD3	TSEV3	DISP3PRISTIM3	PRISCD3	PROBMON3	PROBCD3	SUPVREL3	FINEAMT3	TTITLE4	TOFFLVL4	TOFFCD4	D2TOFFCD4	TSEV4	DISP4	PRISTIM4	PRISCD4	PROBMON4	PROBCD4	SUPVREL4	FINEAMT4	TTITLE5	TOFFLVL5	TOFFCD5	D2TOFFCD5	TSEV5	DISP5	PRISTIM5	PRISCD5	PROBMON5	PROBCD5	SUPVREL5	FINEAMT5	PRISTOT	PROBTOT	FINETOT	CTFILTRN	CTFIL	CTFILWOR	CTFILR	CTTRTRN	CTTR	CTTRWOR	CTTRR	CTPN	CTPNWOF	SOURCE	VER	LOADDATE	TAPEYEAR\n"
    with open(location_of_file + "dataproperheaders.csv", "w") as f:
        f.writelines(lines)

def createTable(headers):
    #Creates a sql query to create the table in mysql
    sql = "CREATE TABLE records2( "
    #We need this header conversion because some of the headers are keywords in mysql
    for header in headers:
        if(header == "INT1"):
            header = "INTONE"
        elif(header == "INT2"):
            header = "INTTWO"
        elif(header == "INT3"):
            header = "INTTHREE"
        elif(header == "UPDATE"):
            header = "UPDATE_COL"

        sql += header + " varchar(255), "
    sql = sql[0:len(sql)-2] + ")"
    return sql

def insertRow(row):
    #I found this process worked way too slowly. So I didn't use it but in theory it works
    endpoint = 'localhost' #Localhost or the AWS endpoint
    try:
        username = "root"
        password = "password"
        db = pymysql.connect(endpoint,username,password,"idb" ) #Changed the schema if it's not idb
    except Exception as e:
        print(e)
        sys.exit()
    try:
        # prepare a cursor object using cursor() method
        cursor = db.cursor()

        #Fix the problem headers
        row["INTONE"] = row["INT1"]
        del(row["INT1"])
        row["INTTWO"] = row["INT2"]
        del(row["INT2"])
        row["INTTHREE"] = row["INT3"]
        del(row["INT3"])
        row["UPDATE_COL"] = row["UPDATE"]
        del(row["UPDATE"])

        #Convert date vaules to mysql date values
        #There must be a better way to do this.
        tempkeys = row.keys()
        newkeys = []
        for key in tempkeys:
            if("DATE" in key):
                newkeys.append(key)
        for key in newkeys:
            #print(row[key])
            row[key] = datetime.strptime(row[key], '%m/%d/%Y').strftime('%Y-%m-%d')

        placeholder = ", ".join(["%s"] * len(row))
        stmt = "insert into `{table}` ({columns}) values ({values});".format(table="records2",
                                                                             columns=",".join(row.keys()),
                                                                             values=placeholder)
        cursor.execute(stmt, list(row.values()))
        db.commit()
    except Exception as e:
        print("Encountered Error")
        print(e)
    except MySQLError as w:
        print("Ran Into MySQL Error")
        print(w)

def importViaInsert(location_of_file):
    x=0
    with open("D:\Dropbox\IDB\cr96on_new\cr96on.txt","r") as f:
        reader = csv.DictReader(f,delimiter='\t')
        for row in reader:
            insertRow(row)
            #Add a break so you can do some testing before sending over the whole file
            if(x==20):
              break
            x+=1

if __name__ == '__main__':
    # Steps:
    # 1) Run the createtable function which gives you the text to create a new sql table.
    # 2) Run that text in whatever sql program you use.
    # 3) Run the createFileWithNewHeaders function to create a new csv file with the fixed headers.False
    # 4) Upload that file to the server and run the sql command below.
    # LOAD DATA INFILE '/mysqlfiles/dataproperheaders.csv'
    # INTO TABLE records3
    # FIELDS TERMINATED BY '\t'
    # LINES TERMINATED BY '\n'
    # IGNORE 1 ROWS;
    # 5) You should be done! Note the step above is riddled with pitfalls. Secure-file option may not have a folder available for uploading files this way.
    # Also if youre running an RDS on AWS Im not sure how you can move the file to that server and you may have to import some other way.
    # One way being to upload it to a local mysql host and then export as a .sql file which can then be uploaded to the AWS server.
    # On the docker mysql container it's at /etc/mysql/my.cnf
    st=time.time() #Start a time to see how long everything takes.
    location_of_file = "D:\Dropbox\IDB\cr96on_new\\"
    with open(location_of_file + "cr96on.txt","r") as f:
        first_line = f.readline()
        print(first_line)
        headers = first_line.split("\t")

    print(createTable(headers))
    #createFileWithNewHeaders(location_of_file)
    #importViaInsert

    print("----%.2f----"%(time.time()-st))