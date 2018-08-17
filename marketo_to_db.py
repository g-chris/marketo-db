import string
import requests
import json
import psycopg2
from datetime import datetime, date, time, timedelta
import time
import sched, time

#Global Variables
m_id_global = #TODO
m_secret_global = #TODO
m_endpoint_global = #TODO
m_identity_global = #TODO

db_name = #TODO

db_port = "5432"

db_user = #TODO

db_host = #TODO

db_password = #TODO


#This will hold all the values for each row, they are popped out as they are added to the DB
#global_list will (should) never have more than 1 row in it at a time
global_list = []

#Call to run on a schedule. Adjust how often at the end of the program
s = sched.scheduler(time.time, time.sleep)

#Checks most recent value currently in the databse and returns a time (as a string) that is 1 second later
def getLastRunDate():
    try:
        conn = psycopg2.connect("dbname=(%s) port=(%s) user=(%s) host=(%s) password=(%s)" % (db_name, db_port, db_user, db_host, db_password))
        cur = conn.cursor()
    except:
        print('Database connection error. - Last Run Date Function')

    lastDateQ = cur.execute("SELECT MAX(active_date) FROM marketo.forms2010")

    lastDate = cur.fetchone()

    strDate = str(lastDate[0])

    lastDate2 = time.strptime(strDate,"%Y-%m-%d %H:%M:%S")

    lastDate3 = datetime(*lastDate2[0:6])

    newDate = lastDate3 + timedelta(seconds=1)

    newDate2 = str(newDate)

    index = newDate2.find(' ')

    newDate3 = newDate2[:index] + 'T' + newDate2[index:] + 'Z'

    newDate4 = newDate3.replace(" ","")


    return newDate4


#Adds values to global_list
def toTXT(var):
    global global_list

    global_list.append(var)


def m_auth():
    m_identity = m_identity_global
    m_id = m_id_global
    m_secret = m_secret_global

    auth_call_url = m_identity + "/oauth/token?grant_type=client_credentials&client_id=" + m_id +"&client_secret="+ m_secret

    response = requests.get(auth_call_url)

    auth = response.json()

    m_token = auth["access_token"]

    #print("Auth Token Called")

    return m_token

def activities_paging_token():
    m_endpoint = m_endpoint_global
    #sinceDateTime = "2017-01-01T00:00:00.000Z"
    sinceDateTime = str(getLastRunDate())
    m_token = m_auth()

    paging_call_url = m_endpoint + "/v1/activities/pagingtoken.json?access_token=" + m_token + "&sinceDatetime=" + sinceDateTime


    r = requests.get(paging_call_url)
    r2 = r.json()

    p_token = r2["nextPageToken"]

    #print("Paging Token Called")


    return p_token


def jsonCallFirst():
    m_token = m_auth()
    m_id = m_id_global
    m_secret = m_secret_global
    m_endpoint = m_endpoint_global
    p_token = activities_paging_token()


    api_call_url = m_endpoint + "/v1/activities.json?access_token=" + m_token + "&nextPageToken=" + p_token + "&activityTypeIds=2"


    r = requests.get(api_call_url)
    r2 = r.json()
    return r2
    #return r

def jsonCall(nextToken):
    m_token = m_auth()
    m_id = m_id_global
    m_secret = m_secret_global
    m_endpoint = m_endpoint_global
    nextToken = str(nextToken)

    api_call_url = m_endpoint + "/v1/activities.json?access_token=" + m_token + "&nextPageToken=" + nextToken + "&activityTypeIds=2"

    r = requests.get(api_call_url)
    r2 = r.json()
    return r2

#TODO
#Replace these values with those in your DB both here and in the attribute_parse function
#Writes JSON data to local Postgres DB
def postgresWrite():

    global global_list

    try:
        conn = psycopg2.connect("dbname=(%s) port=(%s) user=(%s) host=(%s) password=(%s)" % (db_name, db_port, db_user, db_host, db_password))
        cur = conn.cursor()
    except:
        print('Database connection error. postgresWrite Function')

    #print(global_list)

    date1 = global_list.pop(0)
    type1 = global_list.pop(0)
    first_name = global_list.pop(0)
    last_name = global_list.pop(0)
    company = global_list.pop(0)
    email = global_list.pop(0)
    phone = global_list.pop(0)
    zip_code = global_list.pop(0)
    industry = global_list.pop(0)
    source_code = global_list.pop(0)
    lead_source = global_list.pop(0)
    assessment_request = global_list.pop(0)
    lp_url = global_list.pop(0)
    ref_url = global_list.pop(0)
    id1 = global_list.pop(0)
    lead_id = global_list.pop(0)
    marketo_guid = global_list.pop(0)
    att_value = global_list.pop(0)
    att_id = global_list.pop(0)

    #print(global_list)



    cur.execute("INSERT INTO marketo.forms2017 (active_date, activity_type, first_name, last_name, company, email, phone, zip_code, industry, source_code, lead_source, assessment_request, lp_url, ref_url, id, lead_id, marketo_guid, att_value, att_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (date1,type1,first_name,last_name,company,email,phone,zip_code,industry,source_code,lead_source,assessment_request,lp_url,ref_url,id1,lead_id,marketo_guid,att_value,att_id))
    conn.commit()



    print("Wrote to DB")
    now = datetime.now()
    now = str(now)
    print (now)

#Can be used to write data to a CSV in addition to or instead of to a DB
def toTXT_OLD(text):
    with open('text9.txt', 'a') as textfile:
        text = str(text)
        textfile.write(text)
        textfile.write(',')
        now = datetime.now()
        now = str(now)
        print (now)
        print("Printed text to file")

def getNextToken(jSonFile):
    nToken = jSonFile["nextPageToken"]
    #print("Getting next page token")
    return nToken

def moreCheck(jSonFile):
    moreResult = jSonFile["moreResult"]

    print("Checking for more results . . . " )

    if moreResult == True:
        print("More Results")
        return True
    else:
        print("No More Results")
        return False

#TODO
#Function uses REGEX to parse based on the fields in the Marketo forms we use.
#Inflexible if forms change, must be manually updated in that case
def attribute_parse(data):
    #print("Attributes:")
    #print(data)

    data_string = data

    data_string2 = data_string[data_string.find("{")+1:data_string.find(";}''")]

    #First Name Parse
    first_name = data_string2[data_string2.find("""FirstName""")+10:data_string2.find("""LastName""")-1]

    first_name2 = first_name[first_name.find(':"')+2:first_name.find('";')]

    fname_len = len(first_name2)

    #print("First Name:")
    if fname_len > 50:
        toTXT("")
    else:
        toTXT(first_name2)

    #Last Name Parse
    last_name = data_string2[data_string2.find("""LastName""")+10:data_string2.find("""Company""")]

    last_name2 = last_name[first_name.find(':"')+1:last_name.find('";')]

    lname_len = len(last_name2)

    #print("Last Name:")
    if lname_len > 50:
        toTXT("")
    else:
        toTXT(last_name2)

    #Company Parse
    company_name = data_string2[data_string2.find("""Company""")+10:data_string2.find("""Email""")]

    company2 = company_name[company_name.find(':"')+2:company_name.find('";')]

    cname_len = len(company2)

    #print("Company Name:")
    if cname_len > 50:
        toTXT("")
    else:
        toTXT(company2)


    #Email Parse
    email_name = data_string2[data_string2.find("""Email""")+10:data_string2.find("""Phone""")]

    email2 = email_name[email_name.find(':"')+2:email_name.find('";')]

    ename_len = len(email2)

    #print("Email Name:")
    if ename_len > 50:
        toTXT("")
    else:
        toTXT(email2)

    #Phone Parse
    phone_name = data_string2[data_string2.find("""Phone""")+10:data_string2.find("""Zip_Code__c""")]

    phone2 = phone_name[phone_name.find(':"')+2:phone_name.find('";')]

    pname_len = len(phone2)

    #print("Phone Number:")
    if ename_len > 50:
        toTXT("")
    else:
        toTXT(phone2)


    #Zip Parse
    zip_name = data_string2[data_string2.find("""Zip_Code__c""")+12:data_string2.find("""Industry""")-1]

    zip2 = zip_name[zip_name.find(':"')+2:zip_name.find('";')]

    zname_len = len(zip2)

    #print("Zip Code:")
    if zname_len > 50:
        toTXT("")
    else:
        toTXT(zip2)


    #Industry Parse
    industry_name = data_string2[data_string2.find("""Industry""")+10:data_string2.find("""Source_Code__c""")]

    industry2 = industry_name[industry_name.find(':"')+2:industry_name.find('";')]

    iname_len = len(industry2)

    #print("Industry:")
    if ename_len > 50:
        toTXT("")
    else:
        toTXT(industry2)

    #Source Code Parse
    industry_name = data_string2[data_string2.find("""Source_Code__c""")+15:data_string2.find("""LeadSource""")-1]

    industry2 = industry_name[industry_name.find(':"')+2:industry_name.find('";')]

    iname_len = len(industry2)

    #print("Source Code:")
    if ename_len > 50:
        toTXT("")
    else:
        toTXT(industry2)

    #Lead Source Parse
    industry_name = data_string2[data_string2.find("""LeadSource""")+15:data_string2.find("""Assessment_Request__c""")-1]

    industry2 = industry_name[industry_name.find(':"')+2:industry_name.find('";')]

    iname_len = len(industry2)

    #print("Lead Source:")
    if ename_len > 50:
        toTXT("")
    else:
        toTXT(industry2)

    #Assessment Request Parse
    industry_name = data_string2[data_string2.find("""Assessment_Request__c""")+23:data_string2.find("""formid""")-1]

    industry2 = industry_name[industry_name.find(':"')+2:industry_name.find('";')]

    iname_len = len(industry2)

    #print("Assessment Request:")
    if iname_len > 50:
        toTXT("")
    else:
        toTXT(industry2)


    #LP URL Parse
    industry_name = data_string2[data_string2.find("""lpurl""")+8:data_string2.find("""cr""")+28]

    industry2 = industry_name[industry_name.find(':"')+2:industry_name.find('";')]

    iname_len = len(industry2)

    #print("LP URL:")
    if iname_len > 150:
        toTXT("")
    else:
        toTXT(industry2)


    #Ref URL Parse
    industry_name = data_string2[-500:]

    string_len = len(industry_name)

    string_len = string_len - 2

    string_len = int(string_len)

    string_start = industry_name.find('"_mktoReferrer"')

    string_start = string_start + 23

    string_start = int(string_start)

    industry2 = industry_name[string_start:string_len]



    iname_len = len(industry2)
    
    #In our Marketo instance, the unsubscribe page had changed over time so I manually compared (URL has been replaced by dummy text)
    #This can probably be deleted for your instance
    unsub_value = industry_name.find('company.com/unsubscribe.html')


    if unsub_value != -1:
        toTXT("company.com/unsubscribe.html")
        #print(unsub_value)
    else:
        #print("Industry2:")
        toTXT(industry2)


def test(data):
    Data1 = data
   

    try:

        for i in Data1['result']:
            #print(i['activityDate'])
            toTXT(i['activityDate'])
            #print(i['activityTypeId'])
            toTXT(i['activityTypeId'])
            attributes = (i['attributes'][1]['value'])
            #Attributes within the form responses themselves are passed to another function
            #I'm sure there's a better way to do this but I couldn't figure out how
            attribute_parse(attributes)
            #print(i['id'])
            toTXT(i['id'])
            #print(i['leadId'])
            toTXT(i['leadId'])
            #print(i['marketoGUID'])
            toTXT(i['marketoGUID'])
            #print(i['primaryAttributeValue'])
            toTXT(i['primaryAttributeValue'])
            #print(i['primaryAttributeValueId'])
            toTXT(i['primaryAttributeValueId'])

            print('Calling Postgres Write function . . .')

            postgresWrite()
            
    #Sometimes the API call returned no results on a given page but the following pages would have more responses.
    #I don't know why that is but this catch allows the program to continue to run in the case of a blank page returned with more results after it
    except KeyError:
        print('No results this time, but continuing to call . . .')

def main():
    Data1 = jsonCallFirst()
    test(Data1)
    nToken = getNextToken(Data1)
    mResults = moreCheck(Data1)

    while mResults == True:
        newData = jsonCall(nToken)
        test(newData)
        nToken = getNextToken(newData)
        mResults = moreCheck(newData)


#3600 seconds = run every hour
def timeControl(sc):
    main()
    s.enter(3600, 1, timeControl, (sc,))

s.enter(3600,1,timeControl, (s,))
s.run()
