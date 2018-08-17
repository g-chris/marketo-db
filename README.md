# marketo-db

Probably too much another user will have to customize to be considered "plug and play" 
but my hope is it can provide a good start for anyone interested in moving data from Marketo to another database.

***Note on PII (Personally Identifiable Information): This example uses the "email" field from marketo. 
I no longer use that in production and instead would use a marketo ID instead.  

Libraries to install:
-pyscopg2

Pull activity information from Marketo into a Postgres DB.

This program in the Main loop:
1. Get initial token from API call
2. Parses and loads first page of results into a DB
3. Checks if first page returned "True" or "False" for "More Results"
4. Loops through API calls with "Next Page Token" until "More Results" is False

Program is current set to run every 3600 seconds (every hour)

Currently uses Marketo Activity Type ID "2" (Fill Out Form) to track activity across landing pages.

A list of Activity Type IDs can be found here: https://nation.marketo.com/thread/40794-activity-type-id-list

Marketo REST API documentation here: http://developers.marketo.com/rest-api/

Besides replacing #TODOs with endpoints,host names and passwords, the "attribute_parse" and "postgresWrite" functions will have to be heavily modified depending on what fields your particular Marketo forms use (if you're looking for form fill activity at all)
