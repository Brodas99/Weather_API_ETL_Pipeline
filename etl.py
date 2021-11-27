import config 
import psycopg2
import psycopg2.extras
from psycopg2 import Error
from create_tables import execute_read_query

from sql_queries import pollution_table_insert


def get_data():
    import json
    from datetime import datetime 
    import os
    import requests


    etl_data = requests.get('https://api.openaq.org/v2/latest?country=US&limit=10000')

    # If the API call was sucessful, get the json and dump it to a file with 
	# today's date as the title.
    if etl_data.status_code == 200:

        # Get the json data 
        pollution_json = etl_data.json()
        file_name  = str(datetime.now().date()) + '.json'
        tot_name   = os.path.join(os.path.dirname(__file__), 'data', file_name)
        
        if os.path.exists(tot_name):
            print ("File exist")
        else:
            print (f"File not exist - inserting new file {tot_name}")
            with open(tot_name, 'w') as outputfile:
                
                json.dump(pollution_json, outputfile)
                print(f"File {tot_name} has been submittied")
            
    else :
        print("Error In API call.")

    return pollution_json




def apiPollution(pollution_reults_json):
    import pandas as pd
    import json
    
    
    air_list = []
    pollution_reults_json = pollution_reults_json['results']
   
    for data in pollution_reults_json:
        for measurement in data['measurements']:
            air_dict = {}
            air_dict['location'] = data['location']
            air_dict['city'] = data['city']
            air_dict['country'] = data['country']
            air_dict['parameter'] = measurement['parameter']
            air_dict['value'] = measurement['value']
            air_dict['lastUpdated'] = measurement['lastUpdated']
            air_dict['unit'] = measurement['unit']
            air_list.append(air_dict)
        
    df = pd.DataFrame(air_list, columns=air_dict.keys())
    #print(df)
    return df 



def load_data(df, connection, cursor):

    # example to manage PostgreSQL transactions
    try:

        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")


        # Executing a SQL query
        version_comment = "SELECT version();"
        record = execute_read_query(connection, version_comment)
        
        # # Fetch result
        # record = cursor.fetchone()
        print("You are connected to - ", record, "\n")


        # ______________________________________________________________________________________________

        for i, row in df.iterrows():
            cursor.execute(pollution_table_insert, list(row))

        print("Transaction completed successfully ")
        connection.commit()


    # rollback if we have an issue
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in transction Reverting all other operations of a transction ", error)
        connection.rollback()
        

    # closing database connection
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def main():
    connection = psycopg2.connect(host=config.db_host, database=config.db_name, user=config.db_user, password=config.db_password)
    cursor = connection.cursor()
    connection.autocommit = False
    pollution_reults_json = get_data()
    df = apiPollution(pollution_reults_json)

    load_data(df, connection, cursor)


    cursor.close()


if __name__ == "__main__":
    main()