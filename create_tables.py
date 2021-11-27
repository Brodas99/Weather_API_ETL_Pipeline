import config
import psycopg2 
from psycopg2 import OperationalError
from sql_queries import create_tables_queries


#fucntion to connection to database
def create_connection():
    connection = None

    try: 
        #1) Connect to an existing database
        connection = psycopg2.connect(
            database = config.db_name,
            user = config.db_user,
            password = config.db_password,
            host = config.db_host,
            port = config.db_port,
        )
        cursor = connection.cursor()
        print(f'Connection to the PostgresSQL SB successful - connected to database: {connection}')
    
    except OperationalError as e:
        print(f"Error: '{e}' has occured")

    return connection

#function to excute our basic queries 
def execute_query(connection, query):
    #connection.autocommit = None
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")


    except OperationalError as e:
        print(f"The error '{e}' occurred")


#function to excute our basic queries - with sql_queries 
def create_tables(connection):

    for query in create_tables_queries:
        execute_query(connection, query)
        connection.commit()


#function to read our basic queries and print
def execute_read_query(connection, query):
    connection.autocommit = None
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")



def main():


    connection = create_connection()
    create_tables(connection)


    connection.close()




if __name__ == "__main__":
    main()