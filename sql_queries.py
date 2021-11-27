pollution_table = ("""
    CREATE TABLE IF NOT EXISTS pollution 
    (id SERIAL PRIMARY KEY, 
    location VARCHAR, 
    city VARCHAR, 
    country VARCHAR,
    parameter VARCHAR, 
    value REAL, 
    lastUpdated VARCHAR,
    unit VARCHAR)""")



pollution_table_insert = ("""
    INSERT INTO pollution 
    (location,city,country,parameter,value,
    lastUpdated, unit)
    VALUES (%s,%s,%s,%s,%s,%s, %s)
    ON CONFLICT DO NOTHING
    """)


create_tables_queries = [pollution_table]