#PYTHON MODULE TO ACCESS FIFFERENT DATABASES

import database_connection

def postgresql_conn(user, password, host, port, database):
    
    from sqlalchemy import create_engine
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    connection = create_engine(connection_string)
    return connection

def microsoftsql_conn(driver, server, database, password, userid):
    
    import pyodbc
    connection_string = (f"DRIVER={driver};"f"SERVER={server};"f"DATABASE={database};"f"PWD={password};"f"UID={userid}")
    connection = pyodbc.connect(connection_string)
    connection = connection.cursor()
    return connection
