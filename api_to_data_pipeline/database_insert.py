#PYTHON MODULE TO PREPARE DWAREHOUSE INSERT

import database_insert
import numpy as np

def db_int(integer:int):
    
    if integer is None:
       integer = "Null"
       return integer
    
    if str(integer) == 'nan':
       integer = "Null"
       return integer
       
    if type(integer) is not int:
      integer = "Null"
      return integer
   
    else:
      return integer
    
def db_cnpj(cnpj:str):
    
    if cnpj == "Null":
        cnpj = None
        if cnpj is None:
            cnpj = "Null"
            return cnpj
        
    if type(cnpj) == float:
        cnpj = None     
        if cnpj == None:
            cnpj = "Null"
            return cnpj

    else:
        return "'" + str(cnpj).replace(".","").replace("/","").replace("-","") + "'"
    

def db_float(decimal:float):
    if decimal is None:
        decimal = "Null"
        return decimal
        
    if type(decimal) is not float:
      decimal = "Null"
      return decimal

    if str(decimal) == 'nan':
        decimal = "Null"
        return decimal
    
    else:
        return decimal
    
def db_date(date:str):
    
    if date == "Null":
        date = None
    if date is None:
        date = "Null"
        return date
    if str(date) == 'nan':
        date = "Null"
    if str(date) == "None":
        date = "Null"
    else:
        return "'" + str(date) + "'"


def db_str(string:str):
    import pandas as pd

    if string is None:
        string = "Null"
        return string
    
    if str(string) == 'nan':
        string = "Null"
        return string

    if string == np.nan:
        string = "Null"
        return string
    
    if "Null" in string:
        string = "Null"
    else:
        return "'" + str(string).replace("'","") + "'"
    
def db_bool(boolean:bool):
    if boolean is None:
        boolean = "Null"
        return boolean
    if boolean is True:
        boolean = int(1)
        return boolean
    if boolean is False:
        boolean = int(0)
        return boolean
    if str(boolean) == "True":
        boolean = int(1)
        return boolean
    if str(boolean) == "False":
        boolean = int(0)
        return boolean
