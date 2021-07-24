import teradata
import pyodbc
import pandas as pd

class Database:
    
    def __init__(self, driver, host, database_, user='', password='', **kwargs):
        
        self.driver = driver
        self.host = host
        self.db_name = database_
        self.user = user
        self.password = password
        self.db = None

    def _connect(self):
        raise NotImplementedError()

    def collect(self, sql):
        self._connect()
#        cursor = self.db.cursor()
        try:
            res = pd.read_sql(sql, con=self.db)
#            cursor.commit()
        except Exception as e:
            print(sql)
#            cursor.rollback()
            raise e   
        finally:
#            cursor.close()
            self._disconnect()
        return res

    def execute(self, sql):
        raise NotImplementedError()

    def execute_many(self, sql, params):
        raise NotImplementedError()
        
    def _disconnect(self):
        if self.db is not None:
            self.db.close()
            self.db = None
            
    def get_connection(self):
        self._connect()
        return self.db
        
    def __del__(self):
        self._disconnect()


class MsSqlDatabase(Database):
    

    def __init__(self, driver, host, database_, user='', password='', **kwargs):
        super().__init__(driver, host, database_, user, password, **kwargs)

    def _connect(self):
        con_string = 'DRIVER={'+self.driver+'};'
        con_string += 'SERVER='+self.host+';'
        con_string += 'DATABASE='+self.db_name+';'
        if self.user+self.password=='':
            con_string+='Trusted_Connection=yes'
        else:
            con_string+=f'user={self.user};password={self.password}'
        #print(con_string)
        self.db = pyodbc.connect(con_string)
        
    
    def execute(self, sql):
        self._connect()
        cursor = self.db.cursor()
        try:
            cursor.execute(sql)
            cursor.commit()
        except Exception as e:
            print(sql)
#            cursor.rollback()
            raise e   
        finally:
            cursor.close()
            self._disconnect()

    def execute_many(self, sql, params):
        self._connect()
        cursor = self.db.cursor()
        # cursor.fast_executemany = True
        try:
            cursor.executemany(sql, params)
            cursor.commit()
        except Exception as e:
            print(sql)
            print(params)
            cursor.rollback()
            raise e 
        finally:
            cursor.close()
            self._disconnect()
            

class TeradataSql:
    
    def __init__(self, driver, host, database_, user='', password='', **kwargs):
        super().__init__(driver, host, database_, user, password, **kwargs)

    def collect(self, sql):
        self._connect()
#        cursor = self.db.cursor()
        try:
            res = pd.read_sql(sql, con=self.db)
#            cursor.commit()
        except Exception as e:
            print(sql)
#            cursor.rollback()
            raise e   
        finally:
#            cursor.close()
            self._disconnect()
        return res  
      
    def execute(self, sql):
        return None

    def execute_many(self, sql, params):
        return None    

        
        
class DatabaseFactory:
    __con_strings: dict 
    __databases: dict
    
    def __init__(self):
        self.__con_strings =    {
                                'CLIENT':  {
                                             'driver':'ODBC Driver 13 for SQL Server',
                                             'host':'*ip сервера*',
                                             'database_': '*база данных*',
                                             },

        self.__databases = {
                            'CLIENT': MsSqlDatabase,
                           }
        
    def change_db(self, server: str, db_name: str):
        if server in self.__con_strings:
            self.__con_strings[server]['database_']=db_name
        
    def build(self, server: str, login: str='', password: str=''):
        server = server.upper()
        if server not in self.__con_strings:
            raise Exception('Неверное имя '+ server +', для выбора доступны следующие: ' + ', '.join(self.__con_strings.keys()))
        if login + password == '':
            return self.__databases[server](**self.__con_strings[server]
                                            )
        return self.__databases[server](**self.__con_strings[server],
                                             login=login,
                                             password=password
                                            )
        
        