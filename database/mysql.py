import pymysql.cursors

class MySQL ():

    def __init__ (self, server:str, database:str, username:str, password:str):
        """ Connect with mysql db

        Args:
            server (str): server host
            database (str): database name
            username (str): database username
            password (str): database password
        """

        self.server = server
        self.database = database
        self.username = username
        self.password = password
        
        self.connection = None
        self.cursor = None

    def run_sql (self, sql:str, auto_commit:bool=True, raise_errors:bool=True) -> list:
        """ Exceute sql code
            Run sql code in the current data base, and commit it
            
        Args:
            sql (str): sql code to run
            auto_commit (bool, optional): commit changes. Defaults to True.
            raise_errors (bool, optional): raise errors running sql. Defaults to False.
            
        Returns:
            list: results of the sql code (like select)
        """
        
        # Validate if connection is open
        if not self.connection or not self.connection.open:
            
            # Connect and get cursor
            self.connection = pymysql.connect(host=self.server,
                                        user=self.username,
                                        database=self.database,
                                        passwd=self.password,
                                        cursorclass=pymysql.cursors.DictCursor)

        self.cursor = self.connection.cursor()
        
        # Replce "None" columns to "NULL"
        sql = sql.replace ('"None"', 'NULL').replace("None", "NULL")

        # Try to run sql
        try:
            self.cursor.execute (sql)
        except Exception as err:

            if raise_errors:
                raise err
            else:
                print(err, sql)

            return None

        # try to get returned part
        try:
            results = self.cursor.fetchall()
        except:
            results = None

        # Commit and close by default
        if auto_commit:
            self.commit_close ()
            
        return results
    
    def get_clean_text (self, text:str, keep:list=[]) -> str():
        
        # Fix none values
        if not text:
            return "NULL"
        
        chars = [";", "--", "\b", "\r", "\t", "\n", "\f", "\v", "\0", "'", '"']
        
        # Ignore chats to keep
        for char in keep:
            chars.remove (char)
        
        for char in chars:
            text = text.replace(char, "")
            
        return f'"{text}"'
    
    def commit_close (self): 
        """ Commit changes and close connection """
        
        self.connection.commit()
        self.connection.close()
        
        