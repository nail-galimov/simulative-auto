import psycopg2

class PGDatabase:
    def __init__(self, host, database, port, user, password):
        self.host = host
        self.database = database
        self.port = port
        self.user = user
        self.password = password
        
        self.connection = psycopg2.connect(
            host = host,
            database=database,
            port=port,
            user=user,
            password=password
        )

        self.cursor = self.connection.cursor()
        self.connection.autocommit = True
    
    def post(self, query, args = ()):
        try:
            self.cursor.execute(query)
        except Exception as e:
            print(repr(e))