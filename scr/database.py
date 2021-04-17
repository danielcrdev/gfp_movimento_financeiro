import mysql.connector

class Database():
    def __init__(self):
        self.host = 'dbgfp.cluster-cdljt5upmmtb.us-east-1.rds.amazonaws.com'
        self.port = '3306'
        self.database = 'DBORCA'
        self.user = 'danielcribeiro'
        self.password = 'danielcribeiro'
    
    def conecta(self):
        self.con = mysql.connector.connect(host=self.host,port=self.port,database=self.database,user=self.user,password=self.password,charset='utf8')

    def desconecta(self):
        self.con.close()

    def commit(self):
        self.con.commit()

    def rollback(self):
        self.con.rollback()

    def abreCursor(self):
        self.cur = self.con.cursor(dictionary=True)

    def executaSQLFetchallCursorAberto(self, sql):
        self.cur.execute(sql)
        res = self.cur.fetchall()
        return res
    
    def executaSQLFetchoneCursorAberto(self, sql):
        self.cur.execute(sql)
        res = self.cur.fetchone()
        return res

    def executaSQLInsertCursorAberto(self, sql):
        self.cur.execute(sql)
        return None

    def executaSQLFetchall(self, sql):
        self.conecta()
        cur = self.con.cursor(dictionary=True)
        cur.execute(sql)
        res = cur.fetchall()
        self.desconecta()
        return res
    
    def executaSQLFetchone(self, sql):
        self.conecta()
        cur = self.con.cursor(dictionary=True)
        cur.execute(sql)
        res = cur.fetchone()
        self.desconecta()
        return res
    
    def executaSQLInsert(self, sql):
        self.conecta()
        cur = self.con.cursor(dictionary=True)
        cur.execute(sql)
        self.commit()
        self.desconecta()
        return None

    def executaSQLUpdate(self, sql):
        self.conecta()
        cur = self.con.cursor(dictionary=True)
        cur.execute(sql)
        self.commit()
        self.desconecta()
        return None

    def executaListaSQLScript(self, script):
        self.conecta()
        cur = self.con.cursor(dictionary=True)
        for sql in script:
            cur.execute(sql)
        self.commit()
        self.desconecta()
        return None
        