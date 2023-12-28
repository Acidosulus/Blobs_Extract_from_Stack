import configparser 
import sys
from sqlalchemy.engine import URL
import pprint
printer = pprint.PrettyPrinter(indent=12, width=180)
prnt = printer.pprint


class Options:
	def __init__(self, path:str):
		self.config = configparser.ConfigParser()
		self.config.read(path)
		#self.SELF_ADRESS = self.config[sys.platform]["webserver"]
		#self.API_ADRESS = self.config[sys.platform]["apiserver"]
		#self.LANDDBURI = self.config[sys.platform]["langdb"]
		#self.SECRET_KEY = self.config[sys.platform]["SECRET_KEY"]
		
		#	print(f'SELF_ADRESS:{self.SELF_ADRESS}')
		#print(f'API_ADRESS:{self.API_ADRESS}')
		#print(f'LANDDBURI:{self.LANDDBURI}')
		if sys.platform == 'linux':
			self.connection_url_ul = URL.create(
			"mssql+pyodbc",
			username="КАЗАКОВЦЕВ_НМ",
			password="1",
			host="10.19.50.11",
			port=1433,
			database="atom_khk_ul_test",
			query={
				"driver": "ODBC Driver 18 for SQL Server",
				"TrustServerCertificate": "yes"	},
		)
		else:
			self.connection_url_ul = URL.create(
			"mssql+pyodbc",
			username="КАЗАКОВЦЕВ_НМ",
			password="1",
			host="10.19.50.11",
			port=1433,
			database="atom_khk_ul_test",
			query={
				"driver": "SQL Server",
				"TrustServerCertificate": "yes"	},
		)
		print()
		prnt(self.connection_url_ul)
		print()
				
