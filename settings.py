import configparser 
import sys
import os
from sqlalchemy.engine import URL
from click import echo, style
import pprint
printer = pprint.PrettyPrinter(indent=12, width=180)
prnt = printer.pprint


class Options:
	def __init__(self, path:str):
		self.config = configparser.ConfigParser()
		self.config.read(path)
		self.path	= self.config[sys.platform]["unload_root_path"]
		print(f'unload path: {self.path}   {os.path.isdir(self.path)}')
		if not os.path.isdir(self.path):
			echo(style(text=f"directory {self.path} doesn't exist", bg='blue', fg='bright_red'))
			exit()
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
				
