from click import echo, style
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Table, MetaData, and_
from sqlalchemy.orm import Session
from sqlalchemy import Integer,  ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Computed, DateTime, ForeignKey, Integer, Table, Text, desc, select, update
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.orm import relationship
from sqlalchemy.orm import declarative_base, registry
from sqlalchemy import text
from collections.abc import Iterable

import pprint
printer = pprint.PrettyPrinter(indent=12, width=180)
prnt = printer.pprint

import math
import datetime
import inspect
from settings import Options

options = Options('options.ini')
#options.LANDDBURI

def get_queryresult_header_and_data(query_result):
	result = []
	for v in query_result:
		drow = {}
		for count, value in enumerate(v._fields):
			drow[value] = v[count]
		result.append(drow)
	headers = []
	if len(result)>0:
		headers = list(result[0].keys())
	return headers, result	                

class DB():
	def __init__(self):
		self.engine = create_engine(options.connection_url_ul)
		self.connection = self.engine.connect()
		self.session = Session(self.engine)

	def get_data(self, sql:str):
		result = self.session.execute(text(sql)).all()
		if len(result)==0:
			header = {}; data=[]
		else:
			header, data = get_queryresult_header_and_data(result)
		return header, data


class OutsideDocument():
	def __init__(self, db:DB, row_id:int):
		self.db			=	db
		self.row_id		=	row_id
		self.agreemen_id = 0
		self.agreement_area, self.agreement_folder, self.agreement_number, self.document_type, self.document_number, self.document_date = '', '', '', '', '', ''
		header, self.data = self.db.get_data(f"""select ROW_ID, [Документ-Файл], [Счет-Файл], [Вид документа], Комментарий, [Задолженность-Файл], [Договор-Файл], [Задолженность-Файлы], [Оригинальное имя], [Реальное имя], [Наряд-Файл], [Постановление-Файлы], [Движения-Файл], [ДоговорУК-Файл], [Документ владения], [ДокументЛич-Файл], [ИсторияЛиц-Файл], Дата, [ЗаявкаАбонента-Файл], [Организация-Файл], [РН-Файл], ИНТ_Тип, [ДокументТип-Файл], ДатаПодпГП, ДатаПодпКонтрагент, [Документ-ПодписьГП], [Документ-ПодписьКонтрагент], ДатКнц, ДатНач, [Документ-Составитель], [Соглашение-Файл], ТипДокЮР, Версия, Главный, Тип, [АккаунтЛК-Файл], [Документ-Свойства], [Документ-ОргОгр], old_id from stack.[Внешние документы] where row_id = {self.row_id} ;""")
		if len(self.data)>0:
			self.data = self.data[0]
			self.agreemen_id = self.get_agreement_id()
			self.get_agreement_area_folder()

	def is_may_be_unload(self):
		if len(self.data)<=0:
			return False
		if self.data['Вид документа']!=None:
			if self.data['Вид документа']>0:
				return False
		return True

	def get_file_data_from_db(self):
		header, data  = self.db.get_data(f"""select [Код файла] from stack.[Внешние документы] where row_id = {self.row_id} ;""")
		if len(data)==0:
			return None
		else:
			return data[0]['Код файла']

	def get_agreement_id(self):
		if self.is_may_be_unload() == False:
			return None
		
		if self.data['Документ-Файл'] != None:
			if self.data['Документ-Файл'] > 0:
				# идентификатор договора по идентификатору документа
				header, data = self.db.get_data(f"""select doc.[Документы-Договор], types.[Название], doc.[Дата], doc.[Полный номер]
														from stack.[Документ] as doc 
														left join stack.[Типы документов] as types on types.row_id = doc.[Тип документа]
														where doc.row_id = {self.data['Документ-Файл']};""")
				if data[0]['Документы-Договор'] != None:
					if data[0]['Документы-Договор'] > 0:
						self.document_type = data[0]['Название']
						self.document_date = data[0]['Дата']
						self.document_number = data[0]['Полный номер']
						return data[0]['Документы-Договор']

		if self.data['Договор-Файл'] != None:
			if self.data['Договор-Файл']>0:
				return self.data['Договор-Файл']


			return None

	def get_agreement_area_folder(self):
		header, data = self.db.get_data(f"""select  stack.[Договор].[Номер] as nc, folders.[Примечание] as folder, folders.area
										from stack.[Договор]
										left join (select sp.row_id, sp.Папки, sp.Примечание, COALESCE (pp.[Примечание], sp.[Примечание]) as area
														from stack.[Договор] sp
														left join (select *
																		from stack.[Договор] 
																		where [Папки] = 80540
																	) as pp on pp.row_id = sp.[Папки] 
										where (sp.Папки_ADD=0 and sp.Заказчик>0) or sp.Папки=-10 ) as folders
										on folders.[row_id] = stack.[Договор].Иерархия2 
										where stack.[Договор].[row_id]={self.agreemen_id}
							;""")
		if len(data)>0:
			self.agreement_number	= data[0]['nc']
			self.agreement_area		= data[0]['area']
			self.agreement_folder	= data[0]['folder']


db = DB()
#header, data = db.get_data("""select top 5 row_id from stack.[Договор]""")
#prnt(header)
#print()
#prnt(data)


#oDoc = OutsideDocument(db, 48)
oDoc = OutsideDocument(db, 182)
print(oDoc.is_may_be_unload())
prnt(oDoc.__dict__)
#print(oDoc.get_file_data_from_db())
