import os
from click import echo, style
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Table, MetaData, and_
from sqlalchemy.orm import Session
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Computed, DateTime, ForeignKey, Integer, Table, Text, desc, select, update
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.orm import relationship
from sqlalchemy.orm import declarative_base, registry
from sqlalchemy import text
from collections.abc import Iterable
import zlib

import pprint
printer = pprint.PrettyPrinter(indent=12, width=180)
prnt = printer.pprint

import math
import datetime
import inspect

from settings import Options
options = Options('settings.ini')

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
		echo(style(text='[Внешние документы].row_id',fg='bright_blue')+' = '+style(text=row_id, fg='bright_cyan'))
		self.db			=	db
		self.row_id		=	row_id
		self.agreemen_id = 0
		self.full_file_path_on_disk, self.agreement_area, self.agreement_folder, self.agreement_number, self.document_type, self.document_number, self.document_date = '', '', '', '', '', '', ''
		header, self.data = self.db.get_data(f"""select ROW_ID, [Документ-Файл], [Счет-Файл], [Вид документа], Комментарий, [Задолженность-Файл], [Договор-Файл], [Задолженность-Файлы], [Оригинальное имя], [Реальное имя], [Наряд-Файл], [Постановление-Файлы], [Движения-Файл], [ДоговорУК-Файл], [Документ владения], [ДокументЛич-Файл], [ИсторияЛиц-Файл], Дата, [ЗаявкаАбонента-Файл], [Организация-Файл], [РН-Файл], ИНТ_Тип, [ДокументТип-Файл], ДатаПодпГП, ДатаПодпКонтрагент, [Документ-ПодписьГП], [Документ-ПодписьКонтрагент], ДатКнц, ДатНач, [Документ-Составитель], [Соглашение-Файл], ТипДокЮР, Версия, Главный, Тип, [АккаунтЛК-Файл], [Документ-Свойства], [Документ-ОргОгр], old_id from stack.[Внешние документы] where row_id = {self.row_id} ;""")
		if len(self.data)>0:
			self.data = self.data[0]
			self.agreemen_id = self.get_agreement_id()
			self.get_agreement_area_folder()

	def is_may_be_unload(self):
		if len(self.data)<=0:
			return False, 'select result is empty'
		if self.data['Вид документа']!=None:
			if self.data['Вид документа']>0:
				return False, "document type isn't zero"
		return True, 'Ok'

	def get_file_data_from_db(self) -> bytes:
		header, data  = self.db.get_data(f"""select [Код файла] from stack.[Внешние документы] where row_id = {self.row_id} ;""")
		if len(data)==0:
			return None
		else:
			return data[0]['Код файла']

	def get_agreement_id(self):
		if self.is_may_be_unload()[0] == False:
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

	def get_store_path(self):
		lc_path_to_folder = options.path
		if len(self.agreement_area)>0: # отделение
			lc_path_to_folder = os.path.join(lc_path_to_folder , self.agreement_area)
			if len(self.agreement_folder)>0: # участок
				lc_path_to_folder = os.path.join(lc_path_to_folder, self.agreement_folder)
				if len(self.agreement_number)>0: # номер договора
					lc_path_to_folder = os.path.join(lc_path_to_folder, self.agreement_number)
				if len(self.document_type + self.document_number)>0:
					lc_path_to_folder = os.path.join(lc_path_to_folder,
													self.document_type + ' ' + self.document_number.replace('/','-').replace('\\','-') + ' ' + f"{oDoc.document_date.year} {'0' if oDoc.document_date.month<10 else ''}{oDoc.document_date.month} {'0' if oDoc.document_date.day<10 else ''}{oDoc.document_date.day}"
													)
		return lc_path_to_folder + os.sep
	
	def create_store_path(self):
		lc_path_to_folder = self.get_store_path()
		try:
			if not os.path.isdir(lc_path_to_folder):
				os.makedirs(lc_path_to_folder)
				echo(style(text='directories ', fg='green')+style(text=lc_path_to_folder, fg='bright_magenta')+style(text=' has been created', fg='green'))
				return ''
			else:
				return ''
		except:
			return 'the directory has not been created'
	
	def save_file_on_disk(self):
		if not self.is_may_be_unload()[0]:
			print('the file cannot be unloaded to disk' + ' ' + self.is_may_be_unload()[1] )
			return False	
		error_text = self.create_store_path()
		if len(error_text)>0:
			print(error_text)
			return False
		self.full_file_path_on_disk = str(os.path.join(self.get_store_path(), self.data['Оригинальное имя']))
		try:
			if not os.path.isfile(self.full_file_path_on_disk):
				file_compressed_source = oDoc.get_file_data_from_db()
				file_handle = open(self.full_file_path_on_disk, 'wb')
				file_handle.write(zlib.decompress(file_compressed_source))
				file_handle.close()
				echo(style(text='file: ', fg='bright_white')+' '+style(text=self.full_file_path_on_disk, fg='bright_yellow')+' '+style(text='is saved', fg='bright_green'))
				return True
			else:
				echo(style(text='file: ', fg='bright_white')+' '+style(text=self.full_file_path_on_disk, fg='bright_yellow')+' '+style(text='is allready exists', fg='bright_green'))
				return False
		except: 
			print('write file error')
			return False
		
	def save_and_null_file_data(self):
		save_result = self.save_file_on_disk()
		if save_result:
			print('---')
			print(self.full_file_path_on_disk)
			print('---')
			path = os.path.split(self.full_file_path_on_disk)[0]
			file_name = os.path.splitext( os.path.split(self.full_file_path_on_disk)[1] )[0]
			ext = os.path.splitext(os.path.split(self.full_file_path_on_disk)[1])[1]
			print(path)
			print(file_name)
			print(ext)
			self.db.session.execute(text(f"update stack.[Внешние документы] set [Оригинальное имя]='{path + os.sep + file_name}', [Реальное имя]='{ext[1:]}', [Код файла]='' where row_id = {self.row_id}; commit;"))
		return save_result






db = DB()




oDoc = OutsideDocument(db, 31190+1)
print(oDoc.save_and_null_file_data())


#print(oDoc.get_file_data_from_db())


