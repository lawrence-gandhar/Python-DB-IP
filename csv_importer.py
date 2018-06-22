#
#	Author: LAWRENCE GANDHAR
#	Date : 15 March 2018
#	Project : GEO LOCATION - Inspired From DBIP PHP Script
#
#


import os, sys, csv
import ipaddress
import gzip
import argparse
import psycopg2
import mysql.connector


#******************************************************************************************
# MSSQL Class 
# Requires pymssql Installed
#******************************************************************************************

"""
class DB_MSSQL(object):

	import pymssql

	def __init__(self, hostname = "", username = "", password = "", database = ""):
		
		self.mssql_hostname = hostname
		self.mssql_username = username
		self.mssql_password = password
		self.mssql_database = database
		self.mssql_connection = ''
		self.mssql_cursor = ''
			
	#		
	# Check MSSQL Connection, and return connection and cursor	
	#
	
	def mssql_connect(self):

		try:
			conn = pymssql.connect(self.mssql_hostname, self.mssql_username, self.mssql_password, self.mssql_database)
			
			self.mssql_cursor = conn.cursor()
			self.mssql_connection = conn	
			return self.mssql_connection, self.mssql_cursor, ">>> MSSQL Server %s connected successfully." %self.mssql_hostname
		except:
			return False, False, ">>> MSSQL Server %s connection failed, check for errors."	%self.mssql_hostname


#******************************************************************************************
# MySQL Class
# Requires MySQLdb Installed
#******************************************************************************************	

class DB_MySQL(object):

	import MySQLdb

	def __init__(self, hostname = "", username = "", password = "", database = ""):
		pass

"""

#*****************************************************************************************
# PostgreSQL Class
# Requires psycopg2 Installed
#*****************************************************************************************

class DB_PGSQL(object):

	def __init__(self, pgsql_dsn ="",  hostname = "", username = "", password = "", database = "", tablename = ""):
		
		self.pgsql_dsn = pgsql_dsn
		self.pgsql_hostname = hostname
		self.pgsql_username = username
		self.pgsql_password = password
		self.pgsql_database = database
		self.pgsql_connection = ''
		self.pgsql_cursor = ''
		self.tablename = 'dbip_lookup'

	#
	# Test Connection
	#

	def connect(self):
		try:
			if self.pgsql_dsn!="":
				self.pgsql_connection = psycopg2.connect(self.pgsql_dsn)
			else:
				self.pgsql_connection = psycopg2.connect("dbname='"+self.pgsql_database+"' user='"+self.pgsql_username+"' host='"+self.pgsql_hostname+"' password='"+self.pgsql_password+"'")
			self.pgsql_cursor = self.pgsql_connection.cursor()
			print(">>> Connection Established with PostgreSQL Database")
			return True
		except Exception as e:
			print(">>> Connection Failed with PostgreSQL Database")
			print(">>> Error Report - %s" %(e))
			return False

	#
	# Execute Function
	#
	def execute(self, query=""):
		self.pgsql_cursor.execute(query)

	def commit(self):
		self.pgsql_connection.commit()

	def rollback(self):
		self.pgsql_connection.rollback()	

	#
	# Check Table is empty or not
	#

	def	record_count(self):
		self.pgsql_cursor.execute("select count(*) from "+self.tablename)
		count = self.pgsql_cursor.fetchone()[0]
		self.pgsql_connection.commit()
		if count > 0:
			if count == 1:
				print(">>> Table is not empty. It has %s record" %(count))
			else:
				print(">>> Table is not empty. It has %s records" %(count))	
		else:
			print(">>> Table is empty.")	

	#
	# Truncate Table
	#

	def empty_table(self):
		print(">>> Truncating Table")
		self.pgsql_cursor.execute("truncate table "+self.tablename)
		#self.pgsql_connection.commit()


#******************************************************************************************
# DBIP Class 
#******************************************************************************************

class DBIP():
	
	def __init__(self, filepath = "", connection = "", insert_type = ""):
		
		self.ip_address = ""
		self.filepath = filepath
		self.connection = connection
		self.TYPE = insert_type
		self.row_num = 0

		self.INSERT_TYPE = dict({
			'TYPE_COUNTRY' : ("ip_start", "ip_end", "country",),
			'TYPE_CITY' : ("ip_start", "ip_end", "country", "stateprov", "city"),
			'TYPE_LOCATION' : ("ip_start", "ip_end", "country", "stateprov", "district", "city", "zipcode", "latitude", "longitude", "geoname_id", "timezone_offset", "timezone_name"),
			'TYPE_ISP' : ("ip_start", "ip_end", "country", "isp_name", "connection_type", "organization_name"),
			'TYPE_FULL' : ("ip_start", "ip_end", "country", "stateprov", "district", "city", "zipcode", "latitude", "longitude", "geoname_id", "timezone_offset", "timezone_name", "isp_name", "connection_type", "organization_name"),
		})


		self.__Is_ValidFile()

	#
	# Private Method - File_Exists
	# Check File path
	#
	
	def __File_Exists(self):
		
		if os.path.isfile(self.filepath):
			print(">>> File : %s  - Found " %(self.filepath))
			return True
		else:
			print(">>> Invalid Path/File : %s" %(self.filepath))
			return False


	#
	# Private Method - Is_Compressed 
	# Check If File is Compressed  or not
	#

	def __Is_Compressed(self):
		
		file_extension = self.filepath.split(".")[-1]

		if file_extension == "gz":
			return True, file_extension, '>>> Valid File extension: %s' % file_extension
		elif file_extension == "csv" or file_extension == "CSV":
			return False, file_extension, '>>> Valid File extension: %s' % file_extension
		else:	
			return False, "", ">>> Not a Valid File. Only CSV/GZ Filetype is allowed"


	#
	# Private Method
	# Check File Validity - Return True if file is a tar.gz or csv
	#

	def __Is_ValidFile(self):
		
		print(">>> Performing File Operations ... ")

		file_exists = self.__File_Exists()

		if file_exists:
			ret, file_extension, msg = self.__Is_Compressed()
			
			if not ret and file_extension!="":
				print(msg)
				print(">>> Reading the file ...")
				return True
			elif not ret and file_extension=="":
				print(msg)
				sys.exit()
			else:
				print(msg)
				self.__Decompress_File()
				return True			
		return False

	#
	# Private Method - Decompress_File
	# Reading gzip file
	# 		
	
	def __Decompress_File(self):
		
		print(">>> Decompressing the file ...")
		with gzip.open(self.filepath, 'rb') as f:
			for line in f:
				self.__DB_Insert(line.decode('utf-8'))
			self.connection.commit()

	#	
	# Private Method - Addr_Type 
	# ("__" denotes that the method is a private method and is not accessible outside the class)
	#
	# Give Address Version i.e IP is whether IPV4 or IPV6
	#
		
	def __Addr_Type(self, ip_address):
		
		ADDR_TYPE = ""
		try:
			ADDR_TYPE = ipaddress.ip_address(ip_address).version
			return True, ADDR_TYPE
		except:
			return False, ADDR_TYPE

	#
	# Private Method - DB_Insert
	# Reading gzip file
	# 

	def __DB_Insert(self,datax):
		datax = datax.replace("\r\n","").split('","')
		data = [x.replace("'","''").replace('"','') for x in datax]

		#data = datax.replace("'","''").replace('"','').replace("\r\n","").split(',')

		self.row_num += 1

		ADDR_TYPE_STATUS, ADDR_TYPE  = self.__Addr_Type(data[0])
		
		if ADDR_TYPE_STATUS:
			data.append("ipv"+str(ADDR_TYPE))
		else:
			print (">>> Unknown Address Type for IP Address : %s" %(data[0]))
			sys.exit()


		if len(self.INSERT_TYPE[self.TYPE]) == len(data)-1:
			sql = "insert into %s (%s, addr_type) values('%s')" %(self.connection.tablename, ', '.join(self.INSERT_TYPE[self.TYPE]), "','".join(data))
			try:
				self.connection.execute(sql)
				sys.stdout.write("\r>>> Records Inserted. %s" %(self.row_num))
				sys.stdout.flush()
			except:
				self.connection.rollback()
				print(">>> Invalid CSV record on row no. %s" %(self.row_num))
				print(">>> System restoring to previous state")
				sys.exit()
		else:
			self.connection.rollback()
			print(">>> Column count mismatch occrred on row no. %s" %(self.row_num))
			print(">>> System restoring to previous state")
			sys.exit()

		
#
# This function executes when use_pgsql is selected 
#
def connect_pgsql(args):
	DATABASE = ""
	HOST = ""
	USER = ""
	PSW = ""
	error = []

	pgsql_connect = ""

	#
	# Check for DSN for connection
	if args.dsn is not None:
		
		if args.database is not None or args.psw is not None or args.user is not None:
			print(">>> For PostgreSQL use only a single connection type. Use either dsn or only the individual parameters for connection.\nPlease follow the documentation for more details.")
			sys.exit()
		else:
			pgsql_connect = DB_PGSQL(pgsql_dsn=args.dsn)
			ret = pgsql_connect.connect()

			if ret:
				pgsql_connect.record_count()
	else:
		#
		# Check for other parameters for connection
		
		if args.psw is not None:
			PSW = args.psw
		else:
			error.append("Password is required") 

		if args.user is not None:
			USER = args.user
		else:
			error.append("Username is required")

		if args.database is not None:
			DATABASE = args.database
		else:
			error.append("Database name is required")	

		if args.host is not None:
			HOST = args.host
		else:
			error.append("Hostname is required")		

	#
	# If DSN is present along with other connection parameters
	if len(error)==4 and args.dsn is None:
		error.append("Use either dsn or only the individual parameters for connection")
		print(">>> Errors : %s" %(', '.join(error[::-1]))) 
		sys.exit()

	#
	# If DSN is missing but others are there
	if len(error)>0:
		print(">>> Errors : %s" %(', '.join(error[::-1])))
		sys.exit()

	if HOST !="" and DATABASE !="" and USER !="" and PSW !="":
		pgsql_connect = DB_PGSQL(hostname=HOST, username=USER, password=PSW, database=DATABASE)
		ret = pgsql_connect.connect()

		if ret:
			pgsql_connect.record_count()

	required_func(pgsql_connect, args)


def connect_mssql(xy):
	pass

def connect_mysql(xy):
	pass


def required_func(connection, args):

	#
	# Check For --clear-table and --update-table
	if args.clear_table:
		connection.empty_table()
	if args.update_table:
		print(">>> Insertion of new records initiated. Not a good idea, may insert duplicate records.")	

	#
	# Check for --file
	if args.file is not None and args.insert_type is not None:
		check_file = DBIP(args.file, connection, args.insert_type)


#***********************************************************************************************************
# Main
#***********************************************************************************************************

if __name__ == "__main__":

	#
	# Using ArgsParser for parsing command line arguments
	#
	parser = argparse.ArgumentParser(description = 'DBIP Python Tool')

	group1 = parser.add_mutually_exclusive_group(required = True)

	group1.add_argument("--clear-table", 
		action = "store_true", 
		help = "Truncate the table before inserting new data",)

	group1.add_argument("--update-table", 
		action = "store_true", 
		help = "Insert new data into the table. Previous records will not be effected",)

	parser.add_argument("--file", 
		action = "store", 
		help = "Provide the complete filepath and filename", 
		required =True,)

	parser.add_argument("--insert-type", action="store", 
		help = "Use any one for --insert-type",
		choices = ('TYPE_COUNTRY', 'TYPE_CITY', 'TYPE_LOCATION', 'TYPE_ISP', 'TYPE_FULL',),
		required = True,)

	#
	# Defining Sub Parsers
	#
	subparsers = parser.add_subparsers(help='type use_pgsql/use_mssql/use_mysql --help for database connection sub-command help')

	#
	# Defining Sub Parsers for connecting different database engines
	#
	parser_mysql = subparsers.add_parser('use_mysql') # MySQL Sub-Parser
	parser_pgsql = subparsers.add_parser('use_pgsql') # PostgreSQL Sub-Parser
	parser_mssql = subparsers.add_parser('use_mssql') # MSSQL Sub-Parser

	parser_mysql.add_argument("--host", help="Hostname", action = "store", required = True,)
	parser_mysql.add_argument("--database", help="Database Name", action = "store", required = True,)
	parser_mysql.add_argument("--user", help="Database Username", action = "store", required = True,)
	parser_mysql.add_argument("--psw", help="Database Password", action = "store", required = True,)
	parser_mysql.add_argument("--table", help="Tablename", action = "store",)
	parser_mysql.set_defaults(func=connect_mysql)

	parser_mssql.add_argument("--host", help="Hostname", action = "store", required = True,)
	parser_mssql.add_argument("--database", help="Database Name", action = "store", required = True,)
	parser_mssql.add_argument("--user", help="Database Username", action = "store", required = True,)
	parser_mssql.add_argument("--psw", help="Database Password", action = "store", required = True,)
	parser_mssql.add_argument("--table", help="Tablename", action = "store",)
	parser_mssql.set_defaults(func=connect_mssql)


	# DSN / Normal arguments for usage - Only PostgreSQL
	parser_pgsql.add_argument("--dsn", help="Use only for PostgreSQL. Example: --dsn \"dbname='DATABASE' user='USERNAME' host='HOST' password='PASSWORD'\"", action = "store",)
	parser_pgsql.add_argument("--host", help="Hostname", action = "store",)
	parser_pgsql.add_argument("--database", help="Database Name", action = "store",)
	parser_pgsql.add_argument("--user", help="Database Username", action = "store",)
	parser_pgsql.add_argument("--psw", help="Database Password", action = "store",)
	parser_pgsql.add_argument("--table", help="Tablename", action = "store",)
	parser_pgsql.set_defaults(func=connect_pgsql) 

	try:
		# This is used for calling the appropriate function after argument parsing is complete
		args = parser.parse_args()
		args.func(args)
	except Exception as msg:
		parser.error(msg)
