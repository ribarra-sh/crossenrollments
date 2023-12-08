import pymysql
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import logging
import pytz
from datetime import datetime



class Conexion():
    
    def __init__(self, address):
        self.ssh_hostname = address[0]
        self.ssh_username = address[1]
        self.ssh_password = address[2]
        self.db_hostname = address[3]
        self.db_port = address[4]
        self.db_username = address[5]
        self.db_password = address[6]
        self.db_database = address[7]
        
        self.Conexion_ssh()
        self.conn = None  # Initialize conn attribute to None
       


#modificado el 02-11-2023 para que la cone
    def Conexion_ssh(self):
       
        global tunnel
        

        
        self.tunnel = SSHTunnelForwarder(
            (self.ssh_hostname, 22),
            ssh_username=self.ssh_username,
            ssh_pkey=self.ssh_password, #Adaptado a PEM KEY, en teoria el valor deberia agrgarse en Address
            remote_bind_address=(self.db_hostname, self.db_port)
            )
        try:
            self.tunnel.start()
            logging.info("SSH connection established successfully.")
        except Exception as e:
            logging.error(f"Failed to establish SSH connection: {e}")
            print (e)
            # You might want to raise an exception or handle the error accordingly.
            # For now, I'm just raising the exception to halt the execution.
            raise e

    
    def Conexion_mysql(self):
      
        global conn
        


        try:
            conn = pymysql.connect(
                host='127.0.0.1',
                port=self.tunnel.local_bind_port,
                user=self.db_username,
                password=self.db_password,
                database=self.db_database
            )
            self.conn = conn
            logging.info("MySQL connection established successfully.")
        except Exception as e:
            logging.error(f"Failed to establish MySQL connection: {e}")
            raise
            
    # def Conexion_posgresql(self):
      
    #     global conn

    #     try:
    #         conn = psycopg2.connect(
    #             host=self.db_hostname,
    #             port=tunnel.local_bind_port,
    #             user=self.db_username,
    #             password=self.db_password,
    #             database=self.db_database
    #         )
    #         self.conn = conn
    #         logging.info("PostgreSQL connection established successfully.")
    #     except Exception as e:
    #         logging.error(f"Failed to establish PostgreSQL connection: {e}")
    #         raise



    def close_ssh(self):
        try:
            self.tunnel.close()
            logging.info("SSH tunnel closed.")
        except Exception as e:
            logging.error(f"Failed to close SSH tunnel: {e}")
            raise
    
    def close_conn(self):
        try:
            if self.conn is not None:
                self.conn.close()
                logging.info("Database connection closed.")
        except Exception as e:
            logging.error(f"Failed to close database connection: {e}")
            raise
    
    def queryToDb(self, query):
        try:
            if self.conn is not None:
                df = pd.read_sql_query(query, self.conn)
                return df
            else:
                logging.warning("No database connection available for query execution.")
                return None
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise
   
    
   
class TimeZoneConverter:
    def __init__(self, from_timezone, to_timezone):
        self.from_timezone = pytz.timezone(from_timezone)
        self.to_timezone = pytz.timezone(to_timezone) 
   
    
    def convert_utc_to_local(self, utc_datetime):
        # Convert the UTC timestamp to a string in the format '%Y-%m-%d %H:%M:%S'
        utc_datetime_str = utc_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert the UTC timestamp string to a datetime object
        utc_datetime = datetime.strptime(utc_datetime_str, '%Y-%m-%d %H:%M:%S')
        
        # Set the UTC timezone for the datetime object
        utc_datetime = self.from_timezone.localize(utc_datetime)
        
        # Convert the UTC datetime to the desired local timezone
        local_datetime = utc_datetime.astimezone(self.to_timezone)
        
        # Format the local datetime as a string
        return local_datetime.strftime('%Y-%m-%d %H:%M:%S')


    def convert_column_utc_to_local(self, dataframe, column_name):
        # Apply the conversion function to the specified column
        dataframe[column_name] = dataframe[column_name].apply(self.convert_utc_to_local)
        return dataframe
    
    def format_datetime_without_timezone(self, datetime_obj):
            # Format the datetime object without the time zone abbreviation
            return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')   
    
   
# class Database_Handler():
        
#     def __init__(self, conn=None, google_sheets_api_json_key_path=None):
#         self.conn = conn
#         self.google_sheets_api_json_key_path = google_sheets_api_json_key_path



#     #     #INSERTS JUST THE MOST RECENT DATE IN THE DATAFRAME
#     def insert_dataframe(self, dataframe, table_name):
#         # Convert 'fecha' to datetime format (if not already done before)
#         dataframe['fecha'] = pd.to_datetime(dataframe['fecha'], format='%d/%m/%Y')

#         # Get the current date when the code is executed
#         current_date = datetime.date.today()

#         # Filter the 'dataframe' to include only rows corresponding to the current date
#         data_today = dataframe[dataframe['fecha'].dt.date == current_date]

#         if data_today.empty:
#             print(f"No data available for the current date: {current_date}")
#             return

#         # Perform the INSERT INTO operation with the most recent data
#         insert_query = f"INSERT INTO {table_name} VALUES %s"
#         data_rows = [tuple(int(value) if isinstance(value, np.uint32) else value for value in row) for row in data_today.itertuples(index=False, name=None)]

#         with self.conn.cursor() as cursor:
#             from psycopg2.extras import execute_values  # Import execute_values directly
#             try:
#                 execute_values(cursor, insert_query, data_rows)
#                 self.conn.commit()
#                 logging.info(f"Data inserted successfully into table: {table_name}")
#             except Exception as e:
#                 logging.error(f"Failed to insert data into table {table_name}: {e}")
#                 # You might want to raise an exception or handle the error accordingly.
#                 # For now, I'm just raising the exception to halt the execution.
#                 raise e
       
#     def insert_dataframe_atd(self, dataframe, table_name):
#         # Convert 'fecha' to datetime format (if not already done before)
#         dataframed = dataframe.copy()
#         dataframed['FECHA'] = pd.to_datetime(dataframed['FECHA'], format='%d/%m/%Y')

#         # Get the current date when the code is executed
#         current_date = datetime.date.today()

#         # Filter the 'dataframe' to include only rows corresponding to the current date
#         data_today = dataframed[dataframed['FECHA'].dt.date == current_date]

#         if data_today.empty:
#             print(f"No data available for the current date: {current_date}")
#             return

#         # Perform the INSERT INTO operation with the most recent data
#         insert_query = f"INSERT INTO {table_name} VALUES %s"
#         data_rows = [tuple(int(value) if isinstance(value, np.uint32) else value for value in row) for row in data_today.itertuples(index=False, name=None)]

#         with self.conn.cursor() as cursor:
#             from psycopg2.extras import execute_values  # Import execute_values directly
#             try:
#                 execute_values(cursor, insert_query, data_rows)
#                 self.conn.commit()
#                 logging.info(f"Data inserted successfully into table: {table_name}")
#             except Exception as e:
#                 logging.error(f"Failed to insert data into table {table_name}: {e}")
#                 # You might want to raise an exception or handle the error accordingly.
#                 # For now, I'm just raising the exception to halt the execution.
#                 raise e            
                
                
                
#     def delete_dataframe_atd(self, table_name):
#         # Get the current date when the code is executed
#         current_date = dt.datetime.today().strftime("%d-%m-%Y")
    
#         # Create a SQL DELETE query to remove rows for the current date
#         delete_query = f"""
#             DELETE FROM {table_name}
#             WHERE "FECHA" = %s
#         """
    
#         try:
#             # Execute the DELETE query
#             with self.conn.cursor() as cursor:
#                 cursor.execute(delete_query, (current_date,))
            
#             # Commit the changes to the database
#             self.conn.commit()
#             logging.info(f"Data for the current date deleted successfully from table: {table_name}")
    
#         except (Exception, psycopg2.DatabaseError) as error:
#             logging.error(f"Error deleting data for the current date from table {table_name}: {error}")
#             # You might want to raise an exception or handle the error accordingly.
#             # For now, I'm just raising the exception to halt the execution.
#             raise error
#     #    #INSERTS ALL DATES WITHIN THE DATAFRAME
#     def insert_dataframe_history(self, dataframe, table_name):
#       insert_query = f"INSERT INTO {table_name} VALUES %s"
#       # Convert 'numpy.uint32' to Python integers
#       data_rows = [tuple(int(value) if isinstance(value, np.uint32) else value for value in row) for row in dataframe.itertuples(index=False, name=None)]
    
#       with self.conn.cursor() as cursor:
#           from psycopg2.extras import execute_values  # Import execute_values directly
#           execute_values(cursor, insert_query, data_rows)
#           self.conn.commit()        
             
             
#     def delete_duplicates(self, table_name, unique_columns):
#         # Create a list of column names enclosed in double quotes as a comma-separated string
#         columns_str = ', '.join(f'"{col}"' for col in unique_columns)
    
#         # Create a SQL DELETE query to remove duplicates, keeping only the most recent occurrence
#         delete_query = f"""
#             DELETE FROM {table_name}
#             WHERE (ctid, fecha) NOT IN (
#                 SELECT DISTINCT ON ({columns_str}, fecha) ctid, fecha
#                 FROM {table_name}
#                 ORDER BY {columns_str}, fecha DESC
#             )
#         """
    
#         try:
#             # Execute the DELETE query
#             with self.conn.cursor() as cursor:
#                 cursor.execute(delete_query)
    
#             # Commit the changes to the database
#             self.conn.commit()
#             logging.info(f"Duplicates deleted successfully for table: {table_name}")

#         except (Exception, psycopg2.DatabaseError) as error:
#             logging.error(f"Error deleting duplicates for table {table_name}: {error}")
#             # You might want to raise an exception or handle the error accordingly.
#             # For now, I'm just raising the exception to halt the execution.
#             raise error

        



# class GoogleSheetsHandler:
#     def __init__(self, service_account_key_path):
#         self.service_account_key_path = service_account_key_path
        
        
#     def access_google_sheets(self, spreadsheet_key, worksheet_name):
#         try:
#             gc = gspread.service_account(filename=self.service_account_key_path)
#             sh = gc.open_by_key(spreadsheet_key)
#             worksheet = sh.worksheet(worksheet_name)
#             return worksheet
#         except Exception as e:
#             return str(e)        
        
        
      

#     def create_dataframe_from_headcount(self):
#         try:
#             spreadsheet_key = "1ZhMoVIaaj2Rqftv1MBvF22OQguBMz6oxySmP1FoYX70"
#             worksheet_name = "Headcount"
            
#             worksheet = self.access_google_sheets(spreadsheet_key, worksheet_name)
            
#             # Find the first blank row below D10
#             values = worksheet.col_values(4)  # Column D is index 4 (0-based)
#             start_row = 10  # Start at row 10
            
#             for i, value in enumerate(values[9:], start=10):
#                 if not value:
#                     break
#                 start_row += 1
            
#             # Calculate the number of non-blank rows
#             num_rows = start_row - 10  # Subtract the starting row
            
#             # Define the range for the DataFrame
#             data_range = f'D10:I{start_row - 1}'  # Adjusted for 0-based index
            
#             # Get the values from the defined range
#             data = worksheet.get(data_range)
            
#             # Convert the data to a DataFrame
#             df = pd.DataFrame(data, columns=['Column D', 'Column E', 'Column F', 'Column G', 'Column H', 'Column I'])
            
#             return df
#         except Exception as e:
#             return str(e)

#     def create_dataframe_from_justificantes(self):
#         try:
#             spreadsheet_key = "15HbrfHjPJp5v8-CvtlxGvMAZ_bl2yglb5XZOZmdhCfA"
#             worksheet_name = "Justificantes"
            
#             worksheet = self.access_google_sheets(spreadsheet_key, worksheet_name)
            
#             # Find the first blank row below B7
#             values = worksheet.col_values(2)  # Column B is index 2 (0-based)
#             start_row = 7  # Start at row 7
            
#             for i, value in enumerate(values[6:], start=7):
#                 if not value:
#                     break
#                 start_row += 1
            
#             # Calculate the number of non-blank rows
#             num_rows = start_row - 7  # Subtract the starting row
            
#             # Define the range for the DataFrame
#             data_range = f'B7:H{start_row - 1}'  # Adjusted for 0-based index
            
#             # Get the values from the defined range
#             data = worksheet.get(data_range)
            
#             # Convert the data to a DataFrame
#             df = pd.DataFrame(data, columns=['Column B', 'Column C', 'Column D', 'Column E', 'Column F', 'Column G', 'Column H'])
            
#             return df
#         except Exception as e:
#             return str(e)




#     def insert_dataframe_into_google_sheets(self, spreadsheet_key, worksheet_name, df):
#         try:
#             worksheet = self.access_google_sheets(spreadsheet_key, worksheet_name)
#             worksheet.update([df.columns.values.tolist()] + df.values.tolist(), raw=True)  # Insert the DataFrame into the worksheet
#             return "DataFrame inserted successfully."
#         except Exception as e:
#             return str(e)
     
        
     
        
     
        
     
        
     
        
     
        
     
        
#            df = pd.DataFrame(rows, columns=['uniqueid', 'call_date', 'status', 'security_phrase'])
    
    
    
# class DateBreakdown:
#     def __init__(self, date_column):
#         self.date_column = pd.to_datetime(date_column)  # Convert to datetime format here
#         self.month_mapping = {
#             1: 'Enero',
#             2: 'Febrero',
#             3: 'Marzo',
#             4: 'Abril',
#             5: 'Mayo',
#             6: 'Junio',
#             7: 'Julio',
#             8: 'Agosto',
#             9: 'Septiembre',
#             10: 'Octubre',
#             11: 'Noviembre',
#             12: 'Diciembre'
#         }
        
#     def breakdown(self, dataframe):
#         new_dataframe = dataframe.copy()
#         new_dataframe['fecha'] = self.date_column.dt.strftime('%d/%m/%Y')
#         new_dataframe['hora'] = self.date_column.dt.hour
#         new_dataframe['dia'] = self.date_column.dt.day
#         new_dataframe['semana'] = self.date_column.dt.isocalendar().week
#         new_dataframe['mes'] = self.date_column.dt.month.map(self.month_mapping)
#         new_dataframe['anio'] = self.date_column.dt.year
#         return new_dataframe

#def insert_dataframe(daily_result, daily_result, posgresqlobj.connection):