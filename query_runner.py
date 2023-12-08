import os
import plotly.express as px
import numpy as np
import pandas as pd
from clases_mema import Conexion , TimeZoneConverter
from credentials import get_credentials
from datetime import datetime
import re
import csv


from constants import enrollments_dbs



def run_comparison(fields,start_date,end_date,comparison_start_date,comparison_end_date): 
      
      df_brands = get_brands(fields, comparison_start_date , comparison_end_date )
      data ={}
      for brand in df_brands.keys():
            # export_to_csv(df_to_export = df_brands[brand], name = f"pruebatimerange{brand}")
            data[brand] = enrollments_break_down(df_brands,brand, start_date)

      print(data)
      
      write_to_csv(data,end_date)


def enrollments_break_down(df_brands,brand, start_date):

      df_brands_aux = df_brands.copy()
      del df_brands_aux[brand]

      df_other_brands = df_brands_aux.values()
      df_other_brands = pd.concat(df_other_brands)
      
      df_main = df_brands[brand]
   

      date_time = np.datetime64(start_date)
      

      df_main['created_at'] = pd.to_datetime(df_main['created_at'])

      df_main_time_range = df_main[df_main['created_at']> date_time]
      


      # export_to_csv(df_to_export = df_main_time_range, name = f"pruebatimerange{brand}")

      df_email_coincidences = email_coincidences_df(df_main_time_range,df_other_brands)

      # export_to_csv(df_to_export = df_email_coincidences, name = f"pruebaemails{brand}")

      data_enrollments = get_enrollments_data(df_main_time_range,df_email_coincidences)
      
      data_breakdown = error_break_down(df_main_time_range)

      data_breakdown.update(data_enrollments)

      return data_breakdown
      


            

def email_coincidences_df(df_main,df_other_brands):
      np_main = np.array(df_main[['email', 'order_id', 'created_at', 'completed_at']])
      np_other = np.array(df_other_brands[['email', 'order_id','client', 'created_at', 'completed_at']])

      arr_aux1=list(filter(lambda x: x is not None, np_main[:,0])) 
      arr_aux2= list(filter(lambda x: x is not None, np_other[:,0]))

      out = np.intersect1d(arr_aux1,arr_aux2)

      return df_other_brands[df_other_brands['email'].isin(out)]


      

def get_brands(fields, comparison_start_date , comparison_end_date ):
     
      df_brands = dict()
      for brand in enrollments_dbs:
            df_aux = query_brand(brand,fields,comparison_start_date,comparison_end_date)
            df_aux['client'] = brand[:3]
            df_brands[brand[:3]] = df_aux
      
      return df_brands


def get_enrollments_data(df_main,df_email_coincidences):
      enrollments_with_previous_attempt = get_unsuccesful_enrollments_attempts(df_email_coincidences)
      attempts_of_already_customers = get_succesful_enrollments_attempts(df_email_coincidences)
      enrollments_without_attempts = len(df_main) - len(df_email_coincidences)


      return dict(
           { 
            "enrollments_with_previous_attempt":enrollments_with_previous_attempt,
            "attempts_of_already_customers":attempts_of_already_customers,
            "enrollments_without_attempts":enrollments_without_attempts
            }

      )
    
def write_to_csv(dict_brands,end_date):
      with open(os.path.join(os.getcwd(),"data","data.csv"), 'a', encoding='UTF8') as f:
            # create the csv writer
            writer = csv.writer(f)

            for brand in dict_brands.keys():
                  aux = dict_brands[brand]
                  # write a row to the csv file
                  writer.writerow([brand,aux['total_leads'],aux['sales'],aux['non_eligible'],aux['fixable'],aux['enrollments_with_previous_attempt'], aux['attempts_of_already_customers'], aux['enrollments_without_attempts'],end_date])


def create_funnel(data,x_label,y_label):
      fig = px.funnel(data, x=x_label, y=y_label)
      fig.show()

      

def get_unsuccesful_enrollments_attempts(df_brand):
      return len(df_brand[pd.isnull(df_brand['completed_at'])])

def get_succesful_enrollments_attempts(df_brand):
      return len(df_brand[pd.notnull(df_brand['completed_at'])])



def export_to_csv(df_to_export, name, file_path = None):
      if not file_path:
            file_path = os.path.join(os.getcwd(),"testing",f"{name}__{datetime.now()}.csv")
      df_to_export.to_csv(file_path, index=False)
            
def error_break_down(df_data):
      non_eligible = len(df_data[df_data['retry_process_response'].str.contains(r'UNAVAILABLE_DEVICE:|This person is not eligible to receive a device', regex=True,na=False)])
      sales = len(df_data[df_data['completed_at'].notnull()])
      fixable = len(df_data) - sales - non_eligible

      data = {
            "total_leads":len(df_data),
            "non_eligible":non_eligible,
            "sales": sales,
            "fixable":fixable
      }

      return data

      
      


def query_brand(brand , fields, start_date , end_date ):
      print(f"Querying {brand}...")
      query = f"SELECT {fields} FROM {brand} WHERE created_at BETWEEN '{start_date}' AND '{end_date}';"

      sh2obj = Conexion(get_credentials())
      sh2obj.Conexion_mysql() 
      data = sh2obj.queryToDb(query)
      sh2obj.close_ssh()
      sh2obj.close_conn()

      return data






