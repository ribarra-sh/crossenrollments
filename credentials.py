import os

def get_credentials():
    return  [
      '', # IP del EC2
      '=', # User del Ec2 (egarcia o vgarcia)
      f'{os.getcwd()}/sos.pem', 
      'sos-prod-instance-1-read1.clj3rhohf9hn.us-west-1.rds.amazonaws.com', # URL de la DB
      3306,
      '', #egarcia o vgarcia (user de la DB)
      '', # password
      'sh2_online_sales' # sh2_online_sales
      ]
