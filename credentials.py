import os

def get_credentials():
    return  [
      '54.183.34.45', # IP del EC2
      'ribarra', # User del Ec2 (egarcia o vgarcia)
      f'{os.getcwd()}/sos.pem', 
      'sos-prod-instance-1-read1.clj3rhohf9hn.us-west-1.rds.amazonaws.com', # URL de la DB
      3306,
      'ribarra', #egarcia o vgarcia (user de la DB)
      '2a7phQn3ZFcfhvcZhBp6GX89gBWXhgaJ', # password
      'sh2_online_sales' # sh2_online_sales
      ]