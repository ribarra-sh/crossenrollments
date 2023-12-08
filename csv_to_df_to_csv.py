import pandas as pd

# Define the path to your CSV file in the "Downloads" folder
#AMB
csv_file_path = "/Users/victoremanuelgarcia/Downloads/ipc_enrollments2wk.csv"


# Define the path to your CSV file in the "Downloads" folder
#EXCESS
csv_file_path1 = "/Users/victoremanuelgarcia/Downloads/whoop_enrollments_jun_to_nov06_2023.csv"


# Define the path to your CSV file in the "Downloads" folder
#WHOOP
csv_file_path2 = "/Users/victoremanuelgarcia/Downloads/whoop_enrollments_jun_to_nov06_2023.csv"





# Specify the sheet name within the CSV file
#sheet_name = "amb_enrollment"

# Read the CSV into a DataFrame
df_amb = pd.read_csv(csv_file_path)
df_excs = pd.read_csv(csv_file_path1)
df_wp = pd.read_csv(csv_file_path2)





df_amb['Client'] = 'AMB'
df_excs['Client'] = 'ECXS'
df_wp['Client'] = 'WP'



# Merge DataFrames B and C
df_ew= pd.concat([df_excs, df_wp])

# Create a new DataFrame for the output
output_df = pd.DataFrame(columns=['email', 'order_id', 'Client', 'created_at', 'completed_at'])

# Iterate through each email in DataFrame A (AMB)
for email in df_amb['email']:
    # Check if the email exists in DataFrame B (EXCESS) or C (WHOOP)
    customer_info = df_ew[df_ew['email'] == email]
    if not customer_info.empty:
        # Extract relevant information and append it to the output DataFrame
        output_df = pd.concat([output_df, customer_info[['email', 'order_id', 'Client', 'created_at', 'completed_at']]])
        
        

# # Define the desired file name for the filtered CSV file
output_file_name = 'ipc_cross_enrollment_061123.csv'  # Change this to your desired file name

# # Define the full path to the output file in the same location
output_file_path = f"/Users/victoremanuelgarcia/Downloads/{output_file_name}"

# # Export the filtered DataFrame to the new CSV file
output_df.to_csv(output_file_path, index=False)


# # # # Define the desired file name for the filtered CSV file
# output_file_name2 = 'amb_daily_oct25.csv'  # Change this to your desired file name

# # # # Define the full path to the output file in the same location
# output_file_path2 = f"/Users/victoremanuelgarcia/Downloads/{output_file_name2}"

# # # # Export the filtered DataFrame to the new CSV file
# df_amb.to_csv(output_file_path2, index=False)
