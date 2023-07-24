import pandas as pd

chunksize = 10 ** 6  # Define the size of chunks
fraud_records = pd.DataFrame()
legit_records = pd.DataFrame()

# Read the CSV file in chunks
for chunk in pd.read_csv('credit_card_transactions-ibm_v2.csv', chunksize=chunksize):
    # Include the condition that 'Zip' is not null
    fraud = chunk[(chunk['Is Fraud?'] == 'Yes') & (chunk['Zip'].notna()) & (chunk['Errors?'].notna())]
    legit = chunk[(chunk['Is Fraud?'] == 'No') & (chunk['Zip'].notna()) & (chunk['Errors?'].notna())]
    
    # Concatenate the fraud and legit records from each chunk
    fraud_records = pd.concat([fraud_records, fraud])
    legit_records = pd.concat([legit_records, legit])
    
    # If we have enough records, break the loop
    if fraud_records.shape[0] >= 3809 and legit_records.shape[0] >= 76180:
        break

# Truncate the dataframes to the desired size
fraud_records = fraud_records.iloc[:3809]
legit_records = legit_records.iloc[:76180]

# Concatenate the fraud and legit records to get the final dataset
final_dataset = pd.concat([fraud_records, legit_records])

# Save the final dataset as a CSV file
final_dataset.to_csv('final_dataset.csv', index=False)
