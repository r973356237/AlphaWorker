import requests
import json
import pandas as pd
import csv
import logging
import time
from os.path import expanduser
from requests.auth import HTTPBasicAuth


class AlphaCreator:
    def __init__(self, username=None, password=None, credentials_file='brain.txt'):
        """
        Initializes the AlphaCreator
        
        Args:
            username: Username (optional, if provided, will not read from file)
            password: Password (optional,if provided, will not read from file)
            credentials_file: Path to the credentials file, defaults to 'brain.txt'
        """
        self.credentials_file = credentials_file
        self.username = username
        self.password = password
        self.session = None
        self.alpha_list = []
        
        # Configure logging
        logging.basicConfig(
            filename='alpha_creator.log', 
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )
        
    def sign_in(self, retries=3, delay=5):
        """
        Logs in to the WorldQuant Brain API, with a retry mechanism.
        
        Args:
            retries: Max number of retries.
            delay: Delay in seconds between retries.
            
        Returns:
            requests.Session: The authenticated session object.
        """
        for i in range(retries):
            try:
                # Load credentials from file if username/password are not provided
                if not self.username or not self.password:
                    with open(expanduser(self.credentials_file)) as f:
                        credentials = json.load(f)
                    self.username, self.password = credentials
                
                # Create a session object
                sess = requests.Session()
                sess.auth = HTTPBasicAuth(self.username, self.password)
                
                # Send authentication request
                response = sess.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                
                logging.info("Successfully logged into WorldQuant Brain API")
                print(f"Login status code: {response.status_code}")
                print(response.json())
                
                self.session = sess
                return sess
                
            except Exception as e:
                logging.error(f"Login failed: {str(e)}")
                print(f"Login failed: {str(e)}")
                if i < retries - 1:
                    print(f"Retrying... ({i + 1}/{retries}), waiting for {delay} seconds...")
                    time.sleep(delay)
        
        logging.error(f"Failed to log in after {retries} attempts.")
        print(f"Failed to log in after {retries} attempts.")
        return None
    
    def get_datafields(self, search_scope, dataset_id='', search=''):
        """
        Retrieves data fields.
        
        Args:
            search_scope: A dictionary containing instrumentType, region, delay, and universe.
            dataset_id: Dataset ID.
            search: Search keyword.
            
        Returns:
            pandas.DataFrame: A DataFrame of data fields.
        """
        if not self.session:
            logging.error("Please sign in first.")
            return None
            
        instrument_type = search_scope['instrumentType']
        region = search_scope['region']
        delay = search_scope['delay']
        universe = search_scope['universe']
        
        if len(search) == 0:
            url_template = ("https://api.worldquantbrain.com/data-fields?"
                          f"&instrumentType={instrument_type}"
                          f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50"
                          "&offset={x}")
            count_response = self.session.get(url_template.format(x=0))
            count_response.raise_for_status()
            count = count_response.json()['count']
        else:
            url_template = ("https://api.worldquantbrain.com/data-fields?"
                          f"&instrumentType={instrument_type}"
                          f"&region={region}&delay={str(delay)}&universe={universe}&limit=50"
                          f"&search={search}"
                          "&offset={x}")
            count_response = self.session.get(url_template.format(x=0))
            count_response.raise_for_status()
            count = count_response.json()['count']
        
        datafields_list = []
        for x in range(0, count, 50):
            datafields_response = self.session.get(url_template.format(x=x))
            datafields_response.raise_for_status()
            datafields_list.append(datafields_response.json()['results'])
        
        datafields_list_flat = [item for sublist in datafields_list for item in sublist]
        datafields_df = pd.DataFrame(datafields_list_flat)
        
        logging.info(f"Retrieved {len(datafields_df)} data fields for search '{search}' in dataset '{dataset_id}'.")
        return datafields_df
    
    def generate_alpha_expressions(self, fundamental2_datafields):
        """
        Generates a list of (expression, group) tuples based on the new template.
        
        Args:
            fundamental2_datafields (list): A list of data field names from the fundamental2 dataset.
            
        Returns:
            list: A list of (expression, group) tuples.
        """
        expressions_with_settings = []
        
        # Parameter lists to fill the template
        datafield_list = ['fnd6_newa1v1300_gdwl', 'fnd6_newqv1300_gdwlq', 'fnd6_acqgdwl', 'goodwill']
        group_list = ['SUBINDUSTRY', 'INDUSTRY', 'SECTOR', 'MARKET']
        
        template = "-ts_backfill(zscore({datafield}/sales), 65) + (rank({fundamental2})*rank(capex)*rank(dividend/sharesout)+rank(debt_st))"

        # Nested loops to iterate through all combinations
        for group in group_list:
            for dfield in datafield_list:
                for f2field in fundamental2_datafields:
                    expression = template.format(datafield=dfield, fundamental2=f2field)
                    expressions_with_settings.append((expression, group))

        logging.info(f"Generated {len(expressions_with_settings)} Alpha expressions with corresponding settings.")
        print(f"There are a total of {len(expressions_with_settings)} alpha expressions.")
        
        return expressions_with_settings
    
    def create_alpha_list(self, expressions_with_settings):
        """
        Creates a list of Alpha objects, applying the specific neutralization group for each.
        
        Args:
            expressions_with_settings: A list of (expression, group) tuples.
            
        Returns:
            list: A list of complete Alpha objects.
        """
        alpha_list = []
        
        for index, (expression, group) in enumerate(expressions_with_settings, start=1):
            if index % 1000 == 0:  # Print progress every 1000 alphas
                print(f"Processing the {index}-th Alpha object.")
                
            simulation_data = {
                "type": "REGULAR",
                "settings": {
                    "instrumentType": "EQUITY",
                    "region": "USA",
                    "universe": "TOP3000",
                    "delay": 1,
                    "decay": 0,
                    "neutralization": group,  # Dynamically set the neutralization group here
                    "truncation": 0.01,
                    "pasteurization": "ON",
                    "unitHandling": "VERIFY",
                    "nanHandling": "OFF",
                    "language": "FASTEXPR",
                    "visualization": False,
                },
                "regular": expression
            }
            alpha_list.append(simulation_data)
        
        self.alpha_list = alpha_list
        logging.info(f"Created {len(alpha_list)} Alpha objects.")
        print(f"Created {len(alpha_list)} Alpha objects.")
        
        return alpha_list
    
    def save_alphas_to_csv(self, filename='alpha_list_pending_simulated.csv'):
        """
        Saves the list of Alphas to a CSV file.
        
        Args:
            filename: The CSV filename, defaults to 'alpha_list_pending_simulated.csv'.
        """
        if not self.alpha_list:
            logging.error("Alpha list is empty, please generate Alphas first.")
            print("Alpha list is empty, please generate Alphas first.")
            return False
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['type', 'settings', 'regular']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data, converting settings dict to string for CSV
                for alpha in self.alpha_list:
                    # Make a copy to avoid modifying the original list
                    writable_alpha = alpha.copy()
                    writable_alpha['settings'] = json.dumps(writable_alpha['settings'])
                    writer.writerow(writable_alpha)
            
            logging.info(f"Successfully saved {len(self.alpha_list)} Alphas to file {filename}.")
            print(f"Successfully saved {len(self.alpha_list)} Alphas to file {filename}.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to save CSV file: {str(e)}")
            print(f"Failed to save CSV file: {str(e)}")
            return False
    
    def create_and_save_alphas(self, filename='alpha_list_pending_simulated.csv'):
        """
        A complete workflow for creating and saving Alphas.
        
        Args:
            filename: The filename for the saved CSV.
            
        Returns:
            bool: Whether the process was successful.
        """
        # Define simulation settings
        search_scope = {
            'region': 'USA', 
            'delay': '1', 
            'universe': 'TOP3000', 
            'instrumentType': 'EQUITY'
        }

        try:
            # 1. Sign in
            if not self.sign_in():
                return False

            # 2. Get data fields from the 'fundamental2' dataset
            print("Getting data fields from 'fundamental2' dataset...")
            fundamental2_df = self.get_datafields(search_scope, dataset_id='fundamental2')

            if fundamental2_df is None or fundamental2_df.empty:
                logging.error("Failed to retrieve data fields from 'fundamental2'. Aborting.")
                print("Could not find any data fields in dataset 'fundamental2'.")
                return False

            # Filter for MATRIX type data fields
            matrix_fields = fundamental2_df[fundamental2_df['type'] == "MATRIX"]
            if matrix_fields.empty:
                logging.error("No MATRIX type data fields found in 'fundamental2'.")
                print("No MATRIX type data fields found. These are required for the alpha expressions.")
                return False

            fundamental2_data_list = matrix_fields['id'].tolist()
            print(f"Found {len(fundamental2_data_list)} MATRIX-type data fields from the dataset.")

            # 3. Generate Alpha expressions
            print("Generating Alpha expressions and settings...")
            expressions_with_settings = self.generate_alpha_expressions(fundamental2_data_list)
            
            # 4. Create the list of Alpha objects
            print("Creating Alpha objects with dynamic settings...")
            self.create_alpha_list(expressions_with_settings)
            
            # 5. Save to a CSV file
            print("Saving to CSV file...")
            return self.save_alphas_to_csv(filename)
            
        except Exception as e:
            logging.error(f"An error occurred during the Alpha creation process: {str(e)}")
            print(f"An error occurred during the Alpha creation process: {str(e)}")
            return False


# Example usage
if __name__ == "__main__":
    # Create an instance of AlphaCreator
    creator = AlphaCreator()
    
    # Execute the complete Alpha creation and saving workflow
    success = creator.create_and_save_alphas()
    
    if success:
        print("Alpha creation and saving completed!")
        print(f"Number of Alphas generated: {len(creator.alpha_list)}")
        print(f"File has been saved as: alpha_list_pending_simulated.csv")
    else:
        print("Alpha creation failed, please check the log file alpha_creator.log")