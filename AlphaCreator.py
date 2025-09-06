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
    
    def generate_alpha_expressions(self, socialmedia_data):
        """
        Generates Alpha expressions based on the new template and parameter lists.
        
        Args:
            socialmedia_data (list): A list of social media data field names.
            
        Returns:
            list: A list of Alpha expressions.
        """
        alpha_expressions = []
        
        # Parameter lists from the image
        group_compare_op = ['group_rank', 'group_zscore', 'group_neutralize']
        ts_compare_op = ['ts_rank', 'ts_zscore', 'ts_av_diff']
        days = [120, 180]
        group = ['market', 'industry', 'subindustry', 'sector', 'bucket(rank(cap),range="0,1,0.1")']

        # Nested loops to iterate through all combinations
        for sm in socialmedia_data:
            for d in days:
                for gco in group_compare_op:
                    for tco in ts_compare_op:
                        for grp in group:
                            # Build the expression step-by-step as shown in the template logic
                            part1 = f"vhat=ts_regression(volume,ts_delay({sm},1),{d});"
                            part2 = f"ehat=ts_regression(returns,vhat,{d});"
                            # CORRECTED THIS LINE: Removed parentheses around {gco}
                            part3 = f"alpha={gco}(-ehat*{tco}(volume,5),{grp});"
                            part4 = "trade_when(abs(returns)<0.075,alpha,abs(returns)>0.1)"
                            
                            expression = part1 + part2 + part3 + part4
                            alpha_expressions.append(expression)

        logging.info(f"Generated {len(alpha_expressions)} Alpha expressions.")
        print(f"there are total {len(alpha_expressions)} alpha expressions")
        
        return alpha_expressions
    
    def create_alpha_list(self, alpha_expressions):
        """
        Creates a list of Alpha objects by wrapping the expressions.
        
        Args:
            alpha_expressions: A list of Alpha expressions.
            
        Returns:
            list: A list of complete Alpha objects.
        """
        alpha_list = []
        
        for index, alpha_expression in enumerate(alpha_expressions, start=1):
            if index % 1000 == 0:  # Print progress every 1000 alphas
                print(f"Processing the {index}-th Alpha expression.")
                
            simulation_data = {
                "type": "REGULAR",
                "settings": {
                    "instrumentType": "EQUITY",
                    "region": "USA",
                    "universe": "TOP3000",
                    "delay": 1,
                    "decay": 0,
                    "neutralization": "SUBINDUSTRY",
                    "truncation": 0.01,
                    "pasteurization": "ON",
                    "unitHandling": "VERIFY",
                    "nanHandling": "OFF",
                    "language": "FASTEXPR",
                    "visualization": False,
                },
                "regular": alpha_expression
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
                
                # Write data
                for alpha in self.alpha_list:
                    writer.writerow(alpha)
            
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
        try:
            # 1. Sign in
            if not self.sign_in():
                return False

            # 2. Define the list of social media data fields as per the image
            socialmedia_data = ['scl12_buzz', 'scl12_sentiment', 'snt_buzz', 'snt_buzz_bfl', 'snt_buzz_ret', 'snt_value']
            print(f"Using a fixed list of {len(socialmedia_data)} social media data fields.")

            # 3. Generate Alpha expressions
            print("Generating Alpha expressions...")
            alpha_expressions = self.generate_alpha_expressions(socialmedia_data)
            
            # 4. Create the list of Alpha objects
            print("Creating Alpha objects...")
            self.create_alpha_list(alpha_expressions)
            
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