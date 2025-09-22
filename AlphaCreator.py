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
        self.credentials_file = credentials_file
        self.username = username
        self.password = password
        self.session = None
        self.alpha_list = []
        
        logging.basicConfig(
            filename='alpha_creator.log', 
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )
        
    def sign_in(self, retries=3, delay=5):
        # ... (此部分与您之前的代码完全相同，保持不变) ...
        for i in range(retries):
            try:
                if not self.username or not self.password:
                    with open(expanduser(self.credentials_file)) as f:
                        credentials = json.load(f)
                    self.username, self.password = credentials
                
                sess = requests.Session()
                sess.auth = HTTPBasicAuth(self.username, self.password)
                
                response = sess.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                
                logging.info("Successfully logged into WorldQuant Brain API")
                print(f"Login status code: {response.status_code}")
                print(response.json())
                
                self.session = sess
                return sess
                
            except Exception as e:
                logging.error(f"Login failed: {str(e)}")
                if i < retries - 1:
                    time.sleep(delay)
        
        logging.error(f"Failed to log in after {retries} attempts.")
        return None
    
    def get_datafields(self, search_scope, dataset_id=''):
        # ... (此部分与您之前的代码完全相同，保持不变) ...
        if not self.session:
            logging.error("Please sign in first.")
            return None
            
        instrument_type = search_scope['instrumentType']
        region = search_scope['region']
        delay = search_scope['delay']
        universe = search_scope['universe']
        
        url_template = ("https://api.worldquantbrain.com/data-fields?"
                      f"&instrumentType={instrument_type}"
                      f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50"
                      "&offset={x}")
        try:
            count_response = self.session.get(url_template.format(x=0))
            count_response.raise_for_status()
            count = count_response.json().get('count', 0)
            if count == 0:
                return pd.DataFrame()

            datafields_list = []
            for x in range(0, count, 50):
                datafields_response = self.session.get(url_template.format(x=x))
                datafields_response.raise_for_status()
                datafields_list.append(datafields_response.json()['results'])
            
            datafields_list_flat = [item for sublist in datafields_list for item in sublist]
            datafields_df = pd.DataFrame(datafields_list_flat)
            
            logging.info(f"Retrieved {len(datafields_df)} data fields for dataset '{dataset_id}'.")
            return datafields_df
        except Exception as e:
            logging.error(f"Failed to fetch datafields for dataset '{dataset_id}': {e}")
            return None
    
    def generate_alpha_expressions(self, fundamental_factors):
        """
        Generates Alpha expressions based on the advanced multi-factor template.
        This version is now simplified to only handle MATRIX-type factors.
        """
        alpha_expressions = []
        
        print(f"Generating expressions for {len(fundamental_factors)} MATRIX-type factors...")
        
        for factor in fundamental_factors:
            # The core logic for the alpha signal
            alpha_signal = (
                "rank("
                "group_rank(ts_decay_linear(volume/ts_sum(volume,252),10),market)*"
                # MODIFICATION: vec_avg is removed as we are only using MATRIX types
                f"group_rank(ts_rank({factor}, 252),market)*" 
                "group_rank(-ts_delta(close,5),market)"
                ")"
            )
            
            neutralization_group = "bucket(rank(cap),range='0,1,0.1')"
            
            # The final expression with the trade_when wrapper
            final_expression = (
                f"trade_when(volume>adv20,"
                f"group_neutralize({alpha_signal},{neutralization_group}),"
                f"-1)"
            )
            
            alpha_expressions.append(final_expression)

        logging.info(f"Generated {len(alpha_expressions)} Alpha expressions.")
        print(f"There are {len(alpha_expressions)} Alphas to simulate")
        
        return alpha_expressions
    
    def create_alpha_list(self, alpha_expressions):
        # ... (此部分与您之前的代码完全相同，保持不变) ...
        alpha_list = []
        for index, expression in enumerate(alpha_expressions, start=1):
            if index > 0 and index % 5000 == 0:
                print(f"Processing the {index}-th Alpha object.")
                
            simulation_data = {
                "type": "REGULAR",
                "settings": {
                    "instrumentType": "EQUITY", "region": "USA", "universe": "TOP3000",
                    "delay": 1, "decay": 0, "neutralization": "SUBINDUSTRY",
                    "truncation": 0.01, "pasteurization": "ON", "unitHandling": "VERIFY",
                    "nanHandling": "OFF", "language": "FASTEXPR", "visualization": False,
                },
                "regular": expression
            }
            alpha_list.append(simulation_data)
        
        self.alpha_list = alpha_list
        logging.info(f"Created {len(alpha_list)} Alpha objects.")
        print(f"Created {len(alpha_list)} Alpha objects.")
        return alpha_list
    
    def save_alphas_to_csv(self, filename='alpha_list_pending_simulated.csv'):
        # ... (此部分与您之前的代码完全相同，保持不变) ...
        if not self.alpha_list:
            logging.error("Alpha list is empty.")
            return False
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['type', 'settings', 'regular']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for alpha in self.alpha_list:
                    writable_alpha = alpha.copy()
                    writable_alpha['settings'] = json.dumps(writable_alpha['settings'])
                    writer.writerow(writable_alpha)
            
            logging.info(f"Successfully saved {len(self.alpha_list)} Alphas to file {filename}.")
            return True
            
        except Exception as e:
            logging.error(f"Failed to save CSV file: {str(e)}")
            return False

    def create_and_save_alphas(self, filename='alphas_for_matrix_factors.csv'):
        """
        A complete workflow that now ONLY tests for MATRIX-type factors.
        MODIFICATION: Method signature now accepts a filename to fix the TypeError.
        """
        search_scope = {
            'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'
        }

        try:
            if not self.sign_in():
                return False

            print("Getting data fields from 'fundamental6' and 'fundamental2' datasets...")
            fundamental6_df = self.get_datafields(search_scope, dataset_id='fundamental6')
            fundamental2_df = self.get_datafields(search_scope, dataset_id='fundamental2')
            
            all_factors_df = pd.concat([fundamental6_df, fundamental2_df], ignore_index=True).drop_duplicates(subset=['id'])

            if all_factors_df.empty:
                logging.error("No data fields found. Aborting.")
                return False

            # --- MODIFICATION: Only processing MATRIX type fields ---
            matrix_fields = all_factors_df[all_factors_df['type'] == "MATRIX"]
            if matrix_fields.empty:
                logging.error("No MATRIX type data fields found. Aborting.")
                return False
                
            matrix_factor_list = matrix_fields['id'].tolist()
            print(f"Found a total of {len(matrix_factor_list)} unique MATRIX-type data fields to test.")

            print("Generating Alpha expressions for MATRIX factors...")
            alpha_expressions = self.generate_alpha_expressions(matrix_factor_list)
            
            print("Creating Alpha objects...")
            self.create_alpha_list(alpha_expressions)
            
            print(f"Saving to CSV file: {filename}")
            if self.save_alphas_to_csv(filename):
                 print(f"Successfully saved {len(self.alpha_list)} Almas to file {filename}.")
                 return True
            else:
                 print(f"Failed to save alphas to {filename}.")
                 return False
            
        except Exception as e:
            logging.error(f"An error occurred during the Alpha creation process: {e}", exc_info=True)
            return False


if __name__ == "__main__":
    creator = AlphaCreator()
    # MODIFICATION: Calling the method correctly with the desired filename
    success = creator.create_and_save_alphas(filename='alpha_list_pending_simulated.csv')
    
    if success:
        print("\n✅ Alpha creation and saving completed!")
    else:
        print("\n❌ Alpha creation failed. Please check the log file alpha_creator.log for details.")