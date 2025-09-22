import requests
import logging
import time
import csv
import os
import json
from datetime import datetime

class AlphaSimulator:
    def __init__(self, max_concurrent, username, password, alpha_list_file_path, batch_number_for_every_queue):
        self.fail_alphas = 'fail_alphas.csv'
        self.simulated_alphas = f'simulated_alphas_{datetime.now().strftime("%Y%m%d")}.csv'
        self.max_concurrent = max_concurrent
        self.active_simulations = []
        self.username = username
        self.password = password
        self.session = self.sign_in(username, password)
        self.alpha_list_file_path = alpha_list_file_path
        self.sim_queue_ls = []
        self.batch_number_for_every_queue = batch_number_for_every_queue

    def sign_in(self, username, password):
        s = requests.Session()
        s.auth = (username, password)
        count = 0
        count_limit = 5
        while count < count_limit:
            try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                logging.info("Login to BRAIN successfully.")
                return s
            except requests.exceptions.RequestException as e:
                count += 1
                logging.warning(f"Connection down, trying to login again... Error: {e} (Attempt {count}/{count_limit})")
                time.sleep(15)
        
        logging.error(f"{username} failed to log in after {count_limit} attempts. Returning None.")
        return None

    def read_alphas_from_csv_in_batches(self, batch_size=50):
        alphas = []
        if not os.path.exists(self.alpha_list_file_path):
            return alphas
            
        temp_file_name = self.alpha_list_file_path + '.tmp'
        try:
            with open(self.alpha_list_file_path, 'r', newline='') as file, open(temp_file_name, 'w', newline='') as temp_file:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    return []
                
                writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
                writer.writeheader()
                
                for _ in range(batch_size):
                    try:
                        row = next(reader)
                        if 'settings' in row and isinstance(row['settings'], str):
                            try:
                                row['settings'] = json.loads(row['settings'])
                            except json.JSONDecodeError:
                                logging.error(f"Error decoding settings JSON: {row['settings']}")
                                continue
                        alphas.append(row)
                    except StopIteration:
                        break
                
                for remaining_row in reader:
                    writer.writerow(remaining_row)

            os.replace(temp_file_name, self.alpha_list_file_path)

        except Exception as e:
            logging.error(f"An unexpected error occurred in read_alphas_from_csv_in_batches: {e}")
            if os.path.exists(temp_file_name):
                os.remove(temp_file_name)

        return alphas

    def simulate_alpha(self, alpha):
        """Sends a simulation request with robust retry logic for 401 and 429 errors."""
        max_retries = 5
        attempt = 0
        while attempt < max_retries:
            try:
                response = self.session.post('https://api.worldquantbrain.com/simulations', json=alpha)
                response.raise_for_status()
                return response.headers.get("location") # Success
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 401:
                    logging.warning("Session expired (401 Unauthorized). Re-logging in...")
                    self.session = self.sign_in(self.username, self.password)
                    if not self.session:
                        logging.error("Failed to re-login. Aborting this alpha.")
                        break
                    logging.info("Re-login successful. Retrying simulation request...")
                    continue
                elif status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 60))
                    logging.warning(f"Rate limited (429 Too Many Requests). Waiting for {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    logging.error(f"HTTP Error (status {status_code}) during simulation request: {e}")
                    attempt += 1
            except requests.exceptions.RequestException as e:
                logging.error(f"A network error occurred during simulation request: {e}")
                attempt += 1

            if attempt < max_retries:
                time.sleep(10)
        
        self.log_failed_alpha(alpha)
        return None

    def log_failed_alpha(self, alpha):
        logging.error(f"Logging failed alpha: {alpha.get('regular')}")
        try:
            if 'settings' in alpha and isinstance(alpha['settings'], dict):
                alpha['settings'] = json.dumps(alpha['settings'])
            
            file_exists = os.path.isfile(self.fail_alphas)
            
            with open(self.fail_alphas, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=alpha.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(alpha)
        except Exception as e:
            logging.error(f"Could not write to fail_alphas.csv: {e}")

    def load_new_alpha_and_simulate(self):
        if not self.sim_queue_ls:
            self.sim_queue_ls = self.read_alphas_from_csv_in_batches(self.batch_number_for_every_queue)
            if not self.sim_queue_ls:
                return

        if len(self.active_simulations) >= self.max_concurrent:
            return

        alpha = self.sim_queue_ls.pop(0)
        logging.info(f"Starting simulation for alpha: {alpha.get('regular')}")
        location_url = self.simulate_alpha(alpha)
        if location_url:
            self.active_simulations.append(location_url)

    def check_simulation_progress(self, simulation_progress_url):
        try:
            response = self.session.get(simulation_progress_url)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logging.warning(f"Session expired (401 Unauthorized) while checking progress. Re-logging in...")
                self.session = self.sign_in(self.username, self.password)
                if self.session:
                    logging.info("Re-login successful. Retrying progress check...")
                    try:
                        response = self.session.get(simulation_progress_url)
                        response.raise_for_status()
                        return response
                    except requests.exceptions.RequestException as retry_e:
                        logging.error(f"Progress check failed even after re-login: {retry_e}")
                else:
                    logging.error("Failed to re-login. Progress check will be retried later.")
            else:
                logging.error(f"HTTP Error fetching progress from {simulation_progress_url}: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"A network error occurred fetching progress from {simulation_progress_url}: {e}")
        
        return None

    def check_simulation_status(self):
        if not self.active_simulations:
            return

        for sim_url in self.active_simulations[:]:
            response = self.check_simulation_progress(sim_url)
            
            if response is None:
                logging.warning(f"Could not get status for {sim_url}, will retry next cycle.")
                continue

            retry_after = float(response.headers.get("Retry-After", "0"))
            
            # --- MODIFICATION START: Added detailed logging ---
            logging.info(f"Checking {sim_url}... Status: {response.status_code}, Retry-After: {retry_after}")
            # --- MODIFICATION END ---
            
            if retry_after == 0:
                self.active_simulations.remove(sim_url)
                sim_result = response.json()
                alpha_id = sim_result.get("alpha")
                status = sim_result.get("status", "UNKNOWN")
                
                if status == "COMPLETE" and alpha_id:
                    logging.info(f"Simulation {sim_url} completed. Alpha ID: {alpha_id}.")
                    try:
                        alpha_details_response = self.session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                        alpha_details_response.raise_for_status()
                        result_data = alpha_details_response.json()

                        file_exists = os.path.isfile(self.simulated_alphas)
                        with open(self.simulated_alphas, 'a', newline='') as file:
                            writer = csv.DictWriter(file, fieldnames=result_data.keys())
                            if not file_exists:
                                writer.writeheader()
                            writer.writerow(result_data)
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Failed to fetch details for completed alpha {alpha_id}: {e}")
                else:
                    logging.warning(f"Simulation {sim_url} ended with non-COMPLETE status: {status}. Details: {sim_result}")
        
        # This log will now be more meaningful after the detailed per-URL logs.
        logging.info(f"{len(self.active_simulations)} simulations still in process for account {self.username}.")

    def manage_simulations(self):
        if not self.session:
            logging.error("Initial sign in failed. Exiting...")
            return
        
        logging.info(f"🚀 Starting simulation management... max_concurrent={self.max_concurrent}, batch_size={self.batch_number_for_every_queue}")
        
        while True:
            try:
                self.check_simulation_status()

                while len(self.active_simulations) < self.max_concurrent:
                    if not self.sim_queue_ls:
                        self.sim_queue_ls = self.read_alphas_from_csv_in_batches(self.batch_number_for_every_queue)
                        if not self.sim_queue_ls:
                            break 
                    self.load_new_alpha_and_simulate()
                
                is_file_present = os.path.exists(self.alpha_list_file_path) and os.path.getsize(self.alpha_list_file_path) > 50
                if not self.sim_queue_ls and not self.active_simulations and not is_file_present:
                     logging.info("All alphas have been simulated. Shutting down.")
                     break

                time.sleep(5)
            except KeyboardInterrupt:
                logging.info("⏹️ Simulation process interrupted by user.")
                break
            except Exception as e:
                logging.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
                time.sleep(30)