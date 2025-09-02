import requests
import logging
import time
import csv
import os
import ast
from datetime import datetime # Presumed import for __init__

# Note: The following variables used in __init__ are not defined in the provided snippets.
# They are likely defined elsewhere in the user's full script.
# loc_dt = datetime.now()
# fmt = '%Y%m%d'

class AlphaSimulator:
    def __init__(self, max_concurrent, username, password, alpha_list_file_path, batch_number_for_every_queue):
        self.fail_alphas = 'fail_alphas.csv'
        # Assuming 'loc_dt' and 'fmt' are defined elsewhere, e.g., globally
        # self.simulated_alphas = f'simulated_alphas_{loc_dt.strftime(fmt)}.csv'
        self.simulated_alphas = f'simulated_alphas_{datetime.now().strftime("%Y%m%d")}.csv' # Placeholder implementation
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
        count_limit = 30
        while True:
            try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                break
            except:
                count += 1
                logging.warning("Connection down, trying to login again...")
                time.sleep(15)
                if count > count_limit:
                    logging.error(f"{username} failed too many times, returning None.")
                    return None
        logging.info("Login to BRAIN successfully.")
        return s

    def read_alphas_from_csv_in_batches(self, batch_size=50):
        """
        1. 打开alpha_list_pending_simulated
        2. 取出batch_size个alpha, 放入列表变量alphas
        3. 取出后要写(overwrite)回alpha_list_pending_simulated
        4. 把取出的alphas, 写到sim_queue.csv文件中, 方便监控在排队的alpha有多少
        5. 返回列表变量alphas
        """
        alphas = []
        temp_file_name = self.alpha_list_file_path + '.tmp'
        try:
            with open(self.alpha_list_file_path, 'r') as file, open(temp_file_name, 'w', newline='') as temp_file:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames
                writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
                writer.writeheader()
                for _ in range(batch_size):
                    try:
                        row = next(reader)
                        if 'settings' in row:
                            if isinstance(row['settings'], str):
                                try:
                                    row['settings'] = ast.literal_eval(row['settings'])
                                except (ValueError, SyntaxError):
                                    print(f"Error evaluating settings: {row['settings']}")
                            elif isinstance(row['settings'], dict):
                                pass
                            else:
                                print(f"Unexpected type for settings: {type(row['settings'])}")
                        alphas.append(row)
                    except StopIteration:
                        break
                for remaining_row in reader:
                    writer.writerow(remaining_row)
            os.replace(temp_file_name, self.alpha_list_file_path)
            if alphas:
                # This part seems to be for monitoring and is not directly used by other methods
                with open('sim_queue.csv', 'w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=alphas[0].keys())
                    if file.tell() == 0:
                        writer.writeheader()
                    writer.writerows(alphas)
        except FileNotFoundError:
            logging.error(f"File not found: {self.alpha_list_file_path}")
        return alphas

    def simulate_alpha(self, alpha):
        count = 0
        while True:
            try:
                response = self.session.post('https://api.worldquantbrain.com/simulations', json=alpha)
                response.raise_for_status()
                if "location" in response.headers:
                    logging.info("Alpha location retrieved successfully.")
                    logging.info(f"Location: {response.headers['Location']}")
                    return response.headers['Location']
            except requests.exceptions.RequestException as e:
                logging.error(f"Error in sending simulation request: {e}")
                if count > 35:
                    self.session = self.sign_in(self.username, self.password)
                    logging.error("Error occurred too many times, skipping this alpha and re-logging in.")
                    break
                logging.error("Error in sending simulation request. Retrying after 5s...")
                time.sleep(5)
                count += 1
        logging.error(f"Simulation request failed after {count} attempts.")
        with open(self.fail_alphas, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=alpha.keys())
            writer.writerow(alpha)
        return None

    def load_new_alpha_and_simulate(self):
        if len(self.sim_queue_ls) < 1:
            self.sim_queue_ls = self.read_alphas_from_csv_in_batches(self.batch_number_for_every_queue)

        if len(self.active_simulations) >= self.max_concurrent:
            logging.info(f"Max concurrent simulations reached ({self.max_concurrent}). Waiting 2 seconds")
            time.sleep(2)
            return

        logging.info('loading new alpha...')
        try:
            alpha = self.sim_queue_ls.pop(0)
            logging.info(f"Starting simulation for alpha: {alpha.get('regular')} with settings: {alpha.get('settings')}")
            location_url = self.simulate_alpha(alpha)
            if location_url:
                self.active_simulations.append(location_url)
        except IndexError:
            logging.info("No more alphas available in the queue.")

    def check_simulation_progress(self, simulation_progress_url):
        try:
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()
            # 修复：处理Retry-After可能是浮点数字符串的情况
            retry_after = simulation_progress.headers.get("Retry-After", "0")
            try:
                retry_after_value = float(retry_after)
            except (ValueError, TypeError):
                retry_after_value = 0.0
            
            if retry_after_value == 0:
                alpha_id = simulation_progress.json().get("alpha")
                if alpha_id:
                    alpha_response = self.session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                    alpha_response.raise_for_status()
                    return alpha_response.json()
                else:
                    return simulation_progress.json()
            else:
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching simulation progress: {e}")
            self.session = self.sign_in(self.username, self.password)
            return None

    def check_simulation_status(self):
        count = 0
        if len(self.active_simulations) == 0:
            logging.info("No one is in active simulation now")
            return None # Changed from 'return' to 'return None' for consistency
        
        # Use a copy of the list to iterate over, as we might modify the original list
        for sim_url in self.active_simulations[:]:
            sim_progress = self.check_simulation_progress(sim_url)
            if sim_progress is None:
                count += 1
                continue
            
            # Assuming a completed simulation will have a 'status' field.
            if sim_progress.get("status"):
                alpha_id = sim_progress.get("id")
                status = sim_progress.get("status")
                logging.info(f"Alpha id: {alpha_id} ended with status: {status}. Removing from active list.")
                self.active_simulations.remove(sim_url)
                with open(self.simulated_alphas, 'a', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=sim_progress.keys())
                    # Check if file is empty to write header
                    file.seek(0, 2) # move to end of file
                    if file.tell() == 0:
                        writer.writeheader()
                    writer.writerow(sim_progress)
            else:
                count += 1

        logging.info(f"Total {count} simulations are in process for account {self.username}.")

    def manage_simulations(self):
        if not self.session:
            logging.error("Failed to sign in. Exiting...")
            return
        while True:
            self.check_simulation_status()
            self.load_new_alpha_and_simulate()
            time.sleep(3)

# Example usage
# Ensure logging is configured, e.g., logging.basicConfig(level=logging.INFO)
# username = "YOUR_USERNAME"
# password = "YOUR_PASSWORD"
alpha_list_file_path = 'alpha_list_pending_simulated.csv' # replace with your actual file path
# simulator = AlphaSimulator(max_concurrent=3, username=username, password=password, alpha_list_file_path=alpha_list_file_path, batch_number_for_every_queue=20)
# simulator.manage_simulations()