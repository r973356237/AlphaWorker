import requests
import logging
import time
import csv
import re
from datetime import datetime
from pathlib import Path

# 日志配置 - 新增encoding='utf-8'解决中文乱码
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('correlation_check.log', encoding='utf-8'),  # 指定UTF-8编码
        logging.StreamHandler()
    ]
)

class AutoCorrelationChecker:
    def __init__(self, alpha_list_md_path, login_creds_path, max_retry=35, retry_interval=5):
        self.alpha_list_md_path = alpha_list_md_path
        self.login_creds_path = login_creds_path
        self.max_retry = max_retry
        self.retry_interval = retry_interval
        self.session = None
        self.result_path = f'self_correlation_results_{datetime.now().strftime("%Y%m%d")}.csv'

    def load_login_creds(self):
        """从文件加载登录信息（邮箱+密码）"""
        try:
            with open(self.login_creds_path, 'r', encoding='utf-8') as f:  # 读取时指定编码
                creds = eval(f.read())
                return creds[0], creds[1]
        except Exception as e:
            logging.error(f"加载登录信息失败: {e}")
            raise

    def sign_in(self):
        """登录WorldQuant平台，建立会话"""
        username, password = self.load_login_creds()
        session = requests.Session()
        session.auth = (username, password)
        count = 0
        count_limit = 30
        while count < count_limit:
            try:
                response = session.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                logging.info("登录成功")
                return session
            except Exception as e:
                count += 1
                logging.warning(f"登录失败（第{count}次重试）: {e}")
                time.sleep(15)
        logging.error("登录失败次数过多，终止程序")
        return None

    def extract_alpha_ids(self):
        """从MD文件提取Alpha ID（跳过表头和分隔线）"""
        ids = []
        try:
            with open(self.alpha_list_md_path, 'r', encoding='utf-8') as f:  # 读取时指定编码
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                if not line or 'grade' in line or '---' in line:
                    continue
                id_pattern = re.compile(r'^\s*(\w+)\s+')
                match = id_pattern.match(line)
                if match:
                    alpha_id = match.group(1)
                    if alpha_id.lower() != 'id':
                        ids.append(alpha_id)
        
            logging.info(f"成功提取{len(ids)}个有效Alpha ID")
            return ids
        except FileNotFoundError:
            logging.error(f"未找到文件: {self.alpha_list_md_path}")
            return []
        except Exception as e:
            logging.error(f"提取ID失败: {e}")
            return []

    def check_single_alpha(self, alpha_id):
        """检查单个Alpha的自相关性"""
        url = f'https://api.worldquantbrain.com/alphas/{alpha_id}/check'
        count = 0
        while count < self.max_retry:
            try:
                response = self.session.get(url)
                response.raise_for_status()
                result = response.json()
                checks = result.get('is', {}).get('checks', [])
                corr_check = next((c for c in checks if c['name'] == 'SELF_CORRELATION'), None)
                if corr_check:
                    return {
                        'alpha_id': alpha_id,
                        'result': corr_check.get('result'),
                        'correlation_value': corr_check.get('value'),
                        'limit': corr_check.get('limit'),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                else:
                    logging.warning(f"未找到自相关性检查结果: {alpha_id}")
                    return {'alpha_id': alpha_id, 'result': 'UNKNOWN', 'error': '未找到SELF_CORRELATION字段'}
            except requests.exceptions.RequestException as e:
                count += 1
                logging.warning(f"检查{alpha_id}失败（第{count}次重试）: {e}")
                time.sleep(self.retry_interval)
                if hasattr(response, 'status_code') and response.status_code == 401 and count % 5 == 0:
                    self.session = self.sign_in()
                    if not self.session:
                        break
        error_msg = f"超过最大重试次数（{self.max_retry}次）"
        logging.error(f"{alpha_id}检查失败: {error_msg}")
        return {'alpha_id': alpha_id, 'result': 'FAIL', 'error': error_msg}

    def save_result(self, result):
        """保存检查结果到CSV"""
        file_exists = Path(self.result_path).exists()
        with open(self.result_path, 'a', newline='', encoding='utf-8') as f:  # 保持UTF-8编码
            fieldnames = ['alpha_id', 'result', 'correlation_value', 'limit', 'timestamp', 'error']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)

    def run(self):
        """执行批量自相关性检查"""
        self.session = self.sign_in()
        if not self.session:
            return
        alpha_ids = self.extract_alpha_ids()
        if not alpha_ids:
            logging.info("没有待检查的Alpha ID，程序退出")
            return
        for alpha_id in alpha_ids:
            logging.info(f"开始检查Alpha: {alpha_id}")
            check_result = self.check_single_alpha(alpha_id)
            self.save_result(check_result)
            logging.info(f"{alpha_id}检查完成: {check_result['result']}")
            time.sleep(1)
        logging.info("所有Alpha检查完成，结果已保存至: {}".format(self.result_path))

if __name__ == "__main__":
    ALPHA_LIST_MD = 'wati_submit_list.md'
    LOGIN_CREDS = 'brain.txt'

    checker = AutoCorrelationChecker(
        alpha_list_md_path=ALPHA_LIST_MD,
        login_creds_path=LOGIN_CREDS,
        max_retry=35,
        retry_interval=5
    )
    checker.run()
