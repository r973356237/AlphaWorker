import requests
import logging
import time
import csv
import re
import threading
import concurrent.futures
from queue import Queue
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
    def __init__(self, alpha_list_md_path, login_creds_path, max_retry=35, retry_interval=5, thread_count=5):
        self.alpha_list_md_path = alpha_list_md_path
        self.login_creds_path = login_creds_path
        self.max_retry = max_retry
        self.retry_interval = retry_interval
        self.thread_count = thread_count
        self.session = None
        self.result_path = f'self_correlation_results_{datetime.now().strftime("%Y%m%d")}.csv'
        self.result_lock = threading.Lock()  # 添加锁以保护结果写入

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
        """从alpha_analysis_report.md文件的'所有检查通过的Alpha列表'部分提取Alpha ID"""
        ids = []
        try:
            with open(self.alpha_list_md_path, 'r', encoding='utf-8') as f:  # 读取时指定编码
                content = f.read()
            
            # 查找"## 1. 所有检查通过的Alpha列表"部分
            section_start = content.find("## 1. 所有检查通过的Alpha列表")
            if section_start == -1:
                logging.error("未找到'所有检查通过的Alpha列表'部分")
                return []
            
            # 从该部分开始查找代码块
            section_content = content[section_start:]
            
            # 查找第一个```开始的代码块
            code_start = section_content.find("```")
            if code_start == -1:
                logging.error("未找到代码块开始标记")
                return []
            
            # 查找代码块结束标记
            code_content_start = section_content.find("\n", code_start) + 1
            code_end = section_content.find("```", code_content_start)
            if code_end == -1:
                logging.error("未找到代码块结束标记")
                return []
            
            # 提取代码块内容
            code_block = section_content[code_content_start:code_end]
            lines = code_block.strip().split('\n')
            
            # 解析表格数据，提取Alpha ID（第一列）
            for line in lines:
                line = line.strip()
                if not line or 'id' in line.lower():  # 跳过空行和表头
                    continue
                
                # 使用正则表达式提取第一列的Alpha ID
                id_pattern = re.compile(r'^\s*(\w+)\s+')
                match = id_pattern.match(line)
                if match:
                    alpha_id = match.group(1)
                    ids.append(alpha_id)
        
            logging.info(f"成功从'所有检查通过的Alpha列表'提取{len(ids)}个有效Alpha ID")
            return ids
        except FileNotFoundError:
            logging.error(f"未找到文件: {self.alpha_list_md_path}")
            return []
        except Exception as e:
            logging.error(f"提取ID失败: {e}")
            return []

    def load_processed_alpha_ids(self):
        """加载已经处理过的Alpha ID，避免重复检查"""
        processed_ids = set()
        try:
            if Path(self.result_path).exists():
                with open(self.result_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('alpha_id'):
                            processed_ids.add(row.get('alpha_id'))
                logging.info(f"从现有结果文件中读取到{len(processed_ids)}个已处理的Alpha ID")
        except Exception as e:
            logging.error(f"读取已处理Alpha ID失败: {e}")
        return processed_ids

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
        """保存检查结果到CSV（线程安全）"""
        with self.result_lock:  # 使用锁确保线程安全
            file_exists = Path(self.result_path).exists()
            with open(self.result_path, 'a', newline='', encoding='utf-8') as f:  # 保持UTF-8编码
                fieldnames = ['alpha_id', 'result', 'correlation_value', 'limit', 'timestamp', 'error']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(result)

    def process_alpha(self, alpha_id):
        """处理单个Alpha的检查（供线程池调用）"""
        logging.info(f"开始检查Alpha: {alpha_id}")
        check_result = self.check_single_alpha(alpha_id)
        self.save_result(check_result)
        logging.info(f"{alpha_id}检查完成: {check_result['result']}")
        return alpha_id, check_result['result']

    def run(self):
        """执行批量自相关性检查（多线程版本）"""
        self.session = self.sign_in()
        if not self.session:
            return
            
        # 获取所有需要检查的Alpha ID
        alpha_ids = self.extract_alpha_ids()
        if not alpha_ids:
            logging.info("没有待检查的Alpha ID，程序退出")
            return
            
        # 加载已处理过的Alpha ID
        processed_ids = self.load_processed_alpha_ids()
        
        # 过滤掉已处理的Alpha ID
        alpha_ids_to_check = [alpha_id for alpha_id in alpha_ids if alpha_id not in processed_ids]
        
        if not alpha_ids_to_check:
            logging.info("所有Alpha ID已处理完毕，无需重复检查")
            return
            
        total_alphas = len(alpha_ids_to_check)
        skipped_alphas = len(alpha_ids) - total_alphas
        
        logging.info(f"共有{len(alpha_ids)}个Alpha，其中{skipped_alphas}个已处理，{total_alphas}个待检查，使用{self.thread_count}个线程并行处理")
        
        # 使用线程池并行处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_count) as executor:
            # 提交所有任务到线程池
            future_to_alpha = {executor.submit(self.process_alpha, alpha_id): alpha_id for alpha_id in alpha_ids_to_check}
            
            # 处理完成的任务结果
            completed = 0
            for future in concurrent.futures.as_completed(future_to_alpha):
                alpha_id = future_to_alpha[future]
                try:
                    _, result = future.result()
                    completed += 1
                    if completed % 10 == 0 or completed == total_alphas:
                        logging.info(f"进度: {completed}/{total_alphas} ({completed/total_alphas*100:.1f}%)")
                except Exception as e:
                    logging.error(f"{alpha_id}处理异常: {e}")
        
        logging.info(f"所有Alpha检查完成，共处理{total_alphas}个Alpha，跳过{skipped_alphas}个已处理Alpha，结果已保存至: {self.result_path}")

if __name__ == "__main__":
    ALPHA_LIST_MD = 'alpha_analysis_report.md'
    LOGIN_CREDS = 'brain.txt'

    checker = AutoCorrelationChecker(
        alpha_list_md_path=ALPHA_LIST_MD,
        login_creds_path=LOGIN_CREDS,
        max_retry=30,
        retry_interval=5,
        thread_count=5  # 设置为5个线程并行处理
    )
    checker.run()
