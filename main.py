#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlphaWorker 主程序
整合 AlphaCreator 和 AlphaSimulator，实现完整的Alpha生成和回测工作流程
"""

import json
import logging
import os
from datetime import datetime
from AlphaCreator import AlphaCreator
from AlphaSimulator import AlphaSimulator


def setup_logging():
    """
    配置日志系统
    """
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f'alpha_worker_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )


def load_credentials(credentials_file='brain.txt'):
    """
    加载登录凭证
    
    Args:
        credentials_file: 凭证文件路径
        
    Returns:
        tuple: (username, password)
    """
    try:
        with open(credentials_file, 'r') as f:
            credentials = json.load(f)
        return credentials[0], credentials[1]
    except Exception as e:
        logging.error(f"加载凭证失败: {str(e)}")
        return None, None


def create_alphas():
    """
    创建Alpha并保存到CSV文件
    
    Returns:
        bool: 是否成功创建
    """
    print("=" * 50)
    print("开始Alpha创建流程")
    print("=" * 50)
    
    # 创建AlphaCreator实例
    creator = AlphaCreator()
    
    # 执行Alpha创建和保存（所有参数由AlphaCreator内部控制）
    success = creator.create_and_save_alphas(
        filename='alpha_list_pending_simulated.csv'
    )
    
    if success:
        print(f"✅ Alpha创建成功！生成了 {len(creator.alpha_list)} 个Alpha")
        print("📁 文件已保存为: alpha_list_pending_simulated.csv")
        logging.info(f"Alpha创建成功，生成了 {len(creator.alpha_list)} 个Alpha")
    else:
        print("❌ Alpha创建失败")
        logging.error("Alpha创建失败")
    
    return success


def simulate_alphas(max_concurrent=3, batch_size=20):
    """
    执行Alpha回测
    
    Args:
        max_concurrent: 最大并发回测数
        batch_size: 每批处理的Alpha数量
        
    Returns:
        bool: 是否成功启动回测
    """
    print("=" * 50)
    print("开始Alpha回测流程")
    print("=" * 50)
    
    # 检查CSV文件是否存在
    csv_file = 'alpha_list_pending_simulated.csv'
    if not os.path.exists(csv_file):
        print(f"❌ 找不到Alpha文件: {csv_file}")
        logging.error(f"找不到Alpha文件: {csv_file}")
        return False
    
    # 加载凭证
    username, password = load_credentials()
    if not username or not password:
        print("❌ 加载登录凭证失败")
        return False
    
    try:
        # 创建AlphaSimulator实例
        simulator = AlphaSimulator(
            max_concurrent=max_concurrent,
            username=username,
            password=password,
            alpha_list_file_path=csv_file,
            batch_number_for_every_queue=batch_size
        )
        
        print(f"✅ AlphaSimulator初始化成功")
        print(f"📊 配置: 最大并发={max_concurrent}, 批处理大小={batch_size}")
        print(f"📁 Alpha文件: {csv_file}")
        print("🚀 开始管理回测任务...")
        
        logging.info(f"AlphaSimulator启动，max_concurrent={max_concurrent}, batch_size={batch_size}")
        
        # 开始管理回测任务（这是一个无限循环）
        simulator.manage_simulations()
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断了回测流程")
        logging.info("用户中断了回测流程")
        return True
    except Exception as e:
        print(f"❌ 回测过程中发生错误: {str(e)}")
        logging.error(f"回测过程中发生错误: {str(e)}")
        return False


def main():
    """
    主函数
    """
    # 设置日志
    setup_logging()
    
    print("🚀 AlphaWorker 启动")
    print(f"⏰ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 用户选择模式
    print("\n请选择运行模式:")
    print("1. 仅创建Alpha (生成alpha_list_pending_simulated.csv)")
    print("2. 仅执行回测 (需要已存在的alpha_list_pending_simulated.csv)")
    print("3. 完整流程 (先创建Alpha，然后执行回测)")
    
    while True:
        try:
            choice = input("\n请输入选择 (1/2/3): ").strip()
            if choice in ['1', '2', '3']:
                break
            else:
                print("❌ 无效选择，请输入 1、2 或 3")
        except KeyboardInterrupt:
            print("\n👋 程序退出")
            return
    
    # 根据选择执行相应流程
    if choice == '1':
        # 仅创建Alpha
        success = create_alphas()
        if success:
            print("\n✅ Alpha创建完成！")
            print("💡 提示: 可以运行模式2来执行回测")
        
    elif choice == '2':
        # 仅执行回测
        print("\n请配置回测参数:")
        try:
            max_concurrent = int(input("最大并发回测数 (默认3): ") or "3")
            batch_size = int(input("每批处理Alpha数量 (默认20): ") or "20")
        except ValueError:
            print("❌ 参数输入错误，使用默认值")
            max_concurrent = 3
            batch_size = 20
        
        simulate_alphas(max_concurrent, batch_size)
        
    elif choice == '3':
        # 完整流程
        print("\n🔄 执行完整流程...")
        
        # 步骤1: 创建Alpha
        success = create_alphas()
        if not success:
            print("❌ Alpha创建失败，流程终止")
            return
        
        # 询问是否继续回测
        print("\n✅ Alpha创建完成！")
        continue_choice = input("是否继续执行回测？(y/n): ").strip().lower()
        
        if continue_choice in ['y', 'yes', '是', '']:
            # 步骤2: 配置并执行回测
            print("\n请配置回测参数:")
            try:
                max_concurrent = int(input("最大并发回测数 (默认3): ") or "3")
                batch_size = int(input("每批处理Alpha数量 (默认20): ") or "20")
            except ValueError:
                print("❌ 参数输入错误，使用默认值")
                max_concurrent = 3
                batch_size = 20
            
            simulate_alphas(max_concurrent, batch_size)
        else:
            print("⏹️  跳过回测流程")
    
    print("\n👋 AlphaWorker 结束")

if __name__ == '__main__':
    main()