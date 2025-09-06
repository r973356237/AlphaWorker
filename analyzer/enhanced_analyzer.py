import pandas as pd
import ast
import glob
from datetime import datetime

def find_csv_file():
    """在当前文件夹中查找以 'simulated_alphas_' 开头的CSV文件。"""
    csv_files = glob.glob('simulated_alphas_*.csv')
    if not csv_files:
        print("错误：在当前文件夹中未找到任何以 'simulated_alphas_' 开头的CSV文件。")
        return None
    if len(csv_files) > 1:
        print(f"警告：找到多个以 'simulated_alphas_' 开头的CSV文件: {csv_files}")
        print(f"将使用第一个文件进行分析: {csv_files[0]}")
    return csv_files[0]

def count_failures(check_list):
    """计算检查项中'FAIL'的数量。"""
    if not isinstance(check_list, list):
        return 0
    return sum(1 for check in check_list if check.get('result') == 'FAIL')

def safe_literal_eval(val):
    """
    一个更安全的eval版本，可以处理'nan'和'null'等非标准值。
    """
    if isinstance(val, str):
        s = val.replace('nan', 'None').replace('null', 'None')
        try:
            return ast.literal_eval(s)
        except (ValueError, SyntaxError):
            return {}
    return val

def parse_data(filename):
    """加载并解析CSV文件，提取嵌套的性能指标和检查项。"""
    print(f"正在加载并解析文件: {filename}...")
    try:
        df = pd.read_csv(filename)
        is_data = df['is'].apply(safe_literal_eval)
        is_metrics_df = pd.json_normalize(is_data)
        
        metrics_to_add = ['fitness', 'sharpe', 'returns', 'turnover', 'margin', 'drawdown']
        for metric in metrics_to_add:
            if metric in is_metrics_df.columns:
                df[metric] = is_metrics_df[metric]
            else:
                df[metric] = pd.NA
        
        if 'checks' in is_metrics_df.columns:
            is_metrics_df['checks'] = is_metrics_df['checks'].fillna('').apply(list)
            df['fail_count'] = is_metrics_df['checks'].apply(count_failures)
        else:
            df['fail_count'] = 0

        if 'grade' not in df.columns:
            df['grade'] = 'N/A'
        
        print("数据解析成功。")
        return df.dropna(subset=metrics_to_add)
        
    except FileNotFoundError:
        print(f"错误：文件 '{filename}' 未找到。")
        return None
    except Exception as e:
        print(f"解析数据时出错: {e}")
        return None



def main():
    """主分析函数"""
    
    filename = find_csv_file()
    if not filename: return
        
    df = parse_data(filename)
    if df is None or df.empty: return

    report_filename = "alpha_analysis_report.md"
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Alpha回测结果分析报告\n\n")
        f.write(f"**报告生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**分析文件:** `{filename}`\n\n")

        # 计算排名和综合分数
        df['returns_to_drawdown'] = df['returns'] / df['drawdown']
        df['fitness_rank'] = df['fitness'].rank(ascending=False, method='first')
        df['sharpe_rank'] = df['sharpe'].rank(ascending=False, method='first')
        df['r_dd_rank'] = df['returns_to_drawdown'].rank(ascending=False, method='first')
        df['comprehensive_score'] = df['fitness_rank'] + df['sharpe_rank'] + df['r_dd_rank']
        df['all_checks_passed'] = df['fail_count'].apply(lambda x: '是' if x == 0 else '否')

        # 1. 所有检查通过的Alpha列表 (移到最前面)
        f.write("## 1. 所有检查通过的Alpha列表\n\n")
        all_pass_df = df[df['fail_count'] == 0].copy()
        
        if not all_pass_df.empty:
            all_pass_cols = ['id', 'grade', 'fitness', 'sharpe', 'returns_to_drawdown', 'comprehensive_score']
            f.write(f"共有 **{len(all_pass_df)}** 个Alpha通过了所有检查项，按综合分数排序如下：\n\n")
            f.write("```\n" + all_pass_df.sort_values(by='comprehensive_score')[all_pass_cols].to_string(index=False) + "\n```\n\n")
        else:
            f.write("未发现任何通过所有检查项的Alpha。\n\n")

        # 2. 数据总览
        f.write("## 2. 数据总览\n\n")
        f.write(f"- **Alpha 总数:** {len(df)}\n\n")
        f.write(f"- **不同 'grade' 评级分布:**\n\n")
        f.write("```\n" + df['grade'].value_counts().to_string() + "\n```\n\n")
        
        f.write(f"- **按检查失败项数量分布:**\n\n")
        fail_counts_dist = df['fail_count'].value_counts().sort_index()
        fail_counts_dist.index.name = "失败项数量"
        fail_counts_dist.name = "Alpha数量"
        f.write("```\n" + fail_counts_dist.to_string() + "\n```\n\n")

        # 3. 核心性能指标统计
        f.write("## 3. 核心性能指标统计分析\n\n")
        metrics_to_describe = ['fitness', 'sharpe', 'returns', 'turnover', 'drawdown']
        f.write("```\n" + df[metrics_to_describe].describe().to_string() + "\n```\n\n")

        # 4. 多维度Top 10 Alpha展示
        f.write("## 4. 多维度排行榜 Top 10\n\n")
        
        comp_cols = ['id', 'grade', 'all_checks_passed', 'fail_count', 'comprehensive_score', 'fitness_rank', 'sharpe_rank', 'r_dd_rank']
        perf_cols = ['id', 'grade', 'all_checks_passed', 'fitness', 'sharpe']
        risk_cols = ['id', 'grade', 'all_checks_passed', 'returns_to_drawdown', 'returns', 'drawdown']

        f.write("### 综合排名 Top 10 (优先显示全Pass, 分数越低越好)\n\n")
        f.write("```\n" + df.sort_values(by=['fail_count', 'comprehensive_score'], ascending=[True, True]).head(10)[comp_cols].to_string(index=False) + "\n```\n\n")
        
        f.write("### **潜力Alpha**排行 Top 10 (有未通过项, 但指标优秀)\n\n")
        
        # --- 错误修复 ---
        # 错误行: failed_but_good_df = df[df[df['fail_count'] > 0].index]
        # 修正后:
        failed_but_good_df = df[df['fail_count'] > 0].copy()
        
        if not failed_but_good_df.empty:
            f.write("```\n" + failed_but_good_df.sort_values(by='comprehensive_score').head(10)[comp_cols].to_string(index=False) + "\n```\n\n")
        else:
            f.write("未发现有失败项的Alpha。\n\n")

        f.write("### Fitness 排行 Top 10\n\n```\n" + df.sort_values(by='fitness', ascending=False).head(10)[perf_cols].to_string(index=False) + "\n```\n\n")
        f.write("### Sharpe 排行 Top 10\n\n```\n" + df.sort_values(by='sharpe', ascending=False).head(10)[perf_cols].to_string(index=False) + "\n```\n\n")
        f.write("### 收益/回撤比 排行 Top 10\n\n```\n" + df.sort_values(by='returns_to_drawdown', ascending=False).head(10)[risk_cols].to_string(index=False) + "\n```\n\n")



    print(f"\n分析全部完成！详细报告已保存到文件: {report_filename}")

if __name__ == "__main__":
    main()