# WorldQuant Brain Alpha Worker

一个用于自动化创建和回测 WorldQuant Brain Alpha 策略的 Python 工具集。

## 项目简介

本项目提供了一套完整的工具链，用于：
- 自动生成 Alpha 表达式
- 批量创建 Alpha 策略
- 执行回测模拟
- 分析 Alpha 性能

## 功能特性

### 🚀 核心功能
- **Alpha 自动生成**：基于数据字段自动生成多样化的 Alpha 表达式
- **批量回测**：支持大批量 Alpha 策略的并行回测
- **性能分析**：提供详细的 Alpha 性能分析和可视化报告
- **灵活配置**：支持多种数据集和搜索范围配置

## 项目结构

```
AlphaWorker/
├── AlphaCreator.py          # Alpha 创建器核心类
├── AlphaSimulator.py        # Alpha 回测模拟器
├── main.py                  # 主程序入口
├── brain.txt               # 登录凭证（不会同步到 Git）
├── analyzer/               # 分析工具目录
│   ├── enhanced_analyzer.py # 增强分析器
│   └── *.png               # 分析图表
├── *.csv                   # 数据文件（不会同步到 Git）
├── *.log                   # 日志文件（不会同步到 Git）
└── __pycache__/            # Python 缓存（不会同步到 Git）
```

## 安装要求

### Python 版本
- Python 3.8+

### 依赖包
```bash
pip install requests pandas matplotlib seaborn numpy
```

## 配置说明

### 1. 登录凭证配置
创建 `brain.txt` 文件，格式如下：
```json
[
    "your_email@example.com",
    "your_password"
]
```

### 2. 数据集配置
在 `AlphaCreator.py` 中的 `create_and_save_alphas` 方法中修改：
```python
# 数据集ID配置（用户可以直接在这里修改）
dataset_id = 'socialmedia12'  # 可选: fundamental6, socialmedia12, etc.
```

### 3. 搜索范围配置
```python
# 配置搜索范围（用户可以直接在这里修改）
search_scope = {
    'region': 'USA', 
    'delay': '1', 
    'universe': 'TOP3000', 
    'instrumentType': 'EQUITY'
}
```

## 使用方法

### 快速开始

1. **克隆项目**
```bash
git clone <repository-url>
cd AlphaWorker
```

2. **配置登录信息**
```bash
# 创建 brain.txt 文件并填入您的 WorldQuant Brain 登录信息
echo '["your_email", "your_password"]' > brain.txt
```

3. **运行主程序**
```bash
python main.py
```

### 运行模式

程序提供三种运行模式：

#### 模式 1：仅创建 Alpha
- 生成 Alpha 表达式
- 创建 Alpha 对象
- 保存到 CSV 文件

#### 模式 2：仅执行回测
- 读取已有的 Alpha 列表
- 执行批量回测
- 生成回测结果

#### 模式 3：完整流程
- 创建 Alpha + 执行回测
- 一站式完成所有操作

### 高级用法

#### 自定义 Alpha 表达式模板
在 `AlphaCreator.py` 的 `generate_alpha_expressions` 方法中修改模板：

```python
templates = [
    "rank(ts_regression({}, {}, {})) * -1",
    "rank(ts_regression({}, {}, {})) * rank({})",
    # 添加更多模板...
]
```

#### 批量分析结果
使用增强分析器：
```bash
cd analyzer
python enhanced_analyzer.py
```

## 输出文件说明

### CSV 文件
- `alpha_list_pending_simulated.csv` - 待回测的 Alpha 列表
- `simulated_alphas_YYYYMMDD.csv` - 回测结果文件
- `sim_queue.csv` - 模拟队列文件

### 日志文件
- `alpha_worker_YYYYMMDD.log` - 程序运行日志
- `alpha_creator.log` - Alpha 创建日志

### 分析报告
- `analyzer/alpha_analysis_report.md` - 详细分析报告
- `analyzer/*.png` - 可视化图表

## 常见问题

### Q: 出现 "unknown variable 'd1'" 错误
A: 这通常是因为 Alpha 表达式模板中的变量未正确格式化。确保在 `generate_alpha_expressions` 方法中使用了正确的字符串格式化。

### Q: 单位不兼容警告
A: 检查数据集与 Alpha 表达式的兼容性。某些函数（如 `ts_regression`）需要特定单位的输入数据。

### Q: 达到最大并发模拟数
A: WorldQuant Brain 限制了同时进行的模拟数量。程序会自动等待，或者可以分批处理 Alpha。

## 开发指南

### 代码规范
- 使用简体中文注释
- 遵循 PEP 8 代码风格
- 变量名使用 snake_case
- 类名使用 PascalCase

### 扩展功能
1. **添加新的 Alpha 模板**：在 `generate_alpha_expressions` 方法中添加
2. **支持新数据集**：修改 `dataset_id` 配置
3. **自定义分析器**：在 `analyzer/` 目录下添加新的分析脚本

## 许可证

本项目仅供学习和研究使用。使用时请遵守 WorldQuant Brain 的服务条款。

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个项目！

## 联系方式

如有问题或建议，请通过 GitHub Issues 联系。

---

**注意**：使用本工具前，请确保您已经注册了 WorldQuant Brain 账户并了解相关的使用条款。