import pandas as pd
import numpy as np
import os
from pathlib import Path
import warnings
import polars as pl
import time
warnings.filterwarnings("ignore")

def read_pl(fp, header=None):
    df = pl.read_excel(fp, has_header=False)
    return df.to_pandas();

def read_pl_csv(fp, header=None):
    df = pl.read_csv(fp, has_header=False)
    return df.to_pandas();

# 导出数据需要按工位分开
# 筛选R1测试PASS的数据
# 当次运行被主动删除的测试项会被保存在deleted_columns_report.csv中，计算CPK时被排除的列（不能转换成数值类型的列）不会显示
# 最终会生成清洗后的数据和CPK数据以及直方图20个频数数据
# 针对OT工位会进行特殊合并after和before
# TODO: 手动选择需要合并的列

# 填充同名列并删除多余列
def Merge(df, station=None):
    # 获取原始列的唯一顺序（按首次出现顺序）
    unique_columns = []
    seen = set()
    for col in df.columns:
        if col not in seen:
            unique_columns.append(col)
            seen.add(col)

    # 按列名分组，横向填充空值，并保留每组的第一列
    filled_cols = []
    for col in unique_columns:
        # 提取当前列名对应的所有列
        cols_group = df.loc[:, df.columns == col]
        if(len(cols_group.columns) >= 1):
            # 横向填充空值（优先用右侧非空值）
            filled = cols_group.bfill(axis=1).iloc[:, 0]
            filled_cols.append(filled)

    # 合并结果并保持原始列顺序
    df_merged = pd.concat(filled_cols, axis=1)
    
    # OT特殊处理 填充AfterCalib
    if(((station == "OT_M") or (station == "OT_M2"))):
        STATION = "M" if(station == "OT_M") else "M2"
        target1 = STATION + "-BeforeCalib-CompressRate-Center-X"
        target2 = STATION + "-AfterCalib-CompressRate-Center-X"
        target3 = STATION + "-BeforeCalib-CompressRate-Center-Y"
        target4 = STATION + "-AfterCalib-CompressRate-Center-Y"
        df_merged[target2] = df_merged[target2].fillna(df_merged[target1])
        df_merged[target4] = df_merged[target4].fillna(df_merged[target3])
        df_merged[target1][1] = 22
        df_merged[target3][1] = 22
        target1 = STATION + "-BeforeCalib-GyroGain-X"
        target2 = STATION + "-AfterCalib-GyroGain-X"
        target3 = STATION + "-BeforeCalib-GyroGain-Y"
        target4 = STATION + "-AfterCalib-GyroGain-Y"
        df_merged[target2] = df_merged[target2].fillna(df_merged[target1])
        df_merged[target4] = df_merged[target4].fillna(df_merged[target3])
        target1 = STATION + "-BeforeCalib-OisOnPixel-X"
        target2 = STATION + "-AfterCalib-OisOnPixel-X"
        target3 = STATION + "-BeforeCalib-OisOnPixel-Y"
        target4 = STATION + "-AfterCalib-OisOnPixel-Y"
        df_merged[target2] = df_merged[target2].fillna(df_merged[target1])
        df_merged[target4] = df_merged[target4].fillna(df_merged[target3])

    return df_merged

def Clean(df) :
    if len(df) < 3:
        return df  # 如果行数不足3行，直接返回原DataFrame
    
    # 获取第二行和第三行的值
    row2 = df.iloc[1]
    row3 = df.iloc[2]
    
    # 比较两行：值相等 或 都是NaN
    mask = [
        not (  # 不保留的条件：值相等或都是NaN
            (a == b) or 
            (pd.isna(a) and pd.isna(b))
        )
        for a, b in zip(row2, row3)
    ]
    
    # 应用掩码，保留符合条件的列
    return df.loc[:, mask]

def DoFilter():
    # 存储被删除的列名（格式：{文件名: [被删除的列列表], ...}）
    deleted_columns_dict = {}

    folder_path = os.getcwd()
    folder = Path(folder_path)
    excel_files = list(folder.glob('*.xlsx'))
    if not excel_files:
        raise Exception("未找到Excel文件")

    for excel_file in excel_files:
        try:
            oldfullname = os.path.basename(excel_file)
            (oldname, oldext) = os.path.splitext(oldfullname)
            (solution, station) = oldname.split(" ",1)
            # print(solution, station)
            station = station.split("-",1)[0].split(" ", 1)[0]
            # print(solution, station)

            df = read_pl(excel_file, header=None)
            df.columns = df.iloc[0]
            original_columns = df.columns.tolist()
            df_merged = Merge(df, station)
            df_cleaned = Clean(df_merged)
            df_cleaned.to_csv("Data_" + oldname + ".csv", sep=',', index=False, header=None)

            # 记录被删除的列（保留原始顺序）
            deleted_cols = [col for col in original_columns if col not in df_cleaned.columns]
            deleted_cols.sort()
            # 获取文件名（不含路径）
            deleted_columns_dict[oldname] = deleted_cols

        except BaseException as e:
            print("Read File Fail:")
            print(excel_file)
            print("Error:")
            print(e)
        else:
            print("Read Success:")
            print(excel_file)

    # 将字典转换为 DataFrame
    if deleted_columns_dict.values():
        max_length = max(len(cols) for cols in deleted_columns_dict.values())
    for key in deleted_columns_dict:
        deleted_columns_dict[key] += [''] * (max_length - len(deleted_columns_dict[key]))

    df = pd.DataFrame(deleted_columns_dict)

    # 导出到 CSV 文件
    output_path = 'deleted_columns_report.csv'
    df.to_csv(output_path, index=False)

    print(f"被过滤列已保存至: {output_path}")
    print("开始计算CPK")
    # input()


def is_no_limit(value):
    """判断数值是否为无规格限标记（以至少3个9结尾的整数）"""
    try:
        str_val = str(int(abs(value)))
        return len(str_val) >= 3 and str_val.endswith('999')
    except:
        return False
        
def calculate_process_capability(df, solution, station):
    metrics = []
    for col in range(1, df.shape[1]):  # 遍历每个测试项列
        # 提取元数据
        name = df.iloc[0, col]        # 测试项名称
        raw_lsl = df.iloc[1, col]     # 原始规格下限
        raw_usl = df.iloc[2, col]     # 原始规格上限
        
        # 转换并处理规格限
        usl = pd.to_numeric(raw_usl, errors='coerce')
        lsl = pd.to_numeric(raw_lsl, errors='coerce')
        
        # 处理以999结尾的无规格限标记
        if not pd.isna(usl) and is_no_limit(usl):
            usl = np.nan
        if not pd.isna(lsl) and is_no_limit(lsl):
            lsl = np.nan
        
        # 提取测量值并过滤无效数据
        values = pd.to_numeric(df.iloc[3:, col], errors='coerce').dropna()
        if values.empty:
            continue
        
        # 生成结果时替换NaN为999999/-999999
        result_usl = 999999 if pd.isna(usl) | (usl==lsl) else usl
        result_lsl = -999999 if pd.isna(lsl) | (usl==lsl) else lsl
        
        # 转换为整数（如果值为整数）
        if isinstance(result_usl, float) and result_usl.is_integer():
            result_usl = int(result_usl)
        if isinstance(result_lsl, float) and result_lsl.is_integer():
            result_lsl = int(result_lsl)

        # 基础统计量
        stat = {
            "Solution": solution,
            "Station": station,
            "Item": name,
            "USL": result_usl,
            "LSL": result_lsl,
            "MIN": values.min(),
            "MAX": values.max(),
            "AVG": values.mean(),
            "Sigma": values.std(ddof=1)
        }
        
        # 过程能力计算
        mu, sigma = stat["AVG"], stat["Sigma"]
        stat.update({ "CA": np.nan, "CP": np.nan, "CPK": np.nan })
        
        # 计算CA（需双边规格）
        if not pd.isna(usl) and not pd.isna(lsl):
            target = (usl + lsl) / 2
            tolerance_half = (usl - lsl) / 2
            if tolerance_half != 0:
                stat["CA"] = (mu - target) / tolerance_half
        
        # 计算CP（需双边规格）
        if not pd.isna(usl) and not pd.isna(lsl) and sigma != 0:
            stat["CP"] = (usl - lsl) / (6 * sigma)
        
        # 修正点：正确闭合括号
        cpu = (usl - mu)/(3*sigma) if (not pd.isna(usl)) and (sigma != 0) else np.nan
        cpl = (mu - lsl)/(3*sigma) if (not pd.isna(lsl)) and (sigma != 0) else np.nan
        
        # 修正点：列表推导式语法
        valid_cpk = [v for v in [cpu, cpl] if not pd.isna(v)]
        if valid_cpk:
            stat["CPK"] = min(valid_cpk) if len(valid_cpk) > 1 else valid_cpk[0]
        
        # --------------------------
        # 新增：20段区间频数计算
        # --------------------------
        # 确定分段范围优先级：1.规格限 2.数据范围
        lower = lsl if not pd.isna(lsl) else values.min()
        upper = usl if not pd.isna(usl) else values.max()
        
        # 处理无效范围（如单边规格且数据超出）
        if pd.isna(lower): lower = values.min()
        if pd.isna(upper): upper = values.max()
        if lower >= upper:  # 无效范围时使用数据范围
            lower, upper = values.min(), values.max()
        
        # 生成20个等宽区间
        bins = np.linspace(lower, upper, 21)  # 20段需要21个边界点
        freq, bin_edges = np.histogram(values, bins=bins)
        
        # 将频数数据写入字典
        for i in range(20):
            stat[f"Range_Lower_Limit{i+1}"] = bin_edges[i]
            stat[f"Range_Upper_Limit{i+1}"] = bin_edges[i+1]
            stat[f"Frequency{i+1}"] = freq[i]
        
        metrics.append(stat)
    
    # 生成结果列名（动态生成频数列名）
    base_columns = ["Solution", "Station", "Item", "USL", "LSL", "MIN", "MAX", "AVG", "Sigma", "CA", "CP", "CPK"]
    freq_columns = []
    for i in range(1, 21):
        freq_columns.extend([f"Range_Lower_Limit{i}", f"Range_Upper_Limit{i}", f"Frequency{i}"])
    
    return pd.DataFrame(metrics)[base_columns + freq_columns].sort_values(by="Item")

def DoCpk() :
    folder_path = os.getcwd()
    folder = Path(folder_path)
    excel_files = list(folder.glob('Data*.csv'))
    if not excel_files:
        raise Exception("未找到CSV文件")
        
    for excel_file in excel_files:
        try:
            oldfullname = os.path.basename(excel_file)
            # print(oldfullname)
            (oldname, oldext) = os.path.splitext(oldfullname)
            # print(oldname)
            # (solution, station) = oldname.split("_",1)[1].split("-",1)[0].split(" ",1);
            # station = station.split("-",1)[0].split(" ", 1)[0]
            (solution, station) = oldname.split("_",1)[1].split(" ")[0:2]
            if((station == "AT") | (station == "MMILOG_OFFLINE")):
                station = "MMIE"
            df = read_pl_csv(excel_file, header=None)
            capability_df = calculate_process_capability(df, solution, station)
            capability_df.to_csv("CPK_" + oldname.split("_",1)[1] + ".csv", index=False, float_format="%.8f")
        except BaseException as e:
            print("Read File Fail:")
            print(excel_file)
            print("Error: ")
            print(e)
        else:
            print("计算结果已保存到"+ "CPK_" + oldname.split("_",1)[1] + ".csv")
    print("计算结束")
    # input()

def DoProcess(path) :
    os.chdir(path)
    DoFilter();
    DoCpk();

start_time = time.time()
DoProcess(".")
end_time = time.time()
print(end_time-start_time)