# 生成一个50MB+、内容不可压缩的xlsx文件
import openpyxl
import random
import string

def random_str(length=32):
    # 生成高熵随机字符串
    return ''.join(random.choices(string.digits, k=length))

wb = openpyxl.Workbook()
ws = wb.active

rows = 170000   # 行数
cols = 50      # 列数
cell_len = 5  # 每个单元格字符串长度

for r in range(1, rows + 1):
    row_data = [random_str(cell_len) for _ in range(cols)]
    ws.append(row_data)
    if r % 100 == 0:
        print(f"写入第{r}行...")

wb.save("random_50mb.xlsx")
print("写入完成！")