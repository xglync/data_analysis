# 生成一个50MB+、内容不可压缩的xlsx文件
import openpyxl
import time

start_time = time.time()
work_book = openpyxl.load_workbook('D:/code/dashboard/data_analysis/random_50mb.xlsx')
sheet = work_book.active

for i in range(1, 170000):
    temp = sheet.cell(row=i, column=1).value

end_time = time.time()
print(end_time-start_time)