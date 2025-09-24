#include <emscripten/emscripten.h>
#include <stdio.h>
#include <stdlib.h>
#include <xlsxio_read.h>

#include <xlnt/workbook/workbook.hpp>
#include <xlnt/xlnt.hpp>
#ifdef __cplusplus
#define EXTERN extern "C"
#else
#define EXTERN
#endif

#include <vector>
using std::string;
using std::vector;
class excelinfo {
public:
    vector<string> headers;
    vector<vector<string>> data;
    xlsxioreadersheet sheet;
};


int main() {
    printf("WASM module loaded. Use read_xlsx(filename) to read an xlsx file.\n");
    return 0;
}

EXTERN EMSCRIPTEN_KEEPALIVE unsigned long long int regist_excel(const char* filename) {
    xlsxioreader xlsxioread;
    if ((xlsxioread = xlsxioread_open(filename)) == NULL) {
        printf("Error opening .xlsx file: %s\n", filename);
        return 0;
    }
    xlsxioreadersheet sheet;
    int count = 0;
    // 读取第一个sheet
    if ((sheet = xlsxioread_sheet_open(xlsxioread, NULL, XLSXIOREAD_SKIP_EMPTY_ROWS)) != NULL) {
        excelinfo* info = new excelinfo();
        info->sheet = sheet;
        if (xlsxioread_sheet_next_row(sheet)) {
            char* value;
            while ((value = xlsxioread_sheet_next_cell(sheet)) != NULL) {
                info->headers.push_back(string(value));
                free(value);
            }
        }
        return (unsigned long long int)info;
    }
    return 0;
}

EXTERN EMSCRIPTEN_KEEPALIVE void get_headers(unsigned long long int index) {
    if (index) {
        excelinfo* info = (excelinfo*)index;
        printf("%lld = %llx Headers:\n", index, index);
        for (const auto& header : info->headers) {
            printf("%s\t", header.c_str());
        }
        printf("\n");
    }
}

// 读取指定 xlsx 文件并打印内容
EXTERN EMSCRIPTEN_KEEPALIVE void read_xlsx(const char* filename) {
    xlsxioreader xlsxioread;
    if ((xlsxioread = xlsxioread_open(filename)) == NULL) {
        printf("Error opening .xlsx file: %s\n", filename);
        return;
    }
    xlsxioreadersheet sheet;
    int count = 0;
    // 读取第一个sheet
    if ((sheet = xlsxioread_sheet_open(xlsxioread, NULL, XLSXIOREAD_SKIP_EMPTY_ROWS)) != NULL) {
        char* value;
        while (xlsxioread_sheet_next_row(sheet)) {
            while ((value = xlsxioread_sheet_next_cell(sheet)) != NULL) {
                count++;
                // printf("%s\t", value);
                free(value);
            }
            // printf("\n");
        }
        xlsxioread_sheet_close(sheet);
    }
    xlsxioread_close(xlsxioread);
    printf("Read done %d.\n", count);
}

// 读取指定 xlsx 文件并打印内容
EXTERN EMSCRIPTEN_KEEPALIVE void read_xlsx_xlnt(const char* filename) {
    printf("1.\n");
    xlnt::workbook wb;
    printf("2.\n");
    wb.load(filename);
    printf("3.\n");
    auto ws = wb.active_sheet();
    printf("Processing spread sheet.\n");
    for (auto row : ws.rows(false)) {
        // for (auto cell : row)
        {
            // printf("%s\n",cell.to_string().c_str());
            // std::clog << cell.to_string() << std::endl;
        }
    }
    printf("Processing complete.\n");
    printf("Read done.\n");
}
EXTERN EMSCRIPTEN_KEEPALIVE void read_large_file_streaming(const char* filename) {
    xlnt::streaming_workbook_reader wb;
    // 启用流式读取模式
    wb.open(filename);

    // auto ws = wb.active_sheet();
    size_t row_count = 0;

    auto start = std::chrono::high_resolution_clock::now();

    if (wb.has_worksheet("Sheet")) {
        wb.begin_worksheet("Sheet");
    }
    while (wb.has_cell()) {
        auto cell = wb.read_cell();
        if (cell.has_value()) {
            // 根据数据类型处理
            auto value = cell.value<int>();
            // ...业务逻辑处理
        }
        row_count++;
        // if (row_count % 10000 == 0) {
        //     std::cout << "已处理 " << row_count << " 行" << std::endl;
        // }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "总行数: " << row_count << ", 耗时: " << duration.count() << "ms" << std::endl;
}
