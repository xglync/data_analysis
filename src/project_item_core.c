#include <stdlib.h>

/**
 * core.c
 * WebAssembly 核心逻辑，提供高性能计算支持。
 */

 /**
  * 快速排序实现
  * 用于在 Wasm 线性内存中直接对索引数组进行排序，避免 JS 对象的频繁 GC。
  *
  * @param data_ptr  原始数据在堆中的起始指针
  * @param indices   需要排序的索引数组指针
  * @param left      左边界
  * @param right     右边界
  * @param type      数据类型 (0: int, 1: double)
  * @param asc       排序方向 (1: 升序, 0: 降序)
  */
void quick_sort(void* data_ptr, int* indices, int left, int right, int type, int asc) {
    if (left >= right) return;
    int pivot_idx = indices[right];
    int i = left - 1;
    for (int j = left; j < right; j++) {
        int current_idx = indices[j];
        int condition = 0;

        // 根据类型指针转换，读取数据值进行比较
        if (type == 0) { // Int
            int* data = (int*)data_ptr;
            condition = asc ? (data[current_idx] < data[pivot_idx]) : (data[current_idx] > data[pivot_idx]);
        } else { // Double
            double* data = (double*)data_ptr;
            condition = asc ? (data[current_idx] < data[pivot_idx]) : (data[current_idx] > data[pivot_idx]);
        }

        if (condition) {
            i++;
            int temp = indices[i];
            indices[i] = indices[j];
            indices[j] = temp;
        }
    }
    int temp = indices[i + 1];
    indices[i + 1] = indices[right];
    indices[right] = temp;

    int partition = i + 1;
    quick_sort(data_ptr, indices, left, partition - 1, type, asc);
    quick_sort(data_ptr, indices, partition + 1, right, type, asc);
}

// JS 调用的入口函数
void sort_indices(void* data_ptr, int* indices, int length, int type, int asc) {
    quick_sort(data_ptr, indices, 0, length - 1, type, asc);
}