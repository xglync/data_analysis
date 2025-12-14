/**
 * DataEngine 类
 * 负责核心数据管理。
 * 特性：
 * 1. 支持联合主键生成唯一 Key。
 * 2. 使用 TypedArray (Int32Array) 存储视图索引，实现零拷贝排序和筛选。
 * 3. 集成 Wasm 排序检测（如有）。
 */
export class DataEngine {
    constructor(config) {
        this.primaryKeys = config.primaryKeys || ['id'];
        this.rawData = [];
        this.viewIndices = null; // 视图索引数组：[0, 1, 5, 2...] 指向 rawData
        this.idMap = new Map(); // Key -> RawIndex 快速查找表

        // 简单检测全局是否有 Wasm 模块
        if (window.Module && window.Module._sort_indices) {
            this.isWasmReady = true;
        }
    }

    loadData(data) {
        this.rawData = data;
        // 初始化索引：默认顺序 0...N
        this.viewIndices = new Int32Array(data.length);
        for (let i = 0; i < data.length; i++) this.viewIndices[i] = i;

        this.buildIdMap();
    }

    /**
     * 生成唯一主键字符串 (e.g., "101" 或 "101::TypeA")
     */
    getCompositeKey(row) {
        if (this.primaryKeys.length === 1) return String(row[this.primaryKeys[0]]);
        return this.primaryKeys.map(k => row[k]).join('::');
    }

    buildIdMap() {
        this.idMap.clear();
        for (let i = 0; i < this.rawData.length; i++) {
            this.idMap.set(this.getCompositeKey(this.rawData[i]), i);
        }
    }

    get count() {
        return this.viewIndices ? this.viewIndices.length : 0;
    }

    /**
     * 获取视图中第 i 行的原始数据
     */
    getRowByIndex(viewIndex) {
        const rawIndex = this.viewIndices[viewIndex];
        return this.rawData[rawIndex];
    }

    /**
     * 排序逻辑
     * 目前降级为 JS 排序，因为 V8 引擎对 TypedArray 的排序优化已经非常出色。
     */
    sort(field, direction = 'asc') {
        if (this.count === 0) return;
        const dir = direction === 'asc' ? 1 : -1;
        const raw = this.rawData;

        // 性能优化：提取列数据到临时数组，避免在 sort 回调中频繁访问对象属性
        const colData = new Array(raw.length);
        for (let i = 0; i < raw.length; i++) colData[i] = raw[i][field];

        this.viewIndices.sort((a, b) => {
            const valA = colData[a];
            const valB = colData[b];
            if (valA < valB) return -dir;
            if (valA > valB) return dir;
            return 0;
        });
    }
}