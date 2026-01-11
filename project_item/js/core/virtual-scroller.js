/**
 * VirtualScroller 类
 * 负责虚拟滚动的坐标计算。
 * 核心特性：支持不定高度的行（通过 heightMap 记录展开行的高度）。
 */
export class VirtualScroller {
    constructor(config) {
        this.baseRowHeight = config.rowHeight || 40;
        this.totalCount = 0;
        this.viewportHeight = 0;

        // Map<ViewIndex, Height>
        // 仅存储高度不为 baseRowHeight 的行（即展开行）
        this.heightMap = new Map();

        // Int32Array，缓存每一行的 Top 偏移量，避免每次渲染都遍历 O(N)
        this.offsets = null;
        this.totalHeight = 0;
        this.dirty = true; // 脏标记，指示是否需要重算 offset
    }

    /**
     * 重置所有高度记录（用于排序或数据重置后）
     */
    resetHeights() {
        this.heightMap.clear();
        this.dirty = true;
    }

    updateMetrics(count, viewportH) {
        this.viewportHeight = viewportH;
        if (this.totalCount !== count) {
            this.totalCount = count;
            this.dirty = true;
            // 【重要修改】移除在此处的 clear()。
            // 只有显式调用 resetHeights 时（如排序、彻底换数据）才清空，
            // 否则在滚动时如果触发了 metrics 更新，会导致高度塌陷引发闪跳。
        }
    }

    setRowHeight(idx, h) {
        if (h === this.baseRowHeight) {
            this.heightMap.delete(idx);
        } else {
            this.heightMap.set(idx, h);
        }
        this.dirty = true;
    }

    /**
     * 计算累积高度
     */
    _calc() {
        if (!this.dirty) return;

        this.offsets = new Int32Array(this.totalCount);
        let currentOffset = 0; // acc

        for (let i = 0; i < this.totalCount; i++) {
            this.offsets[i] = currentOffset;
            const h = this.heightMap.get(i) || this.baseRowHeight;
            currentOffset += h;
        }

        this.totalHeight = currentOffset;
        this.dirty = false;
    }

    /**
     * 二分查找：找到 top <= offset 的最后一行
     */
    _findIdx(offset) {
        if (this.totalCount === 0) return 0;
        let low = 0, high = this.totalCount - 1, res = 0;

        while (low <= high) {
            const mid = (low + high) >>> 1;
            if (this.offsets[mid] <= offset) {
                res = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return res;
    }

    /**
     * 根据 scrollTop 计算可见行范围
     */
    getRenderRange(scrollTop, buffer = 2) {
        this._calc();
        if (this.totalCount === 0) return { start: 0, end: 0, totalHeight: 0 };

        const start = this._findIdx(scrollTop);
        let end = start;
        let currentH = 0;

        // 向加累加直到填满视口
        while (end < this.totalCount && currentH < this.viewportHeight) {
            currentH += (this.heightMap.get(end) || this.baseRowHeight);
            end++;
        }

        return {
            start: Math.max(0, start - buffer),
            end: Math.min(this.totalCount, end + buffer),
            totalHeight: this.totalHeight
        };
    }

    /**
     * 给定像素区间，反查行索引范围（用于框选）
     */
    getIndicesInRange(top, bottom) {
        this._calc();
        if (this.totalCount === 0) return [0, 0];

        const s = this._findIdx(top);
        let e = this._findIdx(bottom);
        if (e < this.totalCount - 1) e++;

        return [s, Math.min(this.totalCount, e + 1)];
    }

    getRowTop(i) {
        this._calc();
        return this.offsets[i];
    }

    getRowHeight(i) {
        return this.heightMap.get(i) || this.baseRowHeight;
    }
}