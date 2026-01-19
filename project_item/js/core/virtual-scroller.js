/**
 * VirtualScroller 类
 * 负责虚拟滚动的坐标计算。
 */
export class VirtualScroller {
    constructor(config) {
        this.baseRowHeight = config.rowHeight || 40;
        this.headerHeight = config.headerHeight || 0; // 【新增】记录表头高度
        this.totalCount = 0;
        this.viewportHeight = 0;
        this.heightMap = new Map();
        this.offsets = null;
        this.totalHeight = 0;
        this.dirty = true;
    }

    resetHeights() {
        this.heightMap.clear();
        this.offsets = null;
        this.totalHeight = 0;
        this.dirty = true;
    }

    updateMetrics(count, viewportH) {
        this.viewportHeight = viewportH;
        if (this.totalCount !== count) {
            this.totalCount = count;
            this.dirty = true;
            this.offsets = null;
        }
    }

    setRowHeight(idx, h) {
        const currentH = this.heightMap.get(idx) || this.baseRowHeight;
        if (currentH !== h) {
            if (h === this.baseRowHeight) this.heightMap.delete(idx);
            else this.heightMap.set(idx, h);
            this.dirty = true;
            this.offsets = null;
        }
    }

    _calc() {
        if (!this.dirty && this.offsets) return;
        this.offsets = new Int32Array(this.totalCount);
        let acc = 0;
        for (let i = 0; i < this.totalCount; i++) {
            this.offsets[i] = acc;
            acc += (this.heightMap.get(i) || this.baseRowHeight);
        }
        this.totalHeight = acc;
        this.dirty = false;
    }

    /**
     * 找到 top <= offset 的最后一行
     * 注意：这里的 offset 是相对于 content 层的物理偏移（不含 header）
     */
    _findIdx(offset) {
        if (this.totalCount === 0 || offset <= 0) return 0;
        this._calc();
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

    getRenderRange(scrollTop, buffer = 3) {
        this._calc();
        if (this.totalCount === 0) return { start: 0, end: 0, totalHeight: 0 };

        // 【关键】计算内容区的实际可见起始点（需减去 header 占用空间）
        const relativeScrollTop = Math.max(0, scrollTop - this.headerHeight);
        const start = this._findIdx(relativeScrollTop);

        let end = start;
        const viewportBottom = relativeScrollTop + this.viewportHeight;
        while (end < this.totalCount) {
            const h = this.heightMap.get(end) || this.baseRowHeight;
            if (this.offsets[end] + h > viewportBottom) break;
            end++;
        }

        return {
            start: Math.max(0, start - buffer),
            end: Math.min(this.totalCount, end + buffer + 1),
            totalHeight: this.totalHeight + this.headerHeight // 总高度包含 header
        };
    }

    getRowTop(i) {
        this._calc();
        // 【关键】每一行的 Top 值直接加上 headerHeight，使其在 content(top:0) 层中位置正确
        const baseTop = (i >= 0 && i < this.totalCount) ? this.offsets[i] : 0;
        return baseTop + this.headerHeight;
    }

    getRowHeight(i) {
        return this.heightMap.get(i) || this.baseRowHeight;
    }
}