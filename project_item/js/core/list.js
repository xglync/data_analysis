import { DataEngine } from './data-engine.js';
import { VirtualScroller } from './virtual-scroller.js';
import { widgetFactory } from './widget-factory.js';

/**
 * List Class
 * 通用列表控件。
 * 特性：
 * 1. 虚拟滚动：支持海量数据列表。
 * 2. 双模式渲染：Template 模式 / Widget 模式。
 * 3. 支持 overflow 内部滚动配置。
 * 4. 支持 rowHeight: 'auto'，配合 ResizeObserver 实现高度自适应。
 * 5. [优化] 滚动时仅在渲染范围改变时重绘 DOM，防止编辑器失去焦点。
 */
export class ListWidget {
    /**
     * @param {HTMLElement} container
     * @param {MessageBus} messageBus
     * @param {string} id
     */
    constructor(container, messageBus, id) {
        this.container = container;
        this.bus = messageBus;
        this.id = id;
        this.config = {};

        this.dataEngine = null;
        this.scroller = null;
        this.scrollTop = 0;
        this.activeWidgets = new Map(); // Map<RowKey, Instance>

        // 记录上一次渲染的范围，用于 Diff
        this.prevRange = { start: -1, end: -1 };

        // 尺寸监听器，用于处理高度自适应
        this.resizeObserver = new ResizeObserver((entries) => this.onEntriesResize(entries));

        this.initDOM();
        this.bindEvents();
    }

    init(config, data) {
        this.config = config;
        this.dataEngine = new DataEngine(config);

        // 如果配置是 'auto'，给 Scroller 一个预估高度
        const baseHeight = (config.rowHeight === 'auto') ? (config.estimatedRowHeight || 50) : config.rowHeight;

        this.scroller = new VirtualScroller({ rowHeight: baseHeight });

        this.dataEngine.loadData(data);

        if (this.config.template && typeof this.config.template === 'string') {
            try {
                this.renderFn = new Function('row', 'index', 'return `' + this.config.template + '`;');
            } catch (e) {
                console.error("List Template Error:", e);
                this.renderFn = () => "Template Error";
            }
        }

        // 初始化时强制刷新
        this.refresh(true);

        if (this.bus) this.bus.emit('TRACK', { event: 'LIST_INIT', payload: { id: this.id, count: data.length } });
    }

    destroy() {
        this.resizeObserver.disconnect();
        this.cleanupWidgets(true);
        this.container.innerHTML = '';
    }

    initDOM() {
        this.container.classList.add('list-container');
        this.container.innerHTML = `
            <div class="list-phantom"></div>
            <div class="list-content"></div>
        `;
        this.els = {
            phantom: this.container.querySelector('.list-phantom'),
            content: this.container.querySelector('.list-content')
        };
    }

    // 处理高度变化
    onEntriesResize(entries) {
        let needsUpdate = false;

        for (const entry of entries) {
            const el = entry.target;
            const idx = parseInt(el.dataset.idx);

            // 获取新的高度 (border-box)
            let newHeight = entry.borderBoxSize ? entry.borderBoxSize[0].blockSize : entry.contentRect.height;

            // 获取 Scroller 记录的旧高度
            const oldHeight = this.scroller.getRowHeight(idx);

            // 如果高度发生实质变化
            if (Math.abs(newHeight - oldHeight) > 1) {
                this.scroller.setRowHeight(idx, newHeight);
                needsUpdate = true;
            }
        }

        // 如果有高度变化，仅更新布局位置，不重绘 DOM
        if (needsUpdate) {
            this.updateLayoutOnly();
        }
    }

    // 仅更新布局位置，不破坏 DOM
    updateLayoutOnly() {
        // 1. 重新计算所有偏移量
        this.scroller._calc();

        // 2. 更新 Phantom 高度
        this.els.phantom.style.height = `${this.scroller.totalHeight}px`;

        // 3. 遍历当前 DOM 节点，更新 top
        const items = this.els.content.children;
        for (let el of items) {
            const idx = parseInt(el.dataset.idx);
            if (!isNaN(idx)) {
                const newTop = this.scroller.getRowTop(idx);
                el.style.top = `${newTop}px`;
            }
        }
    }

    /**
     * 刷新列表视图
     * @param {boolean} force 是否强制重绘
     */
    refresh(force = false) {
        const viewportH = this.container.clientHeight || 400;
        this.scroller.updateMetrics(this.dataEngine.count, viewportH);
        const range = this.scroller.getRenderRange(this.scrollTop);

        // 【关键修复】脏检查
        // 如果非强制刷新，且渲染范围没有变化，直接跳过 DOM 重构
        // 这保证了在长行内部滚动时，编辑器不会被销毁重建
        if (!force && range.start === this.prevRange.start && range.end === this.prevRange.end) {
            return;
        }

        // 更新缓存
        this.prevRange = range;

        this.els.phantom.style.height = `${range.totalHeight}px`;

        this.cleanupWidgets(true);
        // 渲染前断开观察
        this.resizeObserver.disconnect();

        const overflowSetting = this.config.overflow || 'hidden';
        const wrapperStyle = `width:100%; height:100%; overflow:${overflowSetting};`;
        const isAutoHeight = this.config.rowHeight === 'auto';

        let html = '';
        for (let i = range.start; i < range.end; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            const key = this.dataEngine.getCompositeKey(rowData);
            const top = this.scroller.getRowTop(i);

            let style = `top:${top}px;`;
            if (!isAutoHeight) {
                const height = this.scroller.getRowHeight(i);
                style += `height:${height}px;`;
            }

            html += `<div class="list-item" style="${style}" data-key="${key}" data-idx="${i}">`;

            if (this.config.itemWidget) {
                const innerStyle = isAutoHeight
                    ? `width:100%; overflow:${overflowSetting};`
                    : wrapperStyle;

                html += `<div class="list-item-widget-wrapper" id="list-widget-${key}" style="${innerStyle}"></div>`;
                setTimeout(() => this.mountItemWidget(key, rowData), 0);
            }
            else if (this.renderFn) {
                const innerStyle = isAutoHeight
                    ? `width:100%; overflow:${overflowSetting};`
                    : wrapperStyle;
                html += `<div class="list-item-content-wrapper" style="${innerStyle}">`;
                html += this.renderFn(rowData, i);
                html += `</div>`;
            }
            else {
                html += `<span>${JSON.stringify(rowData)}</span>`;
            }

            html += `</div>`;
        }

        this.els.content.innerHTML = html;

        // 重新观察新节点
        if (isAutoHeight) {
            const items = this.els.content.children;
            for (let el of items) {
                this.resizeObserver.observe(el);
            }
        }
    }

    mountItemWidget(key, rowData) {
        const container = this.container.querySelector(`#list-widget-${key}`);
        if (!container || this.activeWidgets.has(key)) return;

        const childId = `${this.id}:item-${key}`;

        const instance = widgetFactory.create(
            container,
            this.config.itemWidget,
            rowData,
            this.config.globalContext,
            this.bus,
            childId
        );

        if (instance) {
            this.activeWidgets.set(key, instance);
        }
    }

    cleanupWidgets(all) {
        if (all) {
            this.activeWidgets.forEach(w => w.destroy && w.destroy());
            this.activeWidgets.clear();
        }
    }

    bindEvents() {
        this.container.addEventListener('scroll', () => {
            requestAnimationFrame(() => {
                this.scrollTop = this.container.scrollTop;
                // 滚动时调用 refresh，但在 refresh 内部会进行 range 检查
                // 从而避免无效的 DOM 重绘
                this.refresh();
            });
        });

        this.els.content.addEventListener('click', (e) => {
            const item = e.target.closest('.list-item');
            if (!item) return;

            const idx = parseInt(item.dataset.idx);
            const rowData = this.dataEngine.getRowByIndex(idx);

            if (this.bus) {
                this.bus.emit('LIST_ITEM_CLICK', {
                    id: this.id,
                    key: item.dataset.key,
                    data: rowData
                });
            }
        });
    }
}