import { DataEngine } from './data-engine.js';
import { VirtualScroller } from './virtual-scroller.js';
import { widgetFactory } from './widget-factory.js';

/**
 * List Class
 * 通用列表控件。
 * 修复：通过 DOM 增量更新防止超长元素滚动复位。
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

        // 核心状态存储
        this.activeWidgets = new Map(); // Map<RowKey, Instance>
        this.domMap = new Map();       // Map<RowKey, HTMLElement> 存储当前在 DOM 中的节点

        // 记录上一次渲染的范围，用于 Diff
        this.prevRange = { start: -1, end: -1 };

        // 尺寸监听器，用于处理高度自适应
        this.resizeObserver = new ResizeObserver((entries) => this.onEntriesResize(entries));

        this.initDOM();
        this.bindEvents();
    }

    init(config, data) {
        const isSoftUpdate = !!this.dataEngine;
        this.config = config;

        if (!this.dataEngine) {
            this.dataEngine = new DataEngine(config);
        }

        const baseHeight = (config.rowHeight === 'auto') ? (config.estimatedRowHeight || 50) : config.rowHeight;

        if (!this.scroller) {
            this.scroller = new VirtualScroller({ rowHeight: baseHeight });
        }

        this.dataEngine.loadData(data);

        if (this.config.template && typeof this.config.template === 'string') {
            try {
                this.renderFn = new Function('row', 'index', 'return `' + this.config.template + '`;');
            } catch (e) {
                console.error("List Template Error:", e);
                this.renderFn = () => "Template Error";
            }
        }

        this.refresh(isSoftUpdate ? false : true);

        if (this.bus) this.bus.emit('TRACK', { event: 'LIST_INIT', payload: { id: this.id, count: data.length, soft: isSoftUpdate } });
    }

    destroy() {
        this.resizeObserver.disconnect();
        this.cleanupWidgets(true);
        this.domMap.clear();
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

    onEntriesResize(entries) {
        let needsUpdate = false;
        for (const entry of entries) {
            const el = entry.target;
            const idx = parseInt(el.dataset.idx);
            if (isNaN(idx)) continue;
            let newHeight = entry.borderBoxSize ? entry.borderBoxSize[0].blockSize : entry.contentRect.height;
            const oldHeight = this.scroller.getRowHeight(idx);
            if (Math.abs(newHeight - oldHeight) > 1) {
                this.scroller.setRowHeight(idx, newHeight);
                needsUpdate = true;
            }
        }
        if (needsUpdate) {
            requestAnimationFrame(() => this.updateLayoutOnly());
        }
    }

    updateLayoutOnly() {
        this.scroller._calc();
        this.els.phantom.style.height = `${this.scroller.totalHeight}px`;
        // 更新所有在场 DOM 的位置
        this.domMap.forEach((el, key) => {
            const idx = parseInt(el.dataset.idx);
            if (!isNaN(idx)) {
                el.style.top = `${this.scroller.getRowTop(idx)}px`;
            }
        });
        // 【关键】高度变化可能导致渲染范围变化，触发一次 refresh
        this.refresh();
    }

    /**
     * 刷新列表视图 (增量 Diff 模式)
     */
    refresh(force = false) {
        const viewportH = this.container.clientHeight || 400;
        this.scroller.updateMetrics(this.dataEngine.count, viewportH);

        const currentScrollTop = this.container.scrollTop;
        const range = this.scroller.getRenderRange(currentScrollTop);

        if (!force && range.start === this.prevRange.start && range.end === this.prevRange.end) {
            return;
        }

        this.prevRange = range;
        this.els.phantom.style.height = `${range.totalHeight}px`;

        const isAutoHeight = this.config.rowHeight === 'auto';
        const overflowSetting = this.config.overflow || 'hidden';
        const wrapperStyle = `width:100%; height:100%; overflow:${overflowSetting};`;

        // 1. 计算当前应该存在的 Keys
        const visibleKeys = new Set();
        for (let i = range.start; i < range.end; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            const key = this.dataEngine.getCompositeKey(rowData);
            visibleKeys.add(key);

            // 2. 如果节点不在 DOM 中，创建并添加
            if (!this.domMap.has(key)) {
                const top = this.scroller.getRowTop(i);
                const itemEl = document.createElement('div');
                itemEl.className = 'list-item';
                itemEl.dataset.key = key;
                itemEl.dataset.idx = i;

                let style = `top:${top}px;`;
                if (!isAutoHeight) {
                    style += `height:${this.scroller.getRowHeight(i)}px;`;
                }
                itemEl.style.cssText = style;

                // 渲染内容
                if (this.config.itemWidget) {
                    const innerStyle = isAutoHeight ? `width:100%; overflow:${overflowSetting};` : wrapperStyle;
                    itemEl.innerHTML = `<div class="list-item-widget-wrapper" id="list-widget-${key}" style="${innerStyle}"></div>`;
                    setTimeout(() => this.mountItemWidget(key, rowData), 0);
                } else if (this.renderFn) {
                    const innerStyle = isAutoHeight ? `width:100%; overflow:${overflowSetting};` : wrapperStyle;
                    let html = `<div class="list-item-content-wrapper" style="${innerStyle}">`;
                    html += this.renderFn(rowData, i);
                    html += `</div>`;
                    itemEl.innerHTML = html;
                } else {
                    itemEl.innerHTML = `<span>${JSON.stringify(rowData)}</span>`;
                }

                this.els.content.appendChild(itemEl);
                this.domMap.set(key, itemEl);
                if (isAutoHeight) this.resizeObserver.observe(itemEl);
            } else {
                // 3. 节点已存在，仅更新位置
                const itemEl = this.domMap.get(key);
                itemEl.style.top = `${this.scroller.getRowTop(i)}px`;
                itemEl.dataset.idx = i;
            }
        }

        // 4. 移除不在范围内的 Keys
        this.domMap.forEach((el, key) => {
            if (!visibleKeys.has(key)) {
                const widget = this.activeWidgets.get(key);
                if (widget && widget.destroy) widget.destroy();
                this.activeWidgets.delete(key);
                this.resizeObserver.unobserve(el);
                el.remove();
                this.domMap.delete(key);
            }
        });
    }

    mountItemWidget(key, rowData) {
        const container = this.container.querySelector(`#list-widget-${key}`);
        if (!container || this.activeWidgets.has(key)) return;
        const childId = `${this.id}:item-${key}`;
        const instance = widgetFactory.create(container, this.config.itemWidget, rowData, this.config.globalContext, this.bus, childId);
        if (instance) this.activeWidgets.set(key, instance);
    }

    cleanupWidgets(all) {
        if (all) {
            this.activeWidgets.forEach(w => w.destroy && w.destroy());
            this.activeWidgets.clear();
            this.domMap.forEach(el => el.remove());
            this.domMap.clear();
        }
    }

    bindEvents() {
        this.container.addEventListener('scroll', () => {
            this.scrollTop = this.container.scrollTop;
            requestAnimationFrame(() => this.refresh());
        }, { passive: true });

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