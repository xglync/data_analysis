import { DataEngine } from './data-engine.js';
import { VirtualScroller } from './virtual-scroller.js';
import { widgetFactory } from './widget-factory.js';

export class ListWidget {
    constructor(container, messageBus, id) {
        this.container = container;
        this.bus = messageBus;
        this.id = id;
        this.config = {};
        this.dataEngine = null;
        this.scroller = null;
        this.activeWidgets = new Map();
        this.domMap = new Map();
        this.prevRange = { start: -1, end: -1 };

        this.itemResizeObserver = new ResizeObserver((entries) => this.onItemsResize(entries));
        this.containerObserver = new ResizeObserver(() => {
            if (this.scroller && this.container.clientHeight > 0) this.refresh();
        });

        this.initDOM();
        this.bindEvents();
    }

    init(config, data) {
        const isSoft = !!this.dataEngine;
        this.config = config;
        if (!this.dataEngine) this.dataEngine = new DataEngine(config);

        // List 通常没有 Header，headerHeight 传 0
        const baseH = (config.rowHeight === 'auto') ? (config.estimatedRowHeight || 50) : config.rowHeight;
        if (!this.scroller) {
            this.scroller = new VirtualScroller({ rowHeight: baseH, headerHeight: 0 });
        } else if (!isSoft) {
            this.scroller.resetHeights();
        }

        this.dataEngine.loadData(data);
        if (!isSoft) {
            this.prevRange = { start: -1, end: -1 };
            this.container.scrollTop = 0;
        }
        this.refresh(true);
        this.containerObserver.observe(this.container);
    }

    initDOM() {
        this.container.classList.add('vl-root');
        this.container.style.overflow = 'auto';

        // 核心：强制内部绝对定位，不依赖容器的 position 属性。
        // 但为了让 absolute 子元素工作，容器至少要是 relative/absolute。
        if (window.getComputedStyle(this.container).position === 'static') {
            this.container.style.position = 'relative';
        }

        this.container.innerHTML = `
            <div class="vl-phantom" style="top:0 !important; left:0 !important; position:absolute;"></div>
            <div class="vl-content" style="top:0 !important; left:0 !important; position:absolute; width:100%;"></div>
        `;
        this.els = {
            phantom: this.container.querySelector('.vl-phantom'),
            content: this.container.querySelector('.vl-content')
        };
    }

    onItemsResize(entries) {
        let update = false;
        for (const entry of entries) {
            const idx = parseInt(entry.target.dataset.idx);
            if (isNaN(idx)) continue;
            let h = entry.borderBoxSize ? entry.borderBoxSize[0].blockSize : entry.contentRect.height;
            if (Math.abs(h - this.scroller.getRowHeight(idx)) > 1) {
                this.scroller.setRowHeight(idx, h);
                update = true;
            }
        }
        if (update) requestAnimationFrame(() => this.updateLayoutOnly());
    }

    updateLayoutOnly() {
        this.scroller._calc();
        const h = `${this.scroller.totalHeight}px`;
        this.els.phantom.style.height = h;
        this.els.content.style.height = h;
        this.domMap.forEach((el, key) => {
            const idx = parseInt(el.dataset.idx);
            if (!isNaN(idx)) el.style.top = `${this.scroller.getRowTop(idx)}px`;
        });
    }

    refresh(force = false) {
        const viewportH = this.container.clientHeight || this.config._forcedHeight || 400;
        this.scroller.updateMetrics(this.dataEngine.count, viewportH);
        const range = this.scroller.getRenderRange(this.container.scrollTop);

        if (!force && range.start === this.prevRange.start && range.end === this.prevRange.end) return;
        this.prevRange = range;

        const h = `${range.totalHeight}px`;
        this.els.phantom.style.height = h;
        this.els.content.style.height = h;

        const visibleKeys = new Set();
        for (let i = range.start; i < range.end; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            if (!rowData) continue;
            const key = this.dataEngine.getCompositeKey(rowData);
            visibleKeys.add(key);

            const top = this.scroller.getRowTop(i);

            if (!this.domMap.has(key)) {
                const itemEl = document.createElement('div');
                itemEl.className = 'vl-item';
                itemEl.dataset.key = key;
                itemEl.dataset.idx = i;
                itemEl.style.top = `${top}px`;
                if (this.config.rowHeight !== 'auto') itemEl.style.height = `${this.scroller.getRowHeight(i)}px`;

                if (this.config.template) {
                    const fn = new Function('row', 'return `' + this.config.template + '`;');
                    itemEl.innerHTML = fn(rowData);
                } else if (this.config.itemWidget) {
                    itemEl.innerHTML = `<div class="vl-widget-wrapper" id="lw-${key}"></div>`;
                    setTimeout(() => this.mountItemWidget(key, rowData), 0);
                }

                this.els.content.appendChild(itemEl);
                this.domMap.set(key, itemEl);
                if (this.config.rowHeight === 'auto') this.itemResizeObserver.observe(itemEl);
            } else {
                const el = this.domMap.get(key);
                el.style.top = `${top}px`;
                el.dataset.idx = i;
            }
        }

        this.domMap.forEach((el, key) => {
            if (!visibleKeys.has(key)) {
                const w = this.activeWidgets.get(key);
                if (w && w.destroy) w.destroy();
                this.activeWidgets.delete(key);
                this.itemResizeObserver.unobserve(el);
                el.remove();
                this.domMap.delete(key);
            }
        });
    }

    mountItemWidget(key, rowData) {
        const container = this.container.querySelector(`#lw-${key}`);
        if (!container || this.activeWidgets.has(key)) return;
        const inst = widgetFactory.create(container, this.config.itemWidget, rowData, this.config.globalContext, this.bus, `${this.id}:i-${key}`);
        if (inst) this.activeWidgets.set(key, instance);
    }

    bindEvents() {
        this.container.addEventListener('scroll', () => {
            requestAnimationFrame(() => this.refresh());
        }, { passive: true });
    }

    cleanupWidgets(all) {
        if (all) {
            this.activeWidgets.forEach(w => w.destroy && w.destroy());
            this.activeWidgets.clear();
            this.domMap.forEach(el => el.remove());
            this.domMap.clear();
        }
    }

    destroy() {
        this.itemResizeObserver.disconnect();
        this.containerObserver.disconnect();
        this.cleanupWidgets(true);
        this.container.innerHTML = '';
    }
}