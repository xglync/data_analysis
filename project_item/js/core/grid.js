import { DataEngine } from './data-engine.js';
import { VirtualScroller } from './virtual-scroller.js';
import { FormatterManager } from './formatter-mgr.js';
import { widgetFactory } from './widget-factory.js';

/**
 * Grid Class
 * 核心表格组件。
 * 修复：编辑不销毁子组件、恢复右键菜单、完善增量渲染。
 */
export class Grid {
    constructor(containerOrId, messageBus, id = 'root') {
        this.container = typeof containerOrId === 'string'
            ? document.getElementById(containerOrId)
            : containerOrId;

        this.container.classList.add('vg-root');
        this.bus = messageBus;
        this.id = id;
        this.config = {};
        this.dataEngine = null;
        this.scroller = null;
        this.formatterMgr = new FormatterManager();

        this.selectedKeys = new Set();
        this.expandedKeys = new Set();
        this.activeWidgets = new Map();
        this.scrollTop = 0;

        this.rowDomMap = new Map();
        this.subDomMap = new Map();
        this.prevRange = { start: -1, end: -1 };

        this.initDOM();
        this.bindEvents();
    }

    init(config, data) {
        const isConfigChanged = JSON.stringify(this.config.columns) !== JSON.stringify(config.columns);
        this.config = config;
        this.applyStyles(config.style);

        if (!this.dataEngine) this.dataEngine = new DataEngine(config);

        const hHeight = this.config.hideHeader ? 0 : 40;
        if (!this.scroller) {
            this.scroller = new VirtualScroller({
                rowHeight: config.rowHeight || 40,
                headerHeight: hHeight
            });
        } else {
            this.scroller.headerHeight = hHeight;
        }

        this.dataEngine.loadData(data);

        const headerPx = this.config.hideHeader ? '0px' : '40px';
        this.els.content.style.top = '0px'; // content 层始终对齐 0，由 scroller 计算物理 top
        this.els.phantom.style.top = '0px';

        if (isConfigChanged) this.renderHeader();

        this.initDragSelect();
        this.syncExpandedRows();
        this.refresh(true);
    }

    applyStyles(styleConfig) {
        if (!styleConfig) return;
        const s = this.container.style;
        if (styleConfig.borderColor) s.setProperty('--grid-border-color', styleConfig.borderColor);
        if (styleConfig.headerBg) s.setProperty('--grid-header-bg', styleConfig.headerBg);
        if (styleConfig.indentWidth) s.setProperty('--indent-width', styleConfig.indentWidth);
    }

    initDOM() {
        this.container.style.overflow = 'auto';
        if (window.getComputedStyle(this.container).position === 'static') {
            this.container.style.position = 'relative';
        }

        this.container.innerHTML = `
            <div class="vg-header"></div>
            <div class="vg-phantom"></div>
            <div class="vg-content"></div>
            <div class="selection-box"></div>
            <div class="context-menu"></div>
        `;
        const q = (s) => this.container.querySelector(s);
        this.els = {
            header: q('.vg-header'),
            phantom: q('.vg-phantom'),
            content: q('.vg-content'),
            selection: q('.selection-box'),
            menu: q('.context-menu')
        };
    }

    calcTotalWidth() {
        let w = 0;
        if (this.config.subWidget) w += 40;
        this.config.columns.forEach(c => w += (c.width || 100));
        return w;
    }

    renderHeader() {
        if (this.config.hideHeader) { this.els.header.style.display = 'none'; return; }
        this.els.header.style.display = 'flex';
        let html = '';
        if (this.config.subWidget) {
            html += `<div class="vg-header-cell no-sort" style="width:40px; min-width:40px; max-width:40px;"></div>`;
        }
        this.config.columns.forEach(col => {
            const w = col.width || 100;
            html += `<div class="vg-header-cell" style="width:${w}px; min-width:${w}px; max-width:${w}px;" data-field="${col.field}">
                ${col.title} ${this.config.sortable !== false ? '<small>⇅</small>' : ''}
            </div>`;
        });
        this.els.header.innerHTML = html;
        this.els.header.style.minWidth = `${this.calcTotalWidth()}px`;
    }

    syncExpandedRows() {
        this.scroller.resetHeights();
        if (this.expandedKeys.size === 0) return;
        const widgetH = (this.config.subWidget && this.config.subWidget.height) || 200;
        const baseH = this.config.rowHeight || 40;
        const expandedH = baseH + widgetH;
        for (let i = 0; i < this.dataEngine.count; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            if (this.expandedKeys.has(this.dataEngine.getCompositeKey(rowData))) {
                this.scroller.setRowHeight(i, expandedH);
            }
        }
    }

    refresh(force = false) {
        const viewportH = this.container.clientHeight || this.config._forcedHeight || 500;
        this.scroller.updateMetrics(this.dataEngine.count, viewportH);
        const range = this.scroller.getRenderRange(this.container.scrollTop);

        const totalH = `${range.totalHeight}px`;
        this.els.phantom.style.height = totalH;
        this.els.content.style.height = totalH;

        const totalW = this.calcTotalWidth();
        const baseH = this.config.rowHeight || 40;

        const visibleKeys = new Set();
        for (let i = range.start; i < range.end; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            if (!rowData) continue;
            const key = this.dataEngine.getCompositeKey(rowData);
            visibleKeys.add(key);

            const isSelected = this.selectedKeys.has(key);
            const isExpanded = this.expandedKeys.has(key);
            const top = this.scroller.getRowTop(i);

            // A. 处理 Row DOM
            if (!this.rowDomMap.has(key) || force) {
                if (force && this.rowDomMap.has(key)) this.rowDomMap.get(key).remove();
                const rowEl = document.createElement('div');
                rowEl.className = `vg-row ${isSelected ? 'selected' : ''} ${isExpanded ? 'expanded' : ''}`;
                rowEl.dataset.key = key;
                rowEl.style.cssText = `top:${top}px !important; height:${baseH}px; min-width:${totalW}px; left:0; margin:0; position:absolute;`;

                let rowHtml = '';
                if (this.config.subWidget) {
                    rowHtml += `<div class="vg-cell expand-btn" style="width:40px;">${isExpanded ? '-' : '+'}</div>`;
                }
                this.config.columns.forEach(col => {
                    let val = rowData[col.field];
                    if (col.formatter) {
                        const fmt = this.formatterMgr.get(col.formatter);
                        if (fmt) val = fmt(val, rowData);
                    }
                    const w = col.width || 100;
                    rowHtml += `<div class="vg-cell" style="width:${w}px; min-width:${w}px; max-width:${w}px;" data-field="${col.field}">${val}</div>`;
                });
                rowEl.innerHTML = rowHtml;
                this.els.content.appendChild(rowEl);
                this.rowDomMap.set(key, rowEl);
            } else {
                const rowEl = this.rowDomMap.get(key);
                rowEl.style.top = `${top}px`;
                rowEl.classList.toggle('selected', isSelected);
                rowEl.classList.toggle('expanded', isExpanded);
                const btn = rowEl.querySelector('.expand-btn');
                if (btn) btn.innerText = isExpanded ? '-' : '+';

                // 【核心修复】增量更新模式下，非强制刷新也要同步单元格内容，防止编辑后显示不更新
                this.config.columns.forEach(col => {
                    const cell = rowEl.querySelector(`.vg-cell[data-field="${col.field}"]`);
                    if (cell && !cell.classList.contains('editing')) {
                        let val = rowData[col.field];
                        if (col.formatter) {
                            const fmt = this.formatterMgr.get(col.formatter);
                            if (fmt) val = fmt(val, rowData);
                        }
                        cell.innerHTML = val;
                    }
                });
            }

            // B. 处理 SubWidget DOM
            if (isExpanded) {
                const subTop = top + baseH;
                const subHeight = this.scroller.getRowHeight(i) - baseH;
                if (!this.subDomMap.has(key) || force) {
                    if (force && this.subDomMap.has(key)) this.subDomMap.get(key).remove();
                    const subEl = document.createElement('div');
                    subEl.className = 'widget-container';
                    subEl.id = `grid-widget-${key}`;
                    subEl.style.cssText = `top:${subTop}px !important; height:${subHeight}px; min-width:${totalW}px; left:0 !important; margin:0 !important; padding:0 !important; z-index:3; position:absolute !important; box-sizing:border-box !important; border-left:var(--indent-width, 50px) solid #fff !important; overflow:hidden;`;
                    this.els.content.appendChild(subEl);
                    this.subDomMap.set(key, subEl);
                    setTimeout(() => this.mountChild(key, rowData, subHeight), 0);
                } else {
                    const subEl = this.subDomMap.get(key);
                    subEl.style.top = `${subTop}px`;
                    subEl.style.height = `${subHeight}px`;
                }
            } else {
                this.removeSubWidget(key);
            }
        }

        this.rowDomMap.forEach((el, key) => {
            if (!visibleKeys.has(key)) {
                el.remove();
                this.rowDomMap.delete(key);
                this.removeSubWidget(key);
            }
        });
        this.prevRange = range;
    }

    removeSubWidget(key) {
        if (this.subDomMap.has(key)) { this.subDomMap.get(key).remove(); this.subDomMap.delete(key); }
        const w = this.activeWidgets.get(key);
        if (w) { if (w.destroy) w.destroy(); this.activeWidgets.delete(key); }
    }

    mountChild(key, rowData, subHeight) {
        const container = this.subDomMap.get(key);
        if (!container || this.activeWidgets.has(key)) return;
        const mergedConfig = {
            ...this.config.subWidget,
            config: { ...this.config.subWidget.config, _forcedHeight: subHeight }
        };
        const inst = widgetFactory.create(container, mergedConfig, rowData, this.config.globalContext, this.bus, `${this.id}:row-${key}:sub`);
        if (inst) this.activeWidgets.set(key, inst);
    }

    cleanupWidgets(all) {
        if (all) {
            this.activeWidgets.forEach(w => w.destroy && w.destroy());
            this.activeWidgets.clear();
            this.rowDomMap.forEach(el => el.remove()); this.rowDomMap.clear();
            this.subDomMap.forEach(el => el.remove()); this.subDomMap.clear();
            this.prevRange = { start: -1, end: -1 };
        }
    }

    isChildWidgetEvent(e) {
        const w = e.target.closest('.widget-container');
        return w && this.els.content.contains(w);
    }

    bindEvents() {
        this.container.addEventListener('scroll', () => {
            requestAnimationFrame(() => { this.scrollTop = this.container.scrollTop; this.refresh(); });
        }, { passive: true });

        this.els.content.addEventListener('click', (e) => {
            if (this.isChildWidgetEvent(e)) return;
            const btn = e.target.closest('.expand-btn');
            const row = e.target.closest('.vg-row');
            if (btn && row) {
                e.stopPropagation();
                this.toggleExpand(row.dataset.key, parseInt(row.dataset.idx));
                return;
            }
            if (row && this.config.selection) {
                const key = row.dataset.key;
                if (e.ctrlKey) { if (this.selectedKeys.has(key)) this.selectedKeys.delete(key); else this.selectedKeys.add(key); }
                else { this.selectedKeys.clear(); this.selectedKeys.add(key); }
                this.refresh();
                this.emit('SELECTION_CHANGED', Array.from(this.selectedKeys));
            }
        });

        // 【恢复】双击编辑事件
        this.els.content.addEventListener('dblclick', (e) => {
            if (this.isChildWidgetEvent(e)) return;
            const cell = e.target.closest('.vg-cell');
            if (!cell || cell.classList.contains('editing')) return;
            const field = cell.dataset.field;
            const col = this.config.columns.find(c => c.field === field);
            if (col && col.editable) this.startEdit(cell, field, col);
        });

        // 【恢复】右键菜单事件
        this.container.addEventListener('contextmenu', (e) => {
            if (this.isChildWidgetEvent(e) || e.target.closest('.vg-header')) return;
            e.preventDefault();
            const row = e.target.closest('.vg-row');
            if (row) {
                const key = row.dataset.key;
                if (!this.selectedKeys.has(key)) {
                    this.selectedKeys.clear();
                    this.selectedKeys.add(key);
                    this.refresh();
                    this.emit('SELECTION_CHANGED', Array.from(this.selectedKeys));
                }
                this.showContextMenu(e.clientX, e.clientY);
            }
        });

        this.els.header.addEventListener('click', (e) => {
            const th = e.target.closest('.vg-header-cell');
            if (th && th.dataset.field) {
                this.dataEngine.sort(th.dataset.field, 'asc');
                this.syncExpandedRows();
                this.refresh(true);
            }
        });
        document.addEventListener('click', () => this.els.menu.style.display = 'none');
    }

    toggleExpand(key, idx) {
        if (this.expandedKeys.has(key)) this.expandedKeys.delete(key);
        else this.expandedKeys.add(key);
        this.syncExpandedRows();
        this.refresh();
    }

    startEdit(cell, field, col) {
        const row = cell.closest('.vg-row');
        const key = row.dataset.key;
        const idx = this.dataEngine.idMap.get(key);
        const oldVal = this.dataEngine.rawData[idx][field];
        cell.classList.add('editing');
        const cfg = col.editor || { type: 'text' };

        let el;
        if (cfg.type === 'textarea') { el = document.createElement('textarea'); }
        else { el = document.createElement('input'); el.type = cfg.type === 'date' ? 'date' : 'text'; }

        el.value = oldVal || '';
        cell.innerHTML = ''; cell.appendChild(el); el.focus();

        const close = (cancel = false) => {
            if (!cell.classList.contains('editing')) return;
            const val = cancel ? oldVal : el.value;
            cell.classList.remove('editing');
            if (!cancel && val !== oldVal) {
                this.dataEngine.rawData[idx][field] = val;
                this.emit('DATA_UPDATED', { key, field, value: val });
            }
            // 【核心修复】编辑结束调用平滑刷新，保留子组件状态
            this.refresh();
        };

        el.onblur = () => close();
        el.onkeydown = (e) => {
            if (e.key === 'Enter' && cfg.type !== 'textarea') close();
            if (e.key === 'Escape') close(true);
        };
        el.onclick = (e) => e.stopPropagation();
    }

    showContextMenu(x, y) {
        if (!this.config.contextMenu) return;
        let html = '';
        this.config.contextMenu.forEach(item => {
            html += `<div class="context-menu-item" data-action="${item.action}">${item.label}</div>`;
        });
        this.els.menu.innerHTML = html;

        // 边界处理
        const menuWidth = 150;
        const left = (x + menuWidth > window.innerWidth) ? x - menuWidth : x;
        this.els.menu.style.left = `${left}px`;
        this.els.menu.style.top = `${y}px`;
        this.els.menu.style.display = 'block';

        this.els.menu.onclick = (e) => {
            const action = e.target.dataset.action;
            if (action) this.emit('MENU_ACTION', { action, selection: Array.from(this.selectedKeys) });
        };
    }

    initDragSelect() {
        if (!this.config.dragSelect) return;
        let start = null, isDrag = false;
        const onDown = (e) => {
            if (e.button !== 0 || e.target.closest('.vg-header') || this.isChildWidgetEvent(e)) return;
            start = { x: e.clientX, y: e.clientY, sl: this.container.scrollLeft, st: this.container.scrollTop };
        };
        const onMove = (e) => {
            if (!start) return;
            if (!isDrag && (Math.abs(e.clientX - start.x) > 5 || Math.abs(e.clientY - start.y) > 5)) {
                isDrag = true; this.els.selection.style.display = 'block';
            }
            if (!isDrag) return;
            const rect = this.container.getBoundingClientRect();
            const sx = start.x - rect.left - this.container.clientLeft + start.sl;
            const sy = start.y - rect.top - this.container.clientTop + start.st;
            const cx = e.clientX - rect.left - this.container.clientLeft + this.container.scrollLeft;
            const cy = e.clientY - rect.top - this.container.clientTop + this.container.scrollTop;
            const l = Math.min(sx, cx), t = Math.min(sy, cy), w = Math.abs(cx - sx), h = Math.abs(cy - sy);
            this.els.selection.style.left = l + 'px'; this.els.selection.style.top = t + 'px';
            this.els.selection.style.width = w + 'px'; this.els.selection.style.height = h + 'px';

            const headerOffset = this.config.hideHeader ? 0 : 40;
            const [s, end] = this.scroller.getIndicesInRange(t - headerOffset, t + h - headerOffset);
            const newSel = new Set(e.ctrlKey ? this.selectedKeys : []);
            for (let i = s; i < end; i++) {
                const rt = this.scroller.getRowTop(i);
                if (rt < t + h && rt + (this.config.rowHeight || 40) > t) {
                    newSel.add(this.dataEngine.getCompositeKey(this.dataEngine.getRowByIndex(i)));
                }
            }
            this.selectedKeys = newSel; this.refresh();
        };
        const onUp = () => {
            if (isDrag) this.emit('SELECTION_CHANGED', Array.from(this.selectedKeys));
            start = null; isDrag = false; this.els.selection.style.display = 'none';
        };
        this.container.addEventListener('mousedown', onDown);
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    }

    destroy() {
        this.cleanupWidgets(true);
        this.container.innerHTML = '';
    }

    emit(type, payload) { if (this.bus) this.bus.emit(type, { ...payload, _widgetId: this.id }); }
}