import { DataEngine } from './data-engine.js';
import { VirtualScroller } from './virtual-scroller.js';
import { FormatterManager } from './formatter-mgr.js';
import { widgetFactory } from './widget-factory.js';

/**
 * Grid Class
 * 核心表格组件。
 */
export class Grid {
    /**
     * @param {string|HTMLElement} containerOrId 
     * @param {MessageBus} messageBus 
     * @param {string} id 控件唯一ID
     */
    constructor(containerOrId, messageBus, id = 'root') {
        this.container = typeof containerOrId === 'string'
            ? document.getElementById(containerOrId)
            : containerOrId;

        this.bus = messageBus;
        this.id = id; // 保存控件 ID
        this.config = {};

        this.dataEngine = null;
        this.scroller = null;
        this.formatterMgr = new FormatterManager();

        this.selectedKeys = new Set();
        this.expandedKeys = new Set();
        this.activeWidgets = new Map();
        this.scrollTop = 0;
        this.isDragSelecting = false;
        this._globalHandlers = [];

        this.initDOM();
        this.bindEvents();
    }

    init(config, data) {
        // 检查配置是否有重大变化（列定义、主键等）
        const isConfigChanged = JSON.stringify(this.config.columns) !== JSON.stringify(config.columns);
        this.config = config;

        // 应用自定义样式
        this.applyStyles(config.style);

        // 仅在首次或核心配置改变时初始化引擎
        if (!this.dataEngine) {
            this.dataEngine = new DataEngine(config);
        }

        if (!this.scroller) {
            this.scroller = new VirtualScroller({ rowHeight: config.rowHeight || 40 });
        }

        if (config.customFormatters) {
            config.customFormatters.forEach(cf => {
                if (cf.type === 'code') this.formatterMgr.registerFromCode(cf.name, cf.body);
                else if (cf.type === 'url') this.formatterMgr.loadFromScript(cf.url, cf.callbackName);
            });
        }

        // 载入数据：DataEngine 内部会处理索引更新
        this.dataEngine.loadData(data);

        // 如果列配置变了，重新渲染表头
        if (isConfigChanged) {
            this.renderHeader();
        }

        this.initDragSelect();
        this.syncExpandedRows();

        // 刷新视图：refresh 内部会处理虚拟滚动的 metrics 更新
        // 这里强制重绘一次以确保数据同步
        this.refresh(true);

        // [埋点] 
        this.emit('TRACK', { event: 'GRID_INIT', payload: { rowCount: data.length, soft: true } });
    }

    applyStyles(styleConfig) {
        if (!styleConfig) return;
        const s = this.container.style;
        if (styleConfig.borderColor) s.setProperty('--grid-border-color', styleConfig.borderColor);
        if (styleConfig.borderWidth) s.setProperty('--grid-border-width', styleConfig.borderWidth);
        if (styleConfig.headerBg) s.setProperty('--grid-header-bg', styleConfig.headerBg);
        if (styleConfig.indentWidth) s.setProperty('--indent-width', styleConfig.indentWidth);
    }

    destroy() {
        this.cleanupWidgets(true);
        this._globalHandlers.forEach(fn => fn());
        this.container.innerHTML = '';
    }

    initDOM() {
        this.container.innerHTML = `
            <div class="grid-header"></div>
            <div class="grid-phantom"></div>
            <div class="grid-content"></div>
            <div class="selection-box"></div>
            <div class="context-menu"></div>
        `;
        const q = (s) => this.container.querySelector(s);
        this.els = {
            header: q('.grid-header'),
            phantom: q('.grid-phantom'),
            content: q('.grid-content'),
            selection: q('.selection-box'),
            menu: q('.context-menu')
        };
    }

    // 计算总列宽，用于强制对齐表头和内容
    calcTotalWidth() {
        let w = 0;
        if (this.config.subWidget) w += 40; // 展开按钮列宽
        this.config.columns.forEach(c => w += (c.width || 100));
        return w;
    }

    renderHeader() {
        if (this.config.hideHeader) {
            this.els.header.style.display = 'none';
            return;
        }
        this.els.header.style.display = 'flex';
        let html = '';
        if (this.config.subWidget) {
            html += `<div class="grid-header-cell no-sort" style="width:40px; min-width:40px; max-width:40px;"></div>`;
        }
        this.config.columns.forEach(col => {
            const w = col.width || 100;
            // 强制设置 min-width 和 max-width 确保列宽严格一致
            const style = `width:${w}px; min-width:${w}px; max-width:${w}px;`;
            html += `<div class="grid-header-cell" style="${style}" data-field="${col.field}">
                ${col.title} ${this.config.sortable !== false ? '<small>⇅</small>' : ''}
            </div>`;
        });
        this.els.header.innerHTML = html;

        // 设置表头容器最小宽度，确保内容不足时撑开
        const totalW = this.calcTotalWidth();
        this.els.header.style.minWidth = `${totalW}px`;
    }

    syncExpandedRows() {
        if (this.expandedKeys.size === 0) return;
        // 注意：不调用 resetHeights() 以保留 Quill 的高度记录
        const count = this.dataEngine.count;
        const widgetH = (this.config.subWidget && this.config.subWidget.height) || 200;
        const baseH = this.config.rowHeight || 40;
        const expandedH = baseH + widgetH;

        for (let i = 0; i < count; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            const key = this.dataEngine.getCompositeKey(rowData);
            if (this.expandedKeys.has(key)) {
                this.scroller.setRowHeight(i, expandedH);
            }
        }
    }

    refresh(force = false) {
        const viewportH = this.container.clientHeight || 500;
        this.scroller.updateMetrics(this.dataEngine.count, viewportH);

        const range = this.scroller.getRenderRange(this.scrollTop);
        this.els.phantom.style.height = `${range.totalHeight}px`;

        this.cleanupWidgets(true);

        const totalW = this.calcTotalWidth();

        let html = '';
        for (let i = range.start; i < range.end; i++) {
            const rowData = this.dataEngine.getRowByIndex(i);
            const key = this.dataEngine.getCompositeKey(rowData);
            const isSelected = this.selectedKeys.has(key);
            const isExpanded = this.expandedKeys.has(key);

            const top = this.scroller.getRowTop(i);
            const totalH = this.scroller.getRowHeight(i);
            const baseH = this.config.rowHeight || 40;

            // 渲染主行，强制设置 min-width 保证对齐
            html += `<div class="grid-row ${isSelected ? 'selected' : ''} ${isExpanded ? 'expanded' : ''}" 
                style="top:${top}px; height:${baseH}px; min-width:${totalW}px;" data-idx="${i}" data-key="${key}">`;

            if (this.config.subWidget) {
                html += `<div class="grid-cell expand-btn" style="width:40px; min-width:40px; max-width:40px;">
                    ${isExpanded ? '-' : '+'}
                </div>`;
            }

            this.config.columns.forEach(col => {
                let val = rowData[col.field];
                if (col.formatter) {
                    const fmt = this.formatterMgr.get(col.formatter);
                    if (fmt) val = fmt(val, rowData);
                }
                const w = col.width || 100;
                // 强制宽度样式
                const style = `width:${w}px; min-width:${w}px; max-width:${w}px;`;
                html += `<div class="grid-cell" style="${style}" data-field="${col.field}">${val}</div>`;
            });
            html += `</div>`;

            // 渲染子控件容器
            if (isExpanded && this.config.subWidget) {
                const subTop = top + baseH;
                const subHeight = totalH - baseH;
                html += `<div class="widget-container" id="widget-${key}" 
                    style="top:${subTop}px; height:${subHeight}px;"></div>`;
                setTimeout(() => this.mountChild(key, rowData), 0);
            }
        }
        this.els.content.innerHTML = html;
        this.restoreSelectionState();
    }

    mountChild(key, rowData) {
        const container = this.container.querySelector(`#widget-${key}`);
        if (!container || this.activeWidgets.has(key)) return;

        // 生成子控件 ID
        const childId = `${this.id}:row-${key}:sub`;

        const instance = widgetFactory.create(
            container,
            this.config.subWidget,
            rowData,
            this.config.globalContext,
            this.bus,
            childId // 传递生成的 ID
        );

        if (instance) this.activeWidgets.set(key, instance);
    }

    cleanupWidgets(all) {
        if (all) {
            this.activeWidgets.forEach(w => w.destroy && w.destroy());
            this.activeWidgets.clear();
        }
    }

    restoreSelectionState() {
        const rows = this.els.content.querySelectorAll('.grid-row');
        rows.forEach(r => {
            if (this.selectedKeys.has(r.dataset.key)) r.classList.add('selected');
            else r.classList.remove('selected');
        });
    }

    isChildWidgetEvent(e) {
        const widgetContainer = e.target.closest('.widget-container');
        if (widgetContainer && this.els.content.contains(widgetContainer)) {
            return true;
        }
        return false;
    }

    bindEvents() {
        this.container.addEventListener('scroll', () => {
            requestAnimationFrame(() => {
                this.scrollTop = this.container.scrollTop;
                this.refresh();
            });
        });

        this.els.content.addEventListener('click', (e) => {
            if (this.isDragSelecting) return;
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
            if (this.isChildWidgetEvent(e)) return;

            const row = e.target.closest('.grid-row');
            if (!row) return;
            const key = row.dataset.key;
            const idx = parseInt(row.dataset.idx);

            if (e.target.closest('.expand-btn')) {
                e.stopPropagation();
                this.toggleExpand(key, idx);
                this.emit('TRACK', { event: 'ROW_EXPAND_TOGGLE', payload: { key } });
                return;
            }

            if (this.config.selection) {
                if (e.ctrlKey) {
                    if (this.selectedKeys.has(key)) this.selectedKeys.delete(key);
                    else this.selectedKeys.add(key);
                } else {
                    this.selectedKeys.clear();
                    this.selectedKeys.add(key);
                }
                this.restoreSelectionState();
                this.emit('SELECTION_CHANGED', Array.from(this.selectedKeys));
                this.emit('TRACK', { event: 'ROW_CLICK_SELECT', payload: { key } });
            }
        });

        this.els.content.addEventListener('dblclick', (e) => {
            if (this.isChildWidgetEvent(e)) return;
            const cell = e.target.closest('.grid-cell');
            if (!cell || cell.classList.contains('editing')) return;
            const field = cell.dataset.field;
            const col = this.config.columns.find(c => c.field === field);
            if (col && col.editable) this.startEdit(cell, field, col);
        });

        this.container.addEventListener('contextmenu', (e) => {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
            if (this.isChildWidgetEvent(e)) return;
            if (e.target.closest('.grid-header')) return;

            e.preventDefault();
            const row = e.target.closest('.grid-row');
            if (row) {
                const key = row.dataset.key;
                if (!this.selectedKeys.has(key)) {
                    this.selectedKeys.clear();
                    this.selectedKeys.add(key);
                    this.restoreSelectionState();
                    this.emit('SELECTION_CHANGED', Array.from(this.selectedKeys));
                }
                this.emit('TRACK', { event: 'CONTEXT_MENU_OPEN', payload: { key } });
                this.showContextMenu(e.clientX, e.clientY);
            }
        });

        document.addEventListener('click', () => this.els.menu.style.display = 'none');

        this.els.header.addEventListener('click', (e) => {
            const th = e.target.closest('.grid-header-cell');
            if (th && th.dataset.field) {
                const f = th.dataset.field;
                this.dataEngine.sort(f, 'asc');
                this.syncExpandedRows();
                this.refresh();
                this.emit('TRACK', { event: 'SORT', payload: { field: f } });
            }
        });
    }

    toggleExpand(key, idx) {
        if (this.expandedKeys.has(key)) {
            this.expandedKeys.delete(key);
            this.scroller.setRowHeight(idx, this.config.rowHeight || 40);
        } else {
            this.expandedKeys.add(key);
            const wH = (this.config.subWidget && this.config.subWidget.height) || 200;
            this.scroller.setRowHeight(idx, (this.config.rowHeight || 40) + wH);
        }
        this.refresh();
    }

    startEdit(cell, field, col) {
        const row = cell.closest('.grid-row');
        const key = row.dataset.key;
        const idx = this.dataEngine.idMap.get(key);
        const originalVal = this.dataEngine.rawData[idx][field];

        this.emit('TRACK', { event: 'EDIT_START', payload: { key, field } });

        cell.classList.add('editing');
        const editorCfg = col.editor || { type: 'text' };

        let editorEl;
        switch (editorCfg.type) {
            case 'textarea':
                editorEl = document.createElement('textarea');
                editorEl.value = originalVal || '';
                break;
            case 'date':
                editorEl = document.createElement('input');
                editorEl.type = 'date';
                editorEl.value = originalVal || '';
                break;
            case 'datetime':
                editorEl = document.createElement('input');
                editorEl.type = 'datetime-local';
                editorEl.value = originalVal || '';
                break;
            case 'select':
                editorEl = document.createElement('select');
                (editorCfg.options || []).forEach(opt => {
                    const o = document.createElement('option');
                    o.value = typeof opt === 'object' ? opt.value : opt;
                    o.text = typeof opt === 'object' ? opt.label : opt;
                    editorEl.appendChild(o);
                });
                editorEl.value = originalVal || '';
                break;
            case 'multi-select':
                editorEl = this._createMultiSelectEditor(editorCfg.options, originalVal);
                break;
            default:
                editorEl = document.createElement('input');
                editorEl.type = 'text';
                editorEl.value = originalVal || '';
        }

        cell.innerHTML = '';
        cell.appendChild(editorEl);

        const focusEl = editorCfg.type === 'multi-select' ? editorEl : editorEl;
        focusEl.focus();
        if (focusEl.select && editorCfg.type === 'text') focusEl.select();

        const close = (isCancel = false) => {
            if (!cell.classList.contains('editing')) return;

            let newVal = originalVal;
            if (!isCancel) {
                newVal = this._getEditorValue(editorEl, editorCfg);
            }

            cell.classList.remove('editing');

            if (!isCancel && newVal !== originalVal) {
                this.dataEngine.rawData[idx][field] = newVal;
                this.emit('DATA_UPDATED', { key, field, value: newVal });
                this.emit('TRACK', { event: 'EDIT_END', payload: { key, field, value: newVal } });
            }

            // 刷新当前行显示 (由于是虚拟滚动，直接 refresh 比较稳妥)
            this.refresh();
        };

        // 处理事件
        if (editorCfg.type === 'multi-select') {
            // 多选逻辑：点击外部关闭
            const outerClick = (e) => {
                if (!cell.contains(e.target)) {
                    close();
                    document.removeEventListener('mousedown', outerClick);
                }
            };
            document.addEventListener('mousedown', outerClick);
        } else {
            editorEl.onblur = () => close();
            editorEl.onkeydown = (e) => {
                if (e.key === 'Enter' && editorCfg.type !== 'textarea') close();
                if (e.key === 'Escape') close(true);
            };
        }

        editorEl.onclick = (e) => e.stopPropagation();
    }

    _getEditorValue(el, cfg) {
        if (cfg.type === 'multi-select') {
            const checked = el.querySelectorAll('input:checked');
            return Array.from(checked).map(i => i.value);
        }
        return el.value;
    }

    _createMultiSelectEditor(options = [], currentValues = []) {
        const container = document.createElement('div');
        container.className = 'multi-select-editor';
        container.tabIndex = -1; // 使其可获焦

        const vals = Array.isArray(currentValues) ? currentValues : [currentValues];

        options.forEach(opt => {
            const val = typeof opt === 'object' ? opt.value : opt;
            const label = typeof opt === 'object' ? opt.label : opt;
            const item = document.createElement('label');
            item.innerHTML = `<input type="checkbox" value="${val}" ${vals.includes(val) ? 'checked' : ''}> ${label}`;
            container.appendChild(item);
        });
        return container;
    }

    showContextMenu(x, y) {
        if (!this.config.contextMenu) return;
        let html = '';
        this.config.contextMenu.forEach(item => {
            html += `<div class="context-menu-item" data-action="${item.action}">${item.label}</div>`;
        });
        this.els.menu.innerHTML = html;
        const left = x + 150 > window.innerWidth ? window.innerWidth - 150 : x;
        this.els.menu.style.left = `${left}px`;
        this.els.menu.style.top = `${y}px`;
        this.els.menu.style.display = 'block';
        this.els.menu.onclick = (e) => {
            const action = e.target.dataset.action;
            if (action) {
                this.emit('MENU_ACTION', { action, selection: Array.from(this.selectedKeys) });
                this.emit('TRACK', { event: 'MENU_ITEM_CLICK', payload: { action } });
            }
        };
    }

    initDragSelect() {
        if (!this.config.dragSelect) return;
        let start = null; let isDragging = false;
        const HEADER_HEIGHT = 40;

        const onDown = (e) => {
            if (e.button !== 0 || e.target.closest('.grid-header') || e.target.closest('.context-menu')) return;
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
            if (this.isChildWidgetEvent(e)) return;

            e.stopPropagation();
            e.preventDefault();
            isDragging = false; this.isDragSelecting = false;
            start = { x: e.clientX, y: e.clientY, sl: this.container.scrollLeft, st: this.container.scrollTop };
        };

        const onMove = (e) => {
            if (!start) return;
            const mx = e.clientX - start.x, my = e.clientY - start.y;
            if (!isDragging && (Math.abs(mx) > 5 || Math.abs(my) > 5)) {
                isDragging = true; this.isDragSelecting = true;
                this.els.selection.style.display = 'block';
                this.emit('TRACK', { event: 'DRAG_START', payload: { x: start.x, y: start.y } });
            }
            if (!isDragging) return;

            const rect = this.container.getBoundingClientRect();
            // 【重要】减去 clientLeft (边框)，修复子表格内拖拽坐标偏移问题
            const offsetLeft = rect.left + this.container.clientLeft;
            const offsetTop = rect.top + this.container.clientTop;

            const sx = start.x - offsetLeft + start.sl;
            const sy = start.y - offsetTop + start.st;
            const cx = e.clientX - offsetLeft + this.container.scrollLeft;
            const cy = e.clientY - offsetTop + this.container.scrollTop;

            const l = Math.min(sx, cx), t = Math.min(sy, cy);
            const w = Math.abs(cx - sx), h = Math.abs(cy - sy);

            this.els.selection.style.left = l + 'px'; this.els.selection.style.top = t + 'px';
            this.els.selection.style.width = w + 'px'; this.els.selection.style.height = h + 'px';

            // 【重要】减去表头高度，修正选区逻辑坐标
            const [sIdx, eIdx] = this.scroller.getIndicesInRange(t - HEADER_HEIGHT, t + h - HEADER_HEIGHT);
            const newSel = new Set(e.ctrlKey ? this.selectedKeys : []);

            for (let i = sIdx; i < eIdx; i++) {
                const rTop = this.scroller.getRowTop(i);
                const baseH = this.config.rowHeight || 40;
                // 逻辑坐标系 (不含表头)
                const selTop = t - HEADER_HEIGHT;
                const selBottom = t + h - HEADER_HEIGHT;
                if (rTop < selBottom && rTop + baseH > selTop) {
                    newSel.add(this.dataEngine.getCompositeKey(this.dataEngine.getRowByIndex(i)));
                }
            }
            this.selectedKeys = newSel;
            this.restoreSelectionState();
        };

        const onUp = () => {
            if (isDragging) {
                this.emit('SELECTION_CHANGED', Array.from(this.selectedKeys));
                this.emit('TRACK', { event: 'DRAG_END', payload: { count: this.selectedKeys.size } });
                setTimeout(() => this.isDragSelecting = false, 0);
            }
            start = null; isDragging = false;
            this.els.selection.style.display = 'none';
        };

        this.container.addEventListener('mousedown', onDown);
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
        this._globalHandlers.push(() => document.removeEventListener('mousemove', onMove));
        this._globalHandlers.push(() => document.removeEventListener('mouseup', onUp));
    }

    /**
     * 发送消息，自动注入来源 ID
     */
    emit(type, payload) {
        if (this.bus) {
            // 注入 _widgetId 以便宿主区分消息来源
            this.bus.emit(type, { ...payload, _widgetId: this.id });
        }
    }
}