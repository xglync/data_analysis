import { widgetFactory } from '../core/widget-factory.js';
import { Grid } from '../core/grid.js';

// 注册 Grid 适配器
widgetFactory.register('grid', Grid);

// 演示 Chart 适配器
class DemoChart {
    constructor(container) { this.container = container; }
    init(config, data) {
        this.container.innerHTML = `
            <div style="height:100%; display:flex; flex-direction:column; align-items:center; justify-content:center; background:#f0f8ff; color:#666;">
                <div style="font-size:24px;">📊</div>
                <div>Custom Widget Analysis</div>
                <div>Data Points: <strong>${data.length}</strong></div>
            </div>`;
    }
    destroy() { this.container.innerHTML = ''; }
}
widgetFactory.register('chart', DemoChart);