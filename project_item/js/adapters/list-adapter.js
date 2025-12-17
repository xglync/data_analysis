import { widgetFactory } from '../core/widget-factory.js';
import { ListWidget } from '../core/list.js';

// 注册 'list' 类型
widgetFactory.register('list', ListWidget);