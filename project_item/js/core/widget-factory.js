/**
 * WidgetFactory 类
 * 控件工厂，负责解耦“表格逻辑”与“子控件具体实现”。
 * 它负责实例化、数据注入和配置注入。
 */
export class WidgetFactory {
    constructor() {
        this.registry = new Map();
    }

    register(type, ClassRef) {
        this.registry.set(type, ClassRef);
    }

    /**
     * 创建子控件实例
     * @param {HTMLElement} container 挂载点
     * @param {object} definition 配置定义
     * @param {object} parentRow 父行数据
     * @param {object} globalContext 全局上下文
     * @param {MessageBus} messageBus 消息总线（用于透传消息）
     * @param {string} id 控件ID
     */
    create(container, definition, parentRow, globalContext, messageBus, id) {
        const ClassRef = this.registry.get(definition.type);
        if (!ClassRef) {
            container.innerHTML = `<div style="padding:10px;color:red">Unknown Widget: ${definition.type}</div>`;
            return null;
        }

        // 实例化时传入 bus 和 id
        const instance = new ClassRef(container, messageBus, id);

        // 数据解析 (Data Resolver)
        // 支持字符串形式的代码，解决 iframe 无法传递函数的问题
        let resolvedData = [];
        if (definition.dataResolver) {
            try {
                let fn = definition.dataResolver;
                if (typeof fn === 'string') {
                    fn = new Function('row', 'ctx', fn);
                }

                if (typeof fn === 'function') {
                    resolvedData = fn(parentRow, globalContext);
                } else {
                    resolvedData = fn;
                }
            } catch (e) {
                console.error("Data Resolver Error:", e);
            }
        }

        // 初始化
        if (instance.init) {
            instance.init(definition.config, resolvedData);
        }

        return instance;
    }
}

export const widgetFactory = new WidgetFactory();