/**
 * MessageBus 类
 * 负责 Iframe 内部与宿主页面（Host）之间的消息通信。
 * 采用发布/订阅模式，封装了 window.postMessage。
 */
export class MessageBus {
    constructor() {
        this.handlers = {};
        // 绑定事件监听，并保存引用以便销毁
        this._listener = (e) => this._onMessage(e);
        window.addEventListener('message', this._listener);
    }

    /**
     * 注册消息处理器
     * @param {string} type 消息类型 (e.g., 'INIT', 'SET_DATA')
     * @param {function} fn 处理函数
     */
    on(type, fn) {
        this.handlers[type] = fn;
    }

    /**
     * 发送消息给父窗口
     * @param {string} type 消息类型
     * @param {any} payload 消息载荷
     */
    emit(type, payload) {
        // 确保是在 Iframe 中且父窗口存在
        if (window.parent && window.parent !== window) {
            window.parent.postMessage({ type, payload }, '*');
        }
    }

    /**
     * 内部消息接收处理
     */
    _onMessage(e) {
        // 简单校验消息格式
        if (!e.data || !e.data.type) return;
        const { type, payload } = e.data;
        if (this.handlers[type]) {
            this.handlers[type](payload);
        }
    }

    /**
     * 销毁实例，移除全局监听
     */
    destroy() {
        window.removeEventListener('message', this._listener);
        this.handlers = {};
    }
}