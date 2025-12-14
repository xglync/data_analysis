/**
 * FormatterManager 类
 * 管理单元格格式化函数。
 * 解决了 Iframe 无法直接传递 Function 对象的问题，支持代码字符串编译和远程脚本加载。
 */
export class FormatterManager {
    constructor() {
        this.formatters = new Map();
        // 注册内置格式化器
        this.register('date', (v) => v ? new Date(v).toLocaleDateString() : '');
    }

    register(name, fn) {
        this.formatters.set(name, fn);
    }

    get(name) {
        return this.formatters.get(name);
    }

    /**
     * 通过代码字符串注册 (new Function 方式)
     * @param {string} name 格式化器名称
     * @param {string} codeBody 函数体，参数为 (value, row)
     */
    registerFromCode(name, codeBody) {
        try {
            const fn = new Function('value', 'row', codeBody);
            this.register(name, fn);
        } catch (e) {
            console.error(`Formatter [${name}] compilation failed:`, e);
        }
    }

    /**
     * 通过加载外部 JS 脚本注册
     * @param {string} url 脚本 URL
     * @param {string} callbackName 脚本执行后挂载在 window 上的对象名
     */
    loadFromScript(url, callbackName) {
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = url;
            script.onload = () => {
                if (window[callbackName]) {
                    const fns = window[callbackName];
                    Object.keys(fns).forEach(k => this.register(k, fns[k]));
                    resolve();
                } else {
                    reject('Callback object missing in script');
                }
            };
            script.onerror = reject;
            document.head.appendChild(script);
        });
    }
}