import { widgetFactory } from '../core/widget-factory.js';

// 【修改】将资源路径指向本地目录
const QUILL_CSS = 'js/thirdparty/quill/dist/quill.snow.css';
const QUILL_JS = 'js/thirdparty/quill/dist/quill.js';

let _quillResourcePromise = null;

class QuillAdapter {
    constructor(container, messageBus) {
        this.container = container;
        this.bus = messageBus;
        this.quill = null;
        this.rowData = null; // 这里持有 DataEngine 中某一行数据的引用
    }

    async loadResources() {
        if (window.Quill) return Promise.resolve();
        if (!_quillResourcePromise) {
            _quillResourcePromise = new Promise((resolve, reject) => {
                if (!document.querySelector(`link[href="${QUILL_CSS}"]`)) {
                    const link = document.createElement('link');
                    link.rel = 'stylesheet';
                    link.href = QUILL_CSS;
                    document.head.appendChild(link);
                }
                const script = document.createElement('script');
                script.src = QUILL_JS;
                script.onload = () => {
                    if (window.Quill) resolve();
                    else reject(new Error("Quill script loaded but window.Quill undefined"));
                };
                script.onerror = reject;
                document.head.appendChild(script);
            });
        }
        return _quillResourcePromise;
    }

    async init(config, data) {
        // 保存数据引用
        this.rowData = data;
        await this.loadResources();

        // 1. 构建 DOM
        // 【修正】移除 min-height: 100%，让内容自然驱动容器高度
        this.container.innerHTML = `
            <div style="display:flex; flex-direction:column; padding: 10px; box-sizing: border-box;">
                <div style="
                    font-weight: bold;
                    color: #1890ff;
                    margin-bottom: 8px;
                    font-size: 14px;
                    padding-left: 8px;
                    border-left: 4px solid #1890ff;
                    flex-shrink: 0;
                ">
                    ${data.duration || 'No Duration'}
                </div>
                <div class="quill-wrapper" style="flex:1; display:flex; flex-direction:column;">
                    <div class="quill-editor-container" style="background:#fff;"></div>
                </div>
            </div>
        `;

        const editorEl = this.container.querySelector('.quill-editor-container');

        // 2. Init Quill
        this.quill = new window.Quill(editorEl, {
            theme: 'snow',
            modules: {
                toolbar: [
                    ['bold', 'italic', 'underline', 'strike'],
                    [{ 'list': 'ordered' }, { 'list': 'bullet' }],
                    [{ 'color': [] }, { 'background': [] }],
                    ['clean']
                ]
            }
        });

        // 3. 样式修正
        const qlContainer = this.container.querySelector('.ql-container');
        if (qlContainer) {
            qlContainer.style.height = 'auto';
            qlContainer.style.flex = 'none'; // 【修正】不强制填充，由内容撑开
            qlContainer.style.fontFamily = 'inherit';
        }
        const qlEditor = this.container.querySelector('.ql-editor');
        if (qlEditor) {
            qlEditor.style.minHeight = '100px';
            qlEditor.style.height = 'auto';
            qlEditor.style.overflowY = 'visible';
        }

        // 4. 设置初始内容
        try {
            if (data.context) {
                const delta = JSON.parse(data.context);
                this.quill.setContents(delta);
            }
        } catch (e) {
            console.error('Quill Parse Error', e);
            this.quill.setText('Error loading content.');
        }

        // 5. 监听变更
        this.quill.on('text-change', (delta, oldDelta, source) => {
            if (source === 'user') {
                const currentContent = JSON.stringify(this.quill.getContents());

                // 【核心修复】同步更新内存中的数据对象
                // 因为 this.rowData 是引用类型，修改它会直接影响 DataEngine 里的数据
                // 当虚拟滚动重新渲染此行时，会读取到更新后的 context
                this.rowData.context = currentContent;

                // 依然发送消息给父窗口（用于保存到数据库等操作）
                if (this.bus) {
                    this.bus.emit('QUILL_UPDATED', {
                        duration: this.rowData.duration,
                        context: currentContent,
                        rowId: this.rowData.id
                    });
                }
            }
        });
    }

    destroy() {
        this.container.innerHTML = '';
        this.quill = null;
    }
}

widgetFactory.register('quill', QuillAdapter);