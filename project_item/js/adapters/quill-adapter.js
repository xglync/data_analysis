import { widgetFactory } from '../core/widget-factory.js';

const QUILL_CSS = 'https://cdn.quilljs.com/1.3.6/quill.snow.css';
const QUILL_JS = 'https://cdn.quilljs.com/1.3.6/quill.min.js';

let _quillResourcePromise = null;

class QuillAdapter {
    constructor(container, messageBus) {
        this.container = container;
        this.bus = messageBus;
        this.quill = null;
        this.rowData = null;
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
        this.rowData = data;
        await this.loadResources();

        // 1. 构建 DOM
        // 外层 min-height: 100% (如果 List 给定高度则撑满，如果是 auto 则随内容)
        this.container.innerHTML = `
            <div style="display:flex; flex-direction:column; min-height:100%; padding: 10px; box-sizing: border-box;">
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

        // 3. 样式强制修正，允许高度撑开
        const qlContainer = this.container.querySelector('.ql-container');
        if (qlContainer) {
            qlContainer.style.height = 'auto'; // 关键
            qlContainer.style.flex = '1';
            qlContainer.style.fontFamily = 'inherit';
        }
        const qlEditor = this.container.querySelector('.ql-editor');
        if (qlEditor) {
            qlEditor.style.minHeight = '100px'; // 给一个最小高度
            qlEditor.style.height = 'auto';
            qlEditor.style.overflowY = 'visible';
        }

        // 4. Set Content
        try {
            if (data.context) {
                const delta = JSON.parse(data.context);
                this.quill.setContents(delta);
            }
        } catch (e) {
            console.error('Quill Parse Error', e);
            this.quill.setText('Error loading content.');
        }

        // 5. Change Listener
        this.quill.on('text-change', (delta, oldDelta, source) => {
            if (source === 'user') {
                const currentContent = JSON.stringify(this.quill.getContents());
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