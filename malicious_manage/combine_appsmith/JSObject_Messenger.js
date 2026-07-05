// ============================================================
// Appsmith JSObject: Messenger  (v2 — 消息队列防竞态)
// 挂载到 Appsmith 中，负责 iframe ↔ Appsmith 双向消息通信
// ============================================================

export default {
    _instanceId: 'appsmith_main',
    _iframeSelector: 'iframe[name="combineFrame"]',
    _messageLog: [],

    // ---- 消息队列（防止短时间多条消息互相覆盖）----
    _queue: [],
    _processing: false,

    _enqueue(fn) {
        this._queue.push(fn);
        storeValue('msgQueueSize', this._queue.length);
        if (!this._processing) this._flush();
    },

    async _flush() {
        if (!this._queue.length) { this._processing = false; storeValue('msgQueueSize', 0); return; }
        this._processing = true;
        const fn = this._queue.shift();
        storeValue('msgQueueSize', this._queue.length);
        try { await fn(); } catch (e) { console.error('[Messenger] queue error:', e); }
        this._flush();
    },

    /** onPageLoad 中调用: await Messenger.init() */
    init() {
        const self = this;
        window.addEventListener('message', (e) => {
            const msg = e.data;
            if (!msg || !msg.type) return;
            if (msg.instanceId && msg.instanceId !== self._instanceId) return;

            self._enqueue(async () => {
                self._log('in', msg.type, msg.data);
                switch (msg.type) {
                    case 'REQ_DATA_QUESTION':
                        await DataService.fetchQuestions(); break;
                    case 'REQ_DATA_PHONE':
                        await DataService.fetchPhones(); break;
                    case 'QUESTION_QUERY_PHONES':
                        await DataService.fetchFilteredPhones(msg.data.questionId, msg.data.question); break;
                    case 'PHONE_TO_QUESTION':
                        storeValue('dragEvent', msg.data);
                        self._log('out', 'PHONE_TO_QUESTION_ACK', { status: 'received' }); break;
                    case 'PHONE_DBLCLICK':
                        storeValue('dblclickEvent', msg.data);
                        self._log('out', 'PHONE_DBLCLICK_ACK', { status: 'received' }); break;
                    default:
                        storeValue('lastAction', { type: msg.type, data: msg.data });
                        self._log('out', msg.type + '_ACK', { status: 'received' });
                }
            });
        });
        return true;
    },

    sendMessage(type, data) {
        postWindowMessage({ type, data, instanceId: this._instanceId }, this._iframeSelector, '*');
        this._log('out', type, data);
    },

    async sendAllData() {
        await DataService.fetchQuestions();
        await DataService.fetchPhones();
    },

    _log(dir, type, data) {
        this._messageLog.unshift({
            time: new Date().toTimeString().slice(0, 8), dir, type,
            data: typeof data === 'string' ? data : JSON.stringify(data).slice(0, 200)
        });
        if (this._messageLog.length > 100) this._messageLog.pop();
        storeValue('messageLog', this._messageLog);
    }
};
