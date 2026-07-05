// ============================================================
// Appsmith JSObject: DataService
// 负责调用 Appsmith Query 获取数据，并通过 Messenger 发送给 combine.html
// 用法：在 Appsmith 中新建第二个 JSObject，粘贴本文件内容
// ============================================================

export default {
    /**
     * 获取全量问题数据并下发
     * 依赖 Query: fetch_questions（见 queries.md）
     */
    async fetchQuestions() {
        try {
            const result = await fetch_questions.run();
            // 根据实际 API 返回结构调整字段映射
            const data = Array.isArray(result) ? result : (result?.data || result?.rows || []);
            Messenger.sendMessage('RES_DATA_QUESTION', data);
            return data;
        } catch (err) {
            console.error('fetchQuestions failed:', err);
            return [];
        }
    },

    /**
     * 获取全量手机数据并下发
     * 依赖 Query: fetch_phones（见 queries.md）
     */
    async fetchPhones() {
        try {
            const result = await fetch_phones.run();
            const data = Array.isArray(result) ? result : (result?.data || result?.rows || []);
            Messenger.sendMessage('RES_DATA_PHONE', data);
            return data;
        } catch (err) {
            console.error('fetchPhones failed:', err);
            return [];
        }
    },

    /**
     * 双击问题 → 查询关联手机 → 下发筛选结果
     * 依赖 Query: fetch_phones_by_problem（见 queries.md）
     * @param {number} questionId
     * @param {object} question
     */
    async fetchFilteredPhones(questionId, question) {
        try {
            const result = await fetch_phones_by_problem.run({ problem_id: questionId });
            const data = Array.isArray(result) ? result : (result?.data || result?.rows || []);
            Messenger.sendMessage('RES_DATA_FILTERED_PHONES', data);
            storeValue('currentDetailQuestion', question);
            return data;
        } catch (err) {
            console.error('fetchFilteredPhones failed:', err);
            return [];
        }
    },

    /**
     * 下发配置到 combine.html
     * @param {object} cfg - { phoneCfg?, questionCfg? }
     */
    sendConfig(cfg) {
        Messenger.sendMessage('SET_CONFIG', cfg);
    },

    /**
     * 处理业务操作（示例：右键菜单 → 调用后端 API）
     * 根据 lastAction 中的 type 分发
     */
    async handleBusinessAction(actionType, actionData) {
        switch (actionType) {
            case 'QUESTION_DETAIL':
                // 打开详情弹窗等
                storeValue('detailQuestionIds', actionData.ids);
                break;
            case 'QUESTION_DELETE':
                await delete_question.run({ ids: actionData.ids });
                await this.fetchQuestions();  // 刷新
                break;
            case 'PHONE_BATCH_REPAIR':
                await batch_repair_phones.run({ ids: actionData.ids });
                await this.fetchPhones();  // 刷新
                break;
            case 'PHONE_BATCH_SCRAP':
                await batch_scrap_phones.run({ ids: actionData.ids });
                await this.fetchPhones();
                break;
            case 'PHONE_TO_QUESTION': {
                // 处理拖拽关联
                const { phoneIds, questionId } = actionData;
                storeValue('lastDragAssign', { phoneIds, questionId });
                // 示例：调用后端的关联 API
                // await assign_phones_to_question.run({ phoneIds, questionId });
                break;
            }
            default:
                console.log('Unhandled action:', actionType, actionData);
        }
    }
};
