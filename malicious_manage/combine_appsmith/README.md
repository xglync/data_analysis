# combine.html → Appsmith 集成指南

## 文件说明

| 文件 | 用途 |
|------|------|
| `JSObject_Messenger.js` | 核心通信层：postMessage 收发、消息路由、日志记录 |
| `JSObject_DataService.js` | 数据服务层：调用 Query 获取数据并下发、处理业务操作 |
| `queries.md` | Appsmith Query 定义（SQL / API） |

## 部署步骤

### 1. 上传 combine.html

将 `combine.html` 部署到可访问的静态服务器，或通过 Appsmith 的 S3 / FilePicker 托管。

URL 格式：`https://your-host/combine.html#id=appsmith_main`

### 2. 创建 Iframe 组件

在 Appsmith 页面中拖入 **Iframe** 组件：

| 属性 | 值 |
|------|-----|
| **name** | `combineFrame` |
| **src** | `combine.html` 的 URL（含 `#id=appsmith_main`） |
| **宽度** | 100% |
| **高度** | `{{appsmith.ui.innerHeight - 80}}px` |

### 3. 创建 JSObject

在 Appsmith 中创建两个 JSObject：

1. **Messenger** — 粘贴 `JSObject_Messenger.js` 内容
2. **DataService** — 粘贴 `JSObject_DataService.js` 内容

### 4. 创建 Query

参照 `queries.md` 创建以下 Query：

- `fetch_questions` — 获取问题数据
- `fetch_phones` — 获取手机数据
- `fetch_phones_by_problem` — 按问题 ID 查关联手机
- （可选）`delete_question`、`batch_repair_phones`、`batch_scrap_phones`

### 5. 页面加载时初始化

在 Appsmith 页面 **onPageLoad** 中添加：

```javascript
// 初始化 Messenger 并自动加载数据
await Messenger.init();
await Messenger.sendAllData();
```

### 6. 业务操作响应（可选）

监听 `lastAction` store 值的变化来处理右键菜单操作：

```javascript
// 在任意组件或 JSObject 中
const { type, data } = appsmith.store.lastAction || {};
if (type) {
  DataService.handleBusinessAction(type, data);
  storeValue('lastAction', null);  // 消费后清除
}
```

## 消息流向

```
combine.html (iframe)          Appsmith (父窗口)
       │                            │
       │── REQ_DATA_QUESTION ──────→│ → fetch_questions.run()
       │←── RES_DATA_QUESTION ─────│
       │                            │
       │── QUESTION_QUERY_PHONES ──→│ → fetch_phones_by_problem.run()
       │←── RES_DATA_FILTERED_... ──│
       │                            │
       │── PHONE_TO_QUESTION ──────→│ → storeValue('dragEvent', ...)
       │── PHONE_DBLCLICK ─────────→│ → storeValue('dblclickEvent', ...)
       │── QUESTION_DELETE ────────→│ → delete_question.run()
       │                            │
```

消息均携带 `instanceId: 'appsmith_main'` 实现多实例路由。
