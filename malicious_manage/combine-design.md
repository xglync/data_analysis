# combine.html — 合并视图设计文档

> 版本: 2.0 | 日期: 2026-06-25

## v2.0 重大更新

- **多实例支持**：引入 `instanceId` 机制，消息精准路由，支持单页部署多个 combine 实例
- **字段映射更新**：Problem/Phone 字段 key 全面升级（详见 §3）
- **布局交换**：问题面板左、手机面板右
- **联动功能**：双击问题 → `QUESTION_QUERY_PHONES` → 父窗口返回筛选数据 → 右侧显示关联手机
- **详情回溯**：状态栈保存/恢复，点击返回无需重新加载
- **Phone 面板重构**：策略模式解耦渲染与操作，支持卡片/表格一键切换
- **SmithApp 部署**：消息队列防覆盖

---

## 1. 概述

`combine.html` 将「不良手机管理」与「不良问题管理」合并为单页面左右双栏视图，并支持拖拽手机到问题行建立关联。采用面向对象架构，核心组件可独立复用。

```
┌─────────────────────────────────────────────────────────┐
│  top-bar: 不良管理 — 合并视图              [← 返回]      │
├──────────────────────┬──────────────────────────────────┤
│  QuestionPanel (左)   │  PhonePanel (右)                 │
│  ┌────┬──────┬──────┐ │  ┌────────────────────────────┐  │
│  │编号│问题..│严重..│ │  │ [▦卡片] [☰表格]             │  │
│  ├────┼──────┼──────┤ │  │ ▼ 组装A线 (12台)            │  │
│  │ 1  │外观..│ 🔴高 │ │  │   ● 屏幕划痕 (4)            │  │
│  │ 2  │性能..│ 🟡中 │ │  │   [卡片][卡片]              │  │
│  └────┴──────┴──────┘ │  └────────────────────────────┘  │
│     ↑ 双击查关联手机    │     ↑ 拖拽源 / 卡片⇔表格切换      │
├──────────────────────┴──────────────────────────────────┤
│  DragBridge: phone → question                           │
│  TooltipManager / 状态回溯栈                             │
└─────────────────────────────────────────────────────────┘
│  │ ▼ 测试A线 (8台)    │  │  └────┴──────┴──────┴────┘    │
│  └───────────────────┘  │                                │
│     ↑ 拖拽源             │     ↑ 拖放目标                  │
├────────────────────────┴────────────────────────────────┤
│  DragBridge: phone → question 拖拽桥接                   │
│  TooltipManager: 溢出文本悬浮提示                         │
└─────────────────────────────────────────────────────────┘
```

---

## 2. 类层次结构

```
BasePanel                         ← 抽象基类
├── PhonePanel                    ← 手机面板
│   ├── 分组卡片渲染
│   ├── 框选 (rubber-band)
│   ├── 右鍵菜单 / 双击
│   └── 拖拽源接口 (getDragPayload, ensureCardSelected, setDraggingVisual)
│
└── QuestionPanel                 ← 问题面板
    ├── 表格渲染
    ├── 右鍵菜单
    └── 拖放目标接口 (findDropTarget, dropHighlightClass, getQuestionByRow)

DragBridge(source, target, onDrop)  ← 通用组件间拖拽
TooltipManager                      ← 溢出文本悬浮提示
```

### 2.1 BasePanel

```typescript
class BasePanel {
  data: any[];              // 原始数据
  selected: Set<number>;    // 选中项 id 集合
  config: object;           // 面板配置（字段/菜单等）

  render(): void;                    // 全量重建 DOM（数据变更时）
  updateSelectionUI(): void;         // 轻量切换 .selected class
  getSelectedItems(): any[];         // 获取选中项原始数据
  receiveData(data): void;           // 接收父窗口下发的数据
  onMenuAction(msgId: string): void; // 处理右键菜单动作
}
```

**关键设计**：`render()` 和 `updateSelectionUI()` 分离。前者只在数据变更时全量重建 DOM；后者仅切换 class，不销毁 DOM，确保双击等事件不被中断。

### 2.2 PhonePanel

```
PhonePanel extends BasePanel
  ├── config.groupBy      → { level1, level2 }  两级分组字段
  ├── config.cardFields   → [{ key, label, mono?, render? }]  卡片字段
  ├── config.contextMenu  → [{ label, msgId, callback? }]     右键菜单
  │
  ├── render()            → 分组 HTML 生成 → innerHTML
  ├── updateSelectionUI() → querySelectorAll → classList.toggle
  │
  ├── _initEvents()
  │   ├── scroll.click       → 卡片选中 / 折叠 / 空白清空
  │   ├── content.dblclick   → 双击发送 PHONE_DBLCLICK
  │   ├── content.contextmenu → 右键菜单
  │   └── scroll.mousedown   → 框选 (position:fixed + clientX/Y)
  │
  └── 拖拽源接口
      ├── getDragPayload()      → { phoneIds, phones }
      ├── ensureCardSelected()  → 拖拽前确保卡片选中
      └── setDraggingVisual()   → 拖拽中半透明效果
```

**框选实现**：

- `selRect` 使用 `position: fixed` + 原始 `clientX/Y`，零坐标换算
- mousemove/mouseup 绑定在 `document` 上，确保鼠标移出容器后仍跟踪
- 碰撞检测用 `getBoundingClientRect()` 在视口坐标系直接比对
- `wasDrag` + `setTimeout(0)` 防止后续 click/dblclick 误清空

### 2.3 QuestionPanel

```
QuestionPanel extends BasePanel
  ├── config.fields       → [{ key, label, width?, render? }]  列定义
  ├── config.contextMenu  → [{ label, msgId, callback? }]      右键菜单
  │
  ├── render()            → 动态 min-width + 表头/表体 HTML
  ├── updateSelectionUI() → querySelectorAll → classList.toggle
  │
  ├── _initEvents()
  │   ├── qWrap.click        → 行选中 / 空白清空
  │   └── qBody.contextmenu  → 右键菜单
  │
  └── 拖放目标接口
      ├── findDropTarget(el)      → el.closest('.q-row')
      ├── dropHighlightClass      → 'drop-target'
      └── getQuestionByRow(tr)    → 根据行元素反查数据
```

**水平滚动**：表格 `min-width` 根据列配置动态计算（auto 列按 120px），字段过多时自动出现水平滚动条。

### 2.4 DragBridge

通用组件间拖拽桥接，不依赖具体面板类型，仅依赖接口约定。

```typescript
class DragBridge {
  constructor(
    source: { getDragPayload, ensureCardSelected, setDraggingVisual },
    target: { findDropTarget, dropHighlightClass, getQuestionByRow },
    onDrop: (payload, targetData) => void,
    ghostEl: HTMLElement
  )
}
```

**事件流**：

```
dragstart (phoneContent)
  → source.ensureCardSelected(card)
  → payload = source.getDragPayload()
  → dataTransfer.setData('application/json', payload)
  → setDragImage(ghostEl)

dragover (qBody)
  → tr = target.findDropTarget(e.target)
  → tr.classList.add(target.dropHighlightClass)

dragleave (qBody)
  → tr.classList.remove(target.dropHighlightClass)

drop (qBody)
  → payload = JSON.parse(dataTransfer.getData(...))
  → question = target.getQuestionByRow(tr)
  → onDrop(payload, question)
```

### 2.5 TooltipManager

通用溢出文本悬浮提示，通过事件委托监听任意容器的任意选择器。

```typescript
class TooltipManager {
  constructor(opts?: {
    delay?: number;      // 显示延迟 (default 300ms)
    hideDelay?: number;  // 隐藏延迟 (default 200ms)
    offsetX?: number;    // 水平偏移
    offsetY?: number;    // 垂直偏移
  })

  attach(
    container: HTMLElement,
    selector: string,
    getText?: (el: HTMLElement) => string
  ): void
}
```

**判定逻辑**：仅当 `el.scrollWidth > el.clientWidth` 或 `el.scrollHeight > el.clientHeight` 时才显示（即文本确实被 CSS 截断）。

**交互设计**：

```
hover 溢出元素 → 300ms → tooltip 出现在鼠标右下方（位置锁定）
  → 鼠标移入 tooltip → 保持显示，可选中复制文字
  → 鼠标离开 tooltip → 200ms 后隐藏
  
mousemove 跟随规则：仅 _onSource && !_onTooltip 时更新位置
（鼠标离开源元素移向 tooltip 时，tooltip 静止不动）
```

**当前挂载**：

- `phoneScroll` 下 `.phone-card .cv`（卡片值）
- `qWrap` 下 `td`（表格单元格）

---

## 3. CSS 布局系统

```
body (flex column, height: 100%)
├── .top-bar (flex-shrink: 0, 固定高 ~30px)
└── .split (flex: 1, min-height: 0)
    ├── .panel (flex: 1, flex column)
    │   ├── .panel-hdr (flex-shrink: 0, ~28px)
    │   └── .phone-scroll (flex: 1, overflow-y: auto)
    └── .panel (flex: 1, flex column)
        ├── .panel-hdr
        └── .q-wrap (flex: 1, overflow: auto)
```

**关键约束链路**：

- `body` 必须 `display: flex; flex-direction: column`，否则 `.split { flex: 1 }` 无效
- 每个 `.panel` 用 `flex column` 让 header 固定、滚动区自适应
- 滚动区用 `overflow: auto` + `min-height: 0`（flex 子元素默认 min-height: auto 会阻止收缩）

**卡片宽度**：

- `width: 24%` + `min-width: 120px` → 宽屏约 4 张/行等宽，窄屏自动换行

---

## 4. 消息协议

与父窗口通过 `postMessage` 通信。消息格式统一为 `{ type, data }`。

### 4.1 数据下发

| 消息                  | 方向   | 数据格式                                                                       |
| --------------------- | ------ | ------------------------------------------------------------------------------ |
| `RES_DATA_PHONE`    | 父→子 | `[{ id, sn, model, workstation, defectType, status, line, operator, time }]` |
| `RES_DATA_QUESTION` | 父→子 | `[{ id, type, description, severity, workstation, status, date, reporter }]` |
| `REQ_DATA_PHONE`    | 子→父 | `null`                                                                       |
| `REQ_DATA_QUESTION` | 子→父 | `null`                                                                       |
| `SET_CONFIG`        | 父→子 | `{ phoneCfg?, questionCfg? }`                                                |

### 4.2 用户操作

**手机面板**：

| 消息                      | 触发         | 数据                                                                  |
| ------------------------- | ------------ | --------------------------------------------------------------------- |
| `PHONE_BATCH_REPAIR`    | 右键菜单     | `{ ids, action: 'repair' }`                                         |
| `PHONE_BATCH_SCRAP`     | 右键菜单     | `{ ids, action: 'scrap' }`                                          |
| `PHONE_EXPORT_SELECTED` | 右键菜单     | `{ ids, action: 'export' }`                                         |
| `PHONE_COPY_SN`         | 右键菜单     | `{ sns, action: 'copy' }`                                           |
| `PHONE_DBLCLICK`        | 双击卡片     | `{ phoneIds, ids, action: 'double_click' }`                         |
| `PHONE_TO_QUESTION`     | 拖拽到问题行 | `{ phoneIds, questionId, phones, question, action: 'drag_assign' }` |

**问题面板**：

| 消息                       | 触发     | 数据                           |
| -------------------------- | -------- | ------------------------------ |
| `QUESTION_DETAIL`        | 右键菜单 | `{ ids, action: 'detail' }`  |
| `QUESTION_MARK_RESOLVED` | 右键菜单 | `{ ids, action: 'resolve' }` |
| `QUESTION_ASSIGN`        | 右键菜单 | `{ ids, action: 'assign' }`  |
| `QUESTION_EXPORT`        | 右键菜单 | `{ ids, action: 'export' }`  |
| `QUESTION_DELETE`        | 右键菜单 | `{ ids, action: 'delete' }`  |

### 4.3 `PHONE_TO_QUESTION` 拖拽消息详析

```json
{
  "type": "PHONE_TO_QUESTION",
  "data": {
    "phoneIds": [1, 2, 3],
    "questionId": 5,
    "phones": [
      { "id": 1, "sn": "SN2026050001", "model": "X100 Pro", "workstation": "组装A线", "defectType": "屏幕划痕" }
    ],
    "question": {
      "id": 5, "type": "外观不良", "description": "后盖玻璃碎裂+中框变形...", "workstation": "组装A线"
    },
    "action": "drag_assign"
  }
}
```

---

## 5. 实例化与初始化

```javascript
// 面板
const phonePanel = new PhonePanel();
const questionPanel = new QuestionPanel();

// 拖拽桥接
new DragBridge(phonePanel, questionPanel, (payload, question) => {
  sendToParent('PHONE_TO_QUESTION', { ... });
}, document.getElementById('dragGhost'));

// 溢出提示
const tooltip = new TooltipManager({ delay: 300 });
tooltip.attach(document.getElementById('phoneScroll'), '.phone-card .cv');
tooltip.attach(document.getElementById('qWrap'), 'td');

// 初始请求数据
sendToParent('REQ_DATA_PHONE', null);
sendToParent('REQ_DATA_QUESTION', null);
```

---

## 6. 扩展指南

### 6.1 添加第三个面板

```javascript
class ModulePanel extends BasePanel {
  constructor() { super('modTotal', 'modSelNum'); /* ... */ }
  // 实现 render / updateSelectionUI / _initEvents
  // 可选实现拖拽源/目标接口
}

const modPanel = new ModulePanel();
new DragBridge(phonePanel, modPanel, (payload, mod) => { /* ... */ }, ghostEl);
```

### 6.2 自定义 tooltip 取文本

```javascript
tooltip.attach(container, '.custom-sel', el => el.dataset.fulltext || el.textContent);
```

### 6.3 更换拖拽目标

只需目标面板实现 `findDropTarget` / `dropHighlightClass` / `getQuestionByRow`（或等价的业务数据查询方法），即可与 `DragBridge` 对接。

---

## 7. 文件清单

| 文件                  | 说明                                                              |
| --------------------- | ----------------------------------------------------------------- |
| `combine.html`      | 合并视图（本文件）                                                |
| `combine-demo.html` | 合并视图 Demo 父窗口（含 mock 数据 + PHONE_TO_QUESTION 高亮日志） |
| `question.html`     | 独立问题页                                                        |
| `phone.html`        | 独立不良机页                                                      |
| `demo.html`         | 双 iframe Demo                                                    |
