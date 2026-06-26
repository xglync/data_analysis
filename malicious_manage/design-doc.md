# 不良管理前端 — 设计文档

> 版本: 1.2 | 日期: 2026-06-01 | 作者: AI Assistant
>
> **v1.2 更新**: 框选功能多项 Bug 修复（坐标系、事件冲突等），详见 §8。

---

## 1. 架构概述

```
┌──────────────────────────────────────────────────┐
│                  父窗口 (Parent)                   │
│  ┌────────────────────────────────────────────┐  │
│  │              demo.html (可选)               │  │
│  │  ┌──────────────┐  ┌──────────────────────┐│  │
│  │  │  iframe      │  │  iframe              ││  │
│  │  │ question.html│  │  phone.html          ││  │
│  │  │              │  │                      ││  │
│  │  └──────┬───────┘  └──────────┬───────────┘│  │
│  │         │ postMessage         │             │  │
│  │         └─────────────────────┘             │  │
│  └────────────────────────────────────────────┘  │
│                       ↑                          │
│             postMessage 双向通信                   │
└──────────────────────────────────────────────────┘
```

两个子页面（`question.html`、`phone.html`）通过 `window.parent.postMessage` 与父窗口单向通信。页面本身**不做任何数据查询或筛选**，仅负责：

- 数据展示（紧凑、高信息密度）
- UI 交互（选择、框选、右键菜单）
- 将用户操作意图通过消息回传父窗口

---

## 2. 消息协议

### 2.1 消息格式

所有消息统一为 JSON 对象：

```json
{
  "type": "MSG_ID",
  "data": { ... }
}
```

### 2.2 预定义消息类型

| 消息 ID               | 方向   | 说明                           |
| --------------------- | ------ | ------------------------------ |
| `REQ_DATA_QUESTION` | 子→父 | 子页面请求不良问题数据         |
| `RES_DATA_QUESTION` | 父→子 | 父窗口下发不良问题数据         |
| `REQ_DATA_PHONE`    | 子→父 | 子页面请求不良手机数据         |
| `RES_DATA_PHONE`    | 父→子 | 父窗口下发不良手机数据         |
| `SET_CONFIG`        | 父→子 | 父窗口下发/更新 appConfig 配置 |
| `OPERATION_RESULT`  | 父→子 | 父窗口返回操作结果确认         |

### 2.3 右键菜单消息

右键菜单项中的 `msgId` 由 `appConfig.contextMenu` 配置，子页面在用户点击菜单项后：

1. 调用对应 `callback(selectedItems)` 获取数据对象
2. 构造 `{ type: msgId, data: callbackResult }` 发送给父窗口

### 2.4 数据格式

#### 不良问题数据 (RES_DATA_QUESTION)

```json
[
  {
    "id": 1,
    "type": "外观不良",
    "description": "屏幕划痕",
    "severity": "高",
    "workstation": "组装A线",
    "status": "待处理",
    "date": "2026-05-28",
    "reporter": "张三"
  }
]
```

#### 不良手机数据 (RES_DATA_PHONE)

```json
[
  {
    "id": 1,
    "sn": "SN2026050001",
    "model": "X100 Pro",
    "workstation": "组装A线",
    "defectType": "屏幕划痕",
    "status": "待维修",
    "line": "L1",
    "operator": "王五",
    "time": "2026-05-30 08:30"
  }
]
```

---

## 3. AppConfig 配置规范

`appConfig` 是控制页面行为的核心配置对象，由父窗口通过 `SET_CONFIG` 消息下发。页面保留默认配置，可在无父窗口配置时独立运行。

### 3.1 完整 Schema

```typescript
interface AppConfig {
  // ===== 问题页专用 =====
  fields?: FieldConfig[];          // 表格列定义

  // ===== 不良机页专用 =====
  groupBy?: {                     // 两级分组字段
    level1: string;               // 一级分组字段名（如 'workstation'）
    level2: string;               // 二级分组字段名（如 'defectType'）
  };
  cardFields?: CardFieldConfig[]; // 卡片展示字段

  // ===== 通用 =====
  contextMenu?: MenuItemConfig[]; // 右键菜单项
}

interface FieldConfig {
  key: string;          // 数据字段名
  label: string;        // 列头显示名
  width?: string;       // 列宽（如 '100px' 或 'auto'）
  render?: 'badge' | 'status' | undefined;  // 渲染方式
}

interface CardFieldConfig {
  key: string;          // 数据字段名
  label: string;        // 标签显示名
  mono?: boolean;       // 是否使用等宽字体（适合 SN 等编码字段）
  render?: 'status' | undefined;  // 渲染方式
}

interface MenuItemConfig {
  label: string;        // 菜单项文字
  msgId: string;        // 发送给父窗口的消息类型
  callback: (selectedItems: any[]) => any;  // 回调，返回发送给父窗口的数据
  type?: 'divider';     // 设为 'divider' 表示分隔线
  className?: string;   // 额外的 CSS 类名（如 'danger'）
}
```

### 3.2 默认配置示例

问题页默认字段：

```js
fields: [
  { key: 'id',          label: '编号',       width: '70px' },
  { key: 'type',        label: '问题类型',   width: '100px' },
  { key: 'description', label: '问题描述',   width: 'auto' },
  { key: 'severity',    label: '严重程度',   width: '90px',  render: 'badge' },
  { key: 'workstation', label: '工位',       width: '100px' },
  { key: 'status',      label: '状态',       width: '90px',  render: 'status' },
  { key: 'date',        label: '发现日期',   width: '110px' },
  { key: 'reporter',    label: '报告人',     width: '80px' }
]
```

不良机页默认分组：

```js
groupBy: { level1: 'workstation', level2: 'defectType' }
```

---

## 4. 页面设计详解

### 4.1 问题页 (`question.html`)

```
┌──────────────────────────────────────────┐
│ 状态栏: 共 N 条 | 已选 M 条 | 右键操作提示 │
├──────────────────────────────────────────┤
│  Sticky 表头                             │
│  ┌────┬────────┬────────┬──────┬──────┐  │
│  │编号│问题类型│问题描述│严重..│状态  │  │
│  ├────┼────────┼────────┼──────┼──────┤  │
│  │ 1  │外观不良│屏幕划痕│ 🔴高 │待处理│  │  ← 行 hover 高亮
│  │ 2  │性能不良│电池不足│ 🟡中 │处理中│  │  ← 单击选中
│  │ ...│        │        │      │      │  │  ← 右键弹出菜单
│  └────┴────────┴────────┴──────┴──────┘  │
├──────────────────────────────────────────┤
│  ← 字段过多时可水平滚动 →                 │
└──────────────────────────────────────────┘
```

**交互特性：**

- 单击行 → 选中该行（替换选择）
- Ctrl+单击 → 切换选中/取消
- 右键行 → 弹出上下文菜单（若该行未选中则自动选中）
- 菜单项点击 → 调用 callback(选中项) → 发送 `{ type: msgId, data: result }` 给父窗口
- Esc / 点击外部 → 关闭菜单

**溢出处理：**

- **水平滚动**：表格 `min-width` 由 `calcTableWidth()` 根据 `appConfig.fields` 动态计算。当总列宽超过容器宽度时，`.table-wrap` 自动出现水平滚动条；字段少时表格自然填满容器。
- 固定宽度字段取配置值（如 `'100px'`），`auto` 字段按 140px 最小值参与计算。
- **垂直滚动**：数据行超出可视区时自动出现垂直滚动条，表头 sticky 保持可见。

### 4.2 不良机页 (`phone.html`)

```
┌──────────────────────────────────────────┐
│ 状态栏: 共 N 台 | 已选 M 台 | 操作提示     │
├──────────────────────────────────────────┤
│  ▼ 组装A线 (12台)          ← 一级分组(可折叠)│
│    ● 屏幕划痕 (4台)        ← 二级分组       │
│    ┌──────┐ ┌──────┐ ┌───↕ Scrollable    │
│    │ SN.. │ │ SN.. │ │ SN.. │  ← 卡片     │
│    │ 机型 │ │ 机型 │ │ ... │             │
│    │ 状态 │ │ 状态 │ │     │             │
│    └──────┘ └──────┘ └─────│             │
│    max-height: 280px       │             │
│    ● 边框变形 (15台) ← 超出时内部滚动      │
│    ┌──────┐ ┌──────┐ ┌───↕              │
│    │ ...  │ │ ...  │ │ ... │             │
│    └──────┘ └──────┘ └─────│             │
│  ▼ 测试A线 (8台)                            │
│    ...                                     │
└──────────────────────────────────────────┘
```

**框选机制：**

1. `mousedown`（在任意非卡片/非菜单区域）→ 记录起点，显示选区矩形
2. `mousemove`（document 级别监听，确保鼠标移出容器后仍跟踪）→ 更新选区矩形大小
3. `mouseup` → 若选区 ≥ 4px 则清空原有选中（非 Ctrl 时）→ 碰撞检测选中相交卡片
4. Ctrl 按下时 → 切换模式（增选/减选）

**溢出处理（新增）：**

- 每个 L2 分类的 `.card-grid` 设置 `max-height: 280px` + `overflow-y: auto`
- 当某分类内不良手机数量过多时，卡片区域内部出现独立滚动条
- L2 标题始终固定可见，仅卡片网格区域滚动
- 全局页面仍可垂直滚动以浏览所有分组

**复选机制：**

- Ctrl+点击卡片 → 切换该卡片选中状态
- 单击卡片 → 替换选中（仅选中该卡片）
- 框选 + Ctrl → 切换范围内卡片

**批量右键菜单：**

右键时，菜单作用于**所有已选中卡片**。菜单项根据当前选中状态动态启用/禁用。支持的特殊操作：

- "全选本组" → 选中当前卡片所在二级分组的所有卡片
- "取消全选" → 清除所有选中

### 4.3 Demo 页 (`demo.html`)

```
┌──────────────────────────────────────────┐
│  Header: 标题 + [发送测试数据] [清空日志]  │
├────────────────────┬─────────────────────┤
│                    │                     │
│  问题页 iframe     │  不良机页 iframe     │
│  (question.html)   │  (phone.html)       │
│                    │                     │
├────────────────────┴─────────────────────┤
│  消息日志面板                             │
│  14:32:05 ← REQ_DATA_QUESTION            │
│  14:32:05 → RES_DATA_QUESTION (22条)     │
│  14:32:12 ← QUESTION_DETAIL {ids:[3]}    │
│  ...                                     │
└──────────────────────────────────────────┘
```

**Demo 页职责：**

- 用 iframe 嵌入两个子页面
- 生成随机 mock 数据（每次数据不同，模拟真实场景）
- 响应子页面的 `REQ_DATA_*` 请求，下发 `RES_DATA_*`
- 接收右键菜单消息并记录到日志
- 提供"发送测试数据"按钮手动刷新数据

---

## 5. 技术实现要点

### 5.1 无外部依赖

- 纯 HTML + CSS + Vanilla JS
- 不引用任何 CDN、npm 包或第三方库
- 兼容 Chrome 80+、Edge 80+、Firefox 75+

### 5.2 CSS 设计系统

| 变量                                         | 用途                     |
| -------------------------------------------- | ------------------------ |
| `--bg`                                     | 页面背景色               |
| `--surface`                                | 卡片/表格背景            |
| `--border`                                 | 边框颜色                 |
| `--text` / `--text-secondary`            | 文字颜色                 |
| `--primary`                                | 主题色（选中、链接）     |
| `--primary-light`                          | 主题色浅色（hover 背景） |
| `--danger` / `--warning` / `--success` | 语义色（严重程度、状态） |

### 5.3 安全性

- 所有用户数据显示前经过 `textContent` 赋值转义（XSS 防护）
- postMessage 使用 `'*'` 目标源（如需增强安全可改为指定 origin）
- 未验证消息来源（可按需添加 `e.origin` 白名单校验）

### 5.4 性能考虑

- 当前全量渲染，数据量由父窗口控制（建议 ≤ 500 条）
- 大量数据场景可扩展虚拟滚动（如 IntersectionObserver）
- 框选碰撞检测为 O(n) 朴素遍历，n≤500 时性能可接受

---

## 6. 扩展指南

### 6.1 添加新页面

1. 新建 `xxx.html`，复制 `question.html` 的消息分发框架
2. 定义该页面的 `appConfig` 默认值
3. 在 `window.addEventListener('message', ...)` 中添加对应的 `RES_DATA_XXX` / `SET_CONFIG` 处理
4. 在父窗口中添加对应的 iframe 和消息转发

### 6.2 添加新的右键菜单项

在 `appConfig.contextMenu` 数组中添加新项：

```js
{
  label: '🆕 新操作',
  msgId: 'CUSTOM_ACTION',
  callback: (selectedItems) => ({
    ids: selectedItems.map(s => s.id),
    customParam: 'value'
  })
}
```

父窗口监听 `CUSTOM_ACTION` 消息即可处理。

### 6.3 添加新的展示字段

在 `appConfig.fields`（问题页）或 `appConfig.cardFields`（不良机页）中添加：

```js
{ key: 'newFieldName', label: '新字段显示名', width: '100px' }
```

可选 `render: 'badge'` 或 `render: 'status'` 来使用预设渲染样式。如需自定义渲染，可在 `renderCell()` / `renderCardValue()` 函数中添加新分支。

### 6.4 自定义消息处理

在子页面的消息 switch 中添加新 case：

```js
case 'CUSTOM_MSG':
  handleCustom(msg.data);
  break;
```

### 6.5 主题定制

修改 CSS `:root` 中的 CSS 变量即可一键切换主题色系。

---

## 7. 文件清单

| 文件              | 说明                        | 行数(约) |
| ----------------- | --------------------------- | -------- |
| `question.html` | 不良问题管理页              | ~310     |
| `phone.html`    | 不良手机管理页              | ~410     |
| `demo.html`     | Demo 展示页（含 mock 数据） | ~320     |
| `design.md`     | 原始需求文档                | —       |
| `design-doc.md` | 本设计文档                  | —       |

---

## 8. Bug 修复记录（框选功能）

### Bug #1：右侧空白无法触发框选
- **根因**：`.group-l1-header` / `.group-l2-header` 为 `display:flex` 块级元素，撑满整行宽度。点击右侧"空白"实际命中 header，被 mousedown 排除列表拦截。
- **修复**：移除 mousedown 中对 header 的排除；将「清空选中」从 mousedown 移至 mouseup（仅真正拖拽时清空，避免单击折叠标题时误清选中）。

### Bug #2：卡片右侧仍无法框选
- **根因**：`.card-grid`（flex-wrap 容器）同样撑满整行宽度，`e.target.closest('.card-grid')` 拦截了整行所有点击。
- **修复**：移除 card-grid 排除，接受拖拽滚动 card-grid 时会同时触发框选的轻微副作用。

### Bug #3：碰撞检测与选框坐标不一致（偏移 ~8px）
- **根因**：`selRect` 用 `position:absolute` 定位（相对于 padding-box），但 `getScrollRelativePos()` 和卡片位置换算用的是 border-box 坐标系，padding（8px）导致系统偏移。
- **修复**：碰撞检测改用 `getBoundingClientRect()` 获取 selRect 和卡片在同一视口坐标系中的位置直接比对。

### Bug #4：拖拽框选后结果被 click 事件清空
- **根因**：DOM 事件顺序 `mousedown → mousemove → mouseup → click`，mouseup 选中卡片后，click 处理器第 3 步（空白清空）将刚选中的全部清掉。拖拽超出 iframe 时 click 落在父页面，侥幸"成功"。
- **修复**：新增 `wasDragSelection` 标志位，mouseup 中真正拖拽（≥4px）时置 true，click 处理器检查并跳过清空。

### Bug #5：选框与鼠标位置仍有视觉偏差
- **根因**：无论怎么换算 `padding`/`scroll`/`border-box`，只要 selRect 用 `position:absolute` 相对 mainScroll 定位，就会引入亚像素累积误差。
- **修复（最终方案）**：
  - `selRect` 改为 `position: fixed`
  - 直接使用原始 `e.clientX/Y` 定位，**零坐标换算**
  - 碰撞检测统一用 `getBoundingClientRect()` 视口坐标系
  - 删除 `getScrollPad()`、`getScrollRelativePos()`、`cardPosInScroll()` 等所有手动换算函数

### 最终框选架构

```
mousedown  → 存 e.clientX/Y (原始值，不转换)
mousemove  → selRect (position:fixed).style.left = clientX
mouseup    → selRect.getBoundingClientRect()  ←→  card.getBoundingClientRect()
             ↑ 同一视口坐标系，浏览器原生计算，零误差 ↑
```

---

## 9. 待定 / 未来扩展

- [X] ~~水平滚动~~（问题页字段过多时，v1.1）
- [X] ~~分类内独立滚动~~（不良机页单项 max-height 限制，v1.1）
- [ ] 虚拟滚动（大量数据场景，替代全量 DOM 渲染）
- [ ] 列排序（通过消息通知父窗口进行服务端排序）
- [ ] `card-grid` 最大高度可通过 appConfig 配置（当前硬编码 280px）
- [ ] 消息来源 origin 白名单校验
- [ ] 不良模组页（第三个子页面，复用 phone 页模式）
