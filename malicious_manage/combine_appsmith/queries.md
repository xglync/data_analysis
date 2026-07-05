# Appsmith Query 定义

> 以下 Query 需在 Appsmith 中逐一创建，类型说明及 SQL / API 示例见各条目。

---

## 1. fetch_questions

| 属性 | 值 |
|------|-----|
| **名称** | `fetch_questions` |
| **类型** | SQL / REST API |
| **说明** | 获取全量不良问题数据 |

### SQL 示例
```sql
SELECT
  id,
  type          AS type,
  problem       AS problem,
  level         AS level,
  station       AS station,
  status        AS status,
  create_time   AS create_time,
  incharge      AS incharge
FROM problem_table
ORDER BY create_time DESC;
```

### REST API 示例
```
GET {{base_url}}/api/problems
Headers: { Authorization: "Bearer {{appsmith.store.token}}" }
```

**返回字段**（需与 combine.html 中 `QUESTION_CFG.fields[].key` 一致）：
`id | type | problem | level | station | status | create_time | incharge`

---

## 2. fetch_phones

| 属性 | 值 |
|------|-----|
| **名称** | `fetch_phones` |
| **类型** | SQL / REST API |
| **说明** | 获取全量不良手机数据 |

### SQL 示例
```sql
SELECT
  id,
  barcode       AS barcode,
  model         AS model,
  station       AS station,
  remark        AS remark,
  analysis      AS analysis,
  problem_id    AS problem_id,
  status        AS status,
  line          AS line,
  operator      AS operator,
  entry_time    AS entry_time
FROM phone_table
ORDER BY entry_time DESC;
```

### REST API 示例
```
GET {{base_url}}/api/phones
```

**返回字段**：
`id | barcode | model | station | remark | analysis | problem_id | status | line | operator | entry_time`

---

## 3. fetch_phones_by_problem

| 属性 | 值 |
|------|-----|
| **名称** | `fetch_phones_by_problem` |
| **类型** | SQL / REST API |
| **参数** | `problem_id: number` |
| **说明** | 获取指定问题关联的不良手机（双击问题行触发） |

### SQL 示例
```sql
SELECT
  id, barcode, model, station, remark, analysis,
  problem_id, status, line, operator, entry_time
FROM phone_table
WHERE problem_id = {{this.params.problem_id}}
ORDER BY entry_time DESC;
```

### REST API 示例
```
GET {{base_url}}/api/phones?problem_id={{this.params.problem_id}}
```

---

## 4. delete_question（可选）

| 属性 | 值 |
|------|-----|
| **名称** | `delete_question` |
| **类型** | SQL |
| **参数** | `ids: number[]` |

```sql
DELETE FROM problem_table WHERE id = ANY({{this.params.ids}});
```

---

## 5. batch_repair_phones（可选）

| 属性 | 值 |
|------|-----|
| **名称** | `batch_repair_phones` |
| **参数** | `ids: number[]` |

```sql
UPDATE phone_table SET status = '已维修' WHERE id = ANY({{this.params.ids}});
```

---

## 6. batch_scrap_phones（可选）

```sql
UPDATE phone_table SET status = '已报废' WHERE id = ANY({{this.params.ids}});
```

---

## 7. assign_phones_to_question（可选，拖拽关联）

```sql
UPDATE phone_table SET problem_id = {{this.params.questionId}}
WHERE id = ANY({{this.params.phoneIds}});
```

---

## Appsmith 配置要点

1. **Query 命名**：必须与 JSObject 中 `fetch_xxx.run()` 的调用名完全一致
2. **返回格式**：Query 的 `run()` 返回数据需是数组，或 `{ data: [] }` 结构（JSObject 已做兼容）
3. **Iframe 组件名**：`JSObject_Messenger.js` 中的 `_iframeSelector` 需与实际 Iframe 组件的 `name` 属性匹配
4. **Iframe src**：可用 Appsmith 的 FilePicker 上传 `combine.html`，或部署到静态服务器后设置 URL；URL 需携带 `#id=appsmith_main`
