# SJDB — 简单关系查询优化器项目说明

> **Author:** 王欣然 Xinran Wang  

---

## 1. 项目定位

本项目实现了一个关系数据库查询优化器原型，主要目标是：

- **目录管理**：解析系统目录文件，读取每个关系（表）的总行数和各列的 distinct 值，构建 `Catalogue`、`NamedRelation`、`Attribute` 等对象模型。  
- **查询解析**：支持简化的 SQL-like 语法（`SELECT … FROM … WHERE …`），通过 `QueryParser` 将其转换为一棵左深的“Scan × … × Scan + Select + Project”Canonical 执行计划树。  
- **成本估计**（`Estimator`）：采用纯逻辑统计模型，遍历算子树，估算每个算子的输出行数和各列 distinct 值，用于后续优化决策。  
- **启发式重排**（`Optimiser`）：在保持左深树结构的前提下，执行  
  1. 谓词下推与列裁剪  
  2. 根表选择（按原始行数最小）  
  3. 贪婪连接顺序重排 + 一阶前瞻，避免局部最优陷阱  
  4. 剩余常量谓词统一挂顶层  
  5. 复原或省略最外层投影  
- **结果输出**（`Inspector`）：再次遍历优化后计划，打印每个算子的输入/输出统计，方便与原始计划对比验证。

---

## 2. 整体架构

项目核心模块及调用流程（纯文字描述）：

1. **CatalogueParser**  
   - 读取 `data/cat.txt`  
   - 按行解析“RelationName:Size:col1,distinct1:col2,distinct2…”  
   - 在 `Catalogue` 中创建对应的 `NamedRelation` 和 `Attribute`  

2. **QueryParser**  
   - 读取每条查询文件（`q1.txt`…`q5.txt`）的三行（`SELECT`、`FROM`、可选 `WHERE`）  
   - 先建一棵左深的笛卡尔积 `Scan×Scan×…`  
   - 再串联 `Select` 算子（按 `WHERE` 子句）  
   - 最后挂 `Project`（按 `SELECT` 列表）  
   - 生成 **Canonical** 执行计划树（`Operator` 子类的组合）

3. **Estimator**  
   - 实现 `PlanVisitor` 接口，多态 `visit(Scan/Project/Select/Product/Join)`  
   - 访问时根据算子类型更新 `Relation` 的 `tupleCount` 和各 `Attribute` `valueCount`  
   - 仅用逻辑统计公式，无 I/O 或 CPU 模型  

4. **Optimiser**  
   - 深度遍历（`collect`）收集所有 `Scan` 节点和 `Predicate`（分 `attr=attr` 与 `attr="value"`）  
   - 计算每张表最终需保留的列集（`computeNeed`）  
   - 对每个 `Scan` 下推常量谓词并消化、剔除仅在下推中出现的列，然后裁剪剩余必要列（`buildLeaves`）  
   - 选根表：按目录中原始行数最小者  
   - 多轮贪婪选取下一个表：  
     - 对每个候选右表构造 `Join` 或 `Product`  
     - 用 `Estimator` 得到本轮输出行数 `outRows`  
     - 若只剩最后一表，再模拟一次最终 join，得 `finalRows`  
     - 比较：1) `finalRows` 越小越优；2) 相等时倾向 `inProd`（输入积）更大；3) 再平手时 `outRows` 更小  
   - 挂剩余常量谓词  
   - 恢复最外层 `Project`，若已被内层裁剪覆盖则省略  

5. **Inspector**  
   - 同样实现 `PlanVisitor`  
   - 访问每个算子时打印其类型、输入/输出 `Relation.render()`  

---

## 3. 关键类详解

### 3.1 Estimator — 逻辑代价估计器

```java
public class Estimator implements PlanVisitor {
    public void visit(Scan op) { … }
    public void visit(Project op) { … }
    public void visit(Select op) { … }
    public void visit(Product op) { … }
    public void visit(Join op) { … }
}
```

#### Scan
- 复制底层 `NamedRelation` 的 `tupleCount`
- 复制所有 `Attribute`，保持原始 `valueCount`

#### Project
- 输出行数不变 (`T_out = T_in`)
- 只保留投影列，对应 `Attribute` 的 `valueCount = min(orig, T_out)`

#### Select
- **常量谓词** (`attr="value"`)：
  - `T_out = max(1, T_in / V(attr))`
  - 该列 `valueCount = 1`
  - 其他列 `min(orig, T_out)`
- **等值谓词** (`attr1=attr2`)：
  - `T_out = max(1, T_in / max(V1, V2))`
  - 两端连接列各自 `valueCount = min(V1, V2, T_out)`
  - 其余列 `min(orig, T_out)`

#### Product
- `T_out = T_left × T_right`
- 属性并集，所有列 `valueCount = min(orig, T_out)`

#### Join
- `T_out = max(1, T_left×T_right / max(Vl, Vr))`
- 连接列 `valueCount = min(Vl, Vr, T_out)`
- 其他列 `min(orig, T_out)`

### 3.2 Optimiser — 启发式连接重排器
```java
public class Optimiser {
    public Operator optimise(Operator canonical) { … }
    // 私有方法：collect, computeNeed, buildLeaves, pickRoot, chooseNext...
}
```
1. **拆顶层 Project**  
   暂存投影列，便于内部裁剪后再恢复或省略冗余。  
2. **收集扫描与谓词** (`collect`)  
   DFS 遍历算子树，将 `Scan` 节点与 `Predicate` 分门别类存入 `Info`。  
3. **计算列需求** (`computeNeed`)  
   汇总顶层投影列 + 等值谓词的左右列 + 常量谓词的左列，构建每张表的“最终所需属性集”。  
4. **构造叶子** (`buildLeaves`)  
   对每个 `Scan`：  
   - 下推常量谓词，并记录“只在此处使用”的列  
   - 剔除冗余：若某列仅在下推谓词中出现、后续无用，则从 `need` 中移除  
   - 裁剪列：若剩余 `need` 列少于原表列，插入 `Project` 保留必要列  
5. **选根表** (`pickRoot`)  
   直接根据目录中原始行数最小，防止被已下推的常量选择误导  
6. **贪婪 + 一阶前瞻** (`chooseNext`)  
   每轮对剩余表：  
   - 构造 `Join` 或 `Product` 候选计划  
   - 估算本轮输出 `outRows`  
   - 若只剩最后一表，再模拟一次最终 `join` 得到 `finalRows`  
   - 比较：  
     1. `finalRows` 越小越优  
     2. 相等时倾向 `inProd`（左行数×右行数）更大  
     3. 再相等时 `outRows` 更小  
7. **挂剩余常量谓词 + 恢复顶层投影**  
   未消化的 `attr="value"` 串联于树顶  
   若最外层投影已由内层裁剪满足，则省略该层

---

### 4. 设计模式与递归策略
- **Visitor**  
  将遍历逻辑封装在 `Operator.accept(PlanVisitor)`，具体操作由不同的 `PlanVisitor`（`Estimator`、`Inspector`）实现  
- **深度优先递归**  
  每个 `accept` 先递归子算子，再调用 `visitor.visit(this)`  
- **左深树约束**  
  将连接枚举限制在“左子树 × 新表”形式，降低枚举复杂度  
- **贪婪 + 前瞻**  
  结合局部贪心与一阶 look-ahead，平衡效率与效果  

### 5. 示例：q5.txt

**表结构示例**  
```text
Person:400:persid,400:persname,350:age,47  
Project:40:projid,40:projname,35:dept,5  
Department:5:deptid,5:deptname,5:manager,5  
格式：表名:行数:列名1,distinct1:列名2,distinct2:…
```

含义举例：

Person:400:persid,400:persname,350:age,47

- Person 表共 400 条记录  
- 属性 persid 有 400 个不同值  
- 属性 persname 有 350 个不同值  
- 属性 age 有 47 个不同值

查询示例

```sql
SELECT projname, deptname
FROM Person, Project, Department
WHERE persid=manager, dept=deptid, persname="Smith"
```

Canonical 执行计划（未优化）

```ruby
-- canonical --
Person
  in:  Person:400:persid,400:persname,350:age,47
  out: 400:persid,400:persname,350:age,47
Project
  in:  Project:40:projid,40:projname,35:dept,5
  out: 40:projid,40:projname,35:dept,5
(Person) TIMES (Project)
  inl: 400:persid,400:persname,350:age,47
  inr: 40:projid,40:projname,35:dept,5
  out: 16000:persid,400:persname,350:age,47:projid,40:projname,35:dept,5
Department
  in:  Department:5:deptid,5:deptname,5:manager,5
  out: 5:deptid,5:deptname,5:manager,5
((Person) TIMES (Project)) TIMES (Department)
  inl: 16000:…
  inr: 5:…
  out: 80000:…
SELECT [persid=manager] …
SELECT [dept=deptid] …
SELECT [persname="Smith"] …
PROJECT [projname,deptname] …
```

Join-rewritten 执行计划（优化后）

```ruby
-- join-rewritten --
Department
  in:  Department:5:deptid,5:deptname,5:manager,5
  out: 5:deptid,5:deptname,5:manager,5
Project
  in:  Project:40:projid,40:projname,35:dept,5
  out: 40:projid,40:projname,35:dept,5
PROJECT [projname,dept] (Project)
…
(Department) JOIN [dept=deptid] (…)
Person
…
((Department) JOIN …) JOIN [persid=manager] (…)
PROJECT [projname,deptname] (…)
```

简单说明

在 Canonical 阶段，三张表直接做笛卡尔积，效率低且中间结果巨大。

在 Join-rewritten 阶段，优化器先下推常量谓词 persname="Smith"并下推投影（过滤列）再执行join Project ▷◁ Department（利用 dept=deptid），再join Person（利用 persid=manager），显著减少中间行数。



