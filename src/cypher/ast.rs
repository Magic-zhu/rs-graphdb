/// 扩展版 Cypher AST

#[derive(Debug, Clone, PartialEq)]
pub enum CypherStatement {
    Query(CypherQuery),
    Create(CreateClause),
    Delete(DeleteStatement),
    Set(SetStatement),
    Merge(MergeStatement),
    Foreach(ForeachStatement),  // FOREACH 语句
    Call(CallStatement),        // CALL 子查询
    Union(UnionStatement),      // UNION ALL 语句
    BeginTransaction,           // BEGIN 语句
    CommitTransaction,          // COMMIT 语句
    RollbackTransaction,        // ROLLBACK 语句
}

#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    pub match_clause: Option<MatchClause>,
    pub with_clause: Option<WithClause>,  // WITH 子句
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub start_node: NodePattern,
    pub relationships: Vec<(RelPattern, NodePattern)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub var: Option<String>,
    pub label: Option<String>,
    pub props: Vec<(String, PropertyValue)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelPattern {
    pub var: Option<String>,
    pub rel_type: Option<String>,
    pub direction: Direction,
    /// 可变长度路径：Some((min_hops, max_hops))
    /// 例如 *2..3 表示最小2跳，最大3跳
    /// * 表示 1..∞
    /// *3 表示 1..3
    pub var_length: Option<(Option<usize>, Option<usize>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    Outgoing, // ->
    Incoming, // <-
    Both,     // -
}

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    String(String),
    Int(i64),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Eq(Expression, Expression),
    Gt(Expression, Expression),
    Lt(Expression, Expression),
    Gte(Expression, Expression),  // >=
    Lte(Expression, Expression),  // <=
    Ne(Expression, Expression),   // <> 或 !=
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    RegexMatch(Expression, String),  // =~ 正则匹配
    Exists(String, String),           // EXISTS(var.prop)
    IsNull(Expression),               // IS NULL
    IsNotNull(Expression),            // IS NOT NULL
    In(Expression, Vec<Expression>),  // IN [v1, v2, ...]
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Property(String, String), // var.prop
    Literal(PropertyValue),
    List(Vec<Expression>),   // 列表字面量 [v1, v2, ...]
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub order_by: Option<OrderBy>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
    pub group_by: Option<Vec<String>>,  // 新增：GROUP BY 变量列表
}

/// WITH 子句：传递中间结果
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,  // 类似 RETURN 的 items
    pub where_clause: Option<WhereClause>,  // WITH 后可选的 WHERE
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Variable(String),
    VariableAs(String, String),  // var AS alias
    Property(String, String),  // var.prop
    PropertyAs(String, String, String),  // var.prop AS alias
    Aggregation(AggFunc, String, String), // func, var, prop
    AggregationAs(AggFunc, String, String, String), // func, var, prop, alias
    AggregationWithParam(AggFunc, String, String, f64), // func, var, prop, param (for percentile)
    AggregationWithParamAs(AggFunc, String, String, f64, String), // func, var, prop, param, alias
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Collect,      // COLLECT 聚合
    StDev,        // 标准差
    PercentileCont,  // 连续百分位数
    PercentileDisc,  // 离散百分位数
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub items: Vec<OrderByItem>,  // 支持多字段排序
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub var: String,
    pub prop: String,
    pub ascending: bool,
}

/// 非常简化版的 CREATE：只支持一条链式 pattern
#[derive(Debug, Clone, PartialEq)]
pub struct CreateClause {
    pub pattern: Pattern,
}

/// DELETE 语句：MATCH ... DELETE var
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub variables: Vec<String>, // 要删除的变量名
}

/// SET 语句：MATCH ... SET var.prop = value
#[derive(Debug, Clone, PartialEq)]
pub struct SetStatement {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub assignments: Vec<SetAssignment>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetAssignment {
    pub var: String,
    pub prop: String,
    pub value: PropertyValue,
}

/// MERGE 语句：匹配则更新，不匹配则创建
#[derive(Debug, Clone, PartialEq)]
pub struct MergeStatement {
    pub pattern: Pattern,
    pub on_create: Option<Vec<SetAssignment>>, // 仅在创建时执行
    pub on_match: Option<Vec<SetAssignment>>,  // 仅在匹配时执行
}

/// FOREACH 语句：遍历列表并执行更新操作
#[derive(Debug, Clone, PartialEq)]
pub struct ForeachStatement {
    pub variable: String,           // 循环变量名
    pub list_expr: Expression,      // 列表表达式
    pub updates: Vec<SetAssignment>, // 要执行的 SET 操作
}

/// CALL 子查询：执行内联子查询并传递变量
/// 语法：
///   - CALL { <subquery> }
///   - CALL { <subquery> } IN (input_vars)
///   - CALL { WITH ... MATCH ... RETURN ... }
#[derive(Debug, Clone, PartialEq)]
pub struct CallStatement {
    pub inner_query: CypherQuery,      // 内层子查询
    pub outer_query: CypherQuery,      // 外层查询
    pub input_vars: Vec<String>,       // 从外层传入内层的变量名（IN 子句）
    pub with_returns: Vec<ReturnItem>, // 子查询返回给外层的变量
}

/// UNION ALL 语句：合并多个查询的结果
/// 语法：MATCH ... RETURN ... UNION ALL MATCH ... RETURN ...
#[derive(Debug, Clone, PartialEq)]
pub struct UnionStatement {
    pub left: CypherQuery,   // 左侧查询
    pub right: CypherQuery,  // 右侧查询
    pub all: bool,           // true = UNION ALL, false = UNION (去重)
}

