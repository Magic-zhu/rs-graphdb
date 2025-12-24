/// 扩展版 Cypher AST

#[derive(Debug, Clone, PartialEq)]
pub enum CypherStatement {
    Query(CypherQuery),
    Create(CreateClause),
    Delete(DeleteStatement),
    Set(SetStatement),
    // 后续可扩展 Merge
}

#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    pub match_clause: Option<MatchClause>,
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
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Property(String, String), // var.prop
    Literal(PropertyValue),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub order_by: Option<OrderBy>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Variable(String),
    Aggregation(AggFunc, String, String), // func, var, prop
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
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
