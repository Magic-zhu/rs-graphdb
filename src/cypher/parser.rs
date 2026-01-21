use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{alpha1, alphanumeric1, char, digit1, multispace0},
    number::complete::double,
    combinator::{map, opt, peek, recognize},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};

use super::ast::*;

fn ws<'a, F, O>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    delimited(multispace0, inner, multispace0)
}

fn identifier(input: &str) -> IResult<&str, String> {
    map(
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        )),
        |s: &str| s.to_string(),
    )(input)
}

fn string_literal(input: &str) -> IResult<&str, String> {
    alt((
        // 双引号字符串
        map(
            delimited(char('"'), take_while1(|c| c != '"'), char('"')),
            |s: &str| s.to_string()
        ),
        // 单引号字符串
        map(
            delimited(char('\''), take_while1(|c| c != '\''), char('\'')),
            |s: &str| s.to_string()
        )
    ))(input)
}

fn int_literal(input: &str) -> IResult<&str, i64> {
    map(digit1, |s: &str| s.parse().unwrap())(input)
}

fn property_value(input: &str) -> IResult<&str, PropertyValue> {
    alt((
        map(string_literal, PropertyValue::String),
        map(int_literal, PropertyValue::Int),
        map(identifier, PropertyValue::Variable),
    ))(input)
}

fn property(input: &str) -> IResult<&str, (String, PropertyValue)> {
    let (input, key) = ws(identifier)(input)?;
    let (input, _) = ws(char(':'))(input)?;
    let (input, val) = ws(property_value)(input)?;
    Ok((input, (key, val)))
}

fn properties(input: &str) -> IResult<&str, Vec<(String, PropertyValue)>> {
    delimited(
        ws(char('{')),
        separated_list0(ws(char(',')), property),
        ws(char('}')),
    )(input)
}

fn node_pattern(input: &str) -> IResult<&str, NodePattern> {
    let (input, _) = ws(char('('))(input)?;
    let (input, var) = opt(ws(identifier))(input)?;
    let (input, label) = opt(preceded(ws(char(':')), ws(identifier)))(input)?;
    let (input, props) = opt(ws(properties))(input)?;
    let (input, _) = ws(char(')'))(input)?;

    Ok((
        input,
        NodePattern {
            var,
            label,
            props: props.unwrap_or_default(),
        },
    ))
}

fn rel_pattern(input: &str) -> IResult<&str, RelPattern> {
    alt((
        // Outgoing: -[r:TYPE*2..3]-> or -[:TYPE*]-> or -[r*2..3]-> or -[]-> or -[:TYPE*2..]->
        map(
            tuple((
                ws(tag("-[")),
                opt(ws(identifier)),  // variable name
                opt(preceded(ws(char(':')), ws(identifier))),  // relationship type
                opt(variable_length_spec),  // 可变长度 *min..max
                ws(tag("]->")),
            )),
            |(_, var, rel_type, var_length, _)| RelPattern {
                var,
                rel_type,
                direction: Direction::Outgoing,
                var_length,
            },
        ),
        // Incoming: <-[r:TYPE*2..3]- or <-[:TYPE*]- or <-[r*2..3]- or <-[]-
        map(
            tuple((
                ws(tag("<-")),
                opt(delimited(ws(char('[')), tuple((
                    opt(ws(identifier)),  // variable name
                    opt(preceded(ws(char(':')), ws(identifier))),  // relationship type
                    opt(variable_length_spec),  // 可变长度 *min..max
                )), ws(char(']')))),
                ws(tag("-")),
            )),
            |(_, var_rel, _)| {
                let (var, rel_type, var_length) = match var_rel {
                    Some((v, rt, vl)) => (v, rt, vl),
                    None => (None, None, None),
                };
                RelPattern {
                    var,
                    rel_type,
                    direction: Direction::Incoming,
                    var_length,
                }
            },
        ),
        // Both: -[r:TYPE*2..3]- or -[:TYPE*]- or -[r*2..3]- or -[]-
        map(
            tuple((
                ws(tag("-[")),
                opt(ws(identifier)),  // variable name
                opt(preceded(ws(char(':')), ws(identifier))),  // relationship type
                opt(variable_length_spec),  // 可变长度 *min..max
                ws(tag("]-")),
            )),
            |(_, var, rel_type, var_length, _)| RelPattern {
                var,
                rel_type,
                direction: Direction::Both,
                var_length,
            },
        ),
    ))(input)
}

/// 解析可变长度路径规格
/// *2..3 -> Some((Some(2), Some(3)))
/// *3 -> Some((Some(1), Some(3)))
/// *..3 -> Some((None, Some(3)))
/// *2.. -> Some((Some(2), None))
/// * -> Some((Some(1), None))  // 默认 1..∞
fn variable_length_spec(input: &str) -> IResult<&str, (Option<usize>, Option<usize>)> {
    let (input, _) = ws(char('*'))(input)?;
    alt((
        // *min..max
        map(
            tuple((
                opt(ws(map(digit1, |s: &str| s.parse().unwrap()))),
                ws(tag("..")),
                opt(ws(map(digit1, |s: &str| s.parse().unwrap()))),
            )),
            |(min, _, max)| (min, max),
        ),
        // 只有一个数字：*n 等价于 *1..n
        map(ws(digit1), |n: &str| (Some(1), Some(n.parse().unwrap()))),
        // 只有 * 等价于 *1..∞
        // 使用 peek 检查但不消耗任何字符，然后返回默认值
        map(peek(tag("")), |_| (Some(1), None)),
    ))(input)
}

fn pattern(input: &str) -> IResult<&str, Pattern> {
    let (input, start) = node_pattern(input)?;
    let (input, rels) = many0(pair(rel_pattern, node_pattern))(input)?;

    Ok((
        input,
        Pattern {
            start_node: start,
            relationships: rels,
        },
    ))
}

fn match_clause(input: &str) -> IResult<&str, MatchClause> {
    let (input, optional) = opt(ws(tag_no_case("OPTIONAL")))(input)?;
    let (input, _) = ws(tag_no_case("MATCH"))(input)?;
    let (input, pat) = ws(pattern)(input)?;
    Ok((
        input,
        MatchClause {
            pattern: pat,
            optional: optional.is_some(),
        },
    ))
}

// WHERE clause parsing
fn expression(input: &str) -> IResult<&str, Expression> {
    alt((
        map(
            tuple((ws(identifier), ws(char('.')), ws(identifier))),
            |(var, _, prop)| Expression::Property(var, prop),
        ),
        map(property_value, Expression::Literal),
        list_literal,  // 列表字面量
    ))(input)
}

// 列表字面量 [v1, v2, ...]
fn list_literal(input: &str) -> IResult<&str, Expression> {
    delimited(
        ws(char('[')),
        map(
            separated_list0(ws(char(',')), map(property_value, Expression::Literal)),
            Expression::List
        ),
        ws(char(']'))
    )(input)
}

// 解析带括号的分组条件
fn parenthesized_condition(input: &str) -> IResult<&str, Condition> {
    delimited(
        ws(char('(')),
        or_condition,
        ws(char(')'))
    )(input)
}

// 解析基础条件（不包含 AND/OR）
fn base_condition(input: &str) -> IResult<&str, Condition> {
    alt((
        parenthesized_condition,
        exists_condition,
        // 所有二元操作条件
        |input| {
            let (input, left) = expression(input)?;
            binary_op_condition(input, left)
        }
    ))(input)
}

// 解析二元操作条件（在左表达式已解析的情况下）
fn binary_op_condition(input: &str, left: Expression) -> IResult<&str, Condition> {
    // 首先跳过空格
    let (input, _) = multispace0(input)?;

    // 检查下一个token来决定使用哪个解析器
    // 注意：关键词操作符（IN, IS）必须使用 ws 分隔，符号操作符直接匹配
    // 先检查符号操作符
    let sym_result = opt(ws(alt((
        tag("=~"),
        tag("<>"),
        tag(">="),
        tag("<="),
        tag("="),
        tag(">"),
        tag("<"),
    ))))(input);

    if let Ok((input_rest, Some(op))) = sym_result {
        // 符号操作符匹配成功
        let (input, right) = expression(input_rest)?;

        let cond = match op {
            "=~" => {
                // 正则表达式需要字符串字面量
                match right {
                    Expression::Literal(PropertyValue::String(pattern)) => {
                        return Ok((input, Condition::RegexMatch(left, pattern)))
                    }
                    _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
                }
            }
            "=" => Condition::Eq(left, right),
            ">" => Condition::Gt(left, right),
            "<" => Condition::Lt(left, right),
            ">=" => Condition::Gte(left, right),
            "<=" => Condition::Lte(left, right),
            "<>" => Condition::Ne(left, right),
            _ => unreachable!(),
        };

        return Ok((input, cond));
    }

    // 检查 IN 操作符（需要空格分隔）
    if let Ok((input_rest, _)) = ws(tag_no_case("IN"))(input) {
        let (input, list_expr) = ws(list_literal)(input_rest)?;
        match list_expr {
            Expression::List(items) => return Ok((input, Condition::In(left, items))),
            _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
        }
    }

    // 检查 IS NULL / IS NOT NULL
    if let Ok((input_rest, _)) = ws(tag_no_case("IS"))(input) {
        let (input_rest, not) = opt(ws(tag_no_case("NOT")))(input_rest)?;
        let (input, _) = ws(tag_no_case("NULL"))(input_rest)?;
        if not.is_some() {
            return Ok((input, Condition::IsNotNull(left)))
        } else {
            return Ok((input, Condition::IsNull(left)))
        }
    }

    // 没有匹配的操作符
    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
}

// EXISTS(var.prop)
fn exists_condition(input: &str) -> IResult<&str, Condition> {
    let (input, _) = ws(tag_no_case("EXISTS"))(input)?;
    let (input, _) = ws(char('('))(input)?;
    let (input, var) = ws(identifier)(input)?;
    let (input, _) = ws(char('.'))(input)?;
    let (input, prop) = ws(identifier)(input)?;
    let (input, _) = ws(char(')'))(input)?;
    Ok((input, Condition::Exists(var, prop)))
}

// 解析 OR 条件（最低优先级）
fn or_condition(input: &str) -> IResult<&str, Condition> {
    let (mut input, mut left) = and_condition(input)?;

    while let Ok((input_next, _)) = ws(tag_no_case("OR"))(input) {
        let (input_next, right) = and_condition(input_next)?;
        left = Condition::Or(Box::new(left), Box::new(right));
        input = input_next;
    }

    Ok((input, left))
}

// 解析 AND 条件（中等优先级）
fn and_condition(input: &str) -> IResult<&str, Condition> {
    let (mut input, mut left) = base_condition(input)?;

    while let Ok((input_next, _)) = ws(tag_no_case("AND"))(input) {
        let (input_next, right) = base_condition(input_next)?;
        left = Condition::And(Box::new(left), Box::new(right));
        input = input_next;
    }

    Ok((input, left))
}

// 主条件解析入口
fn condition(input: &str) -> IResult<&str, Condition> {
    or_condition(input)
}

fn where_clause(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = ws(tag_no_case("WHERE"))(input)?;
    let (input, cond) = condition(input)?;
    // 将单个条件包装成 Vec，保持向后兼容
    Ok((input, WhereClause { conditions: vec![cond] }))
}

// RETURN clause parsing
fn agg_func(input: &str) -> IResult<&str, AggFunc> {
    alt((
        map(tag_no_case("SUM"), |_| AggFunc::Sum),
        map(tag_no_case("AVG"), |_| AggFunc::Avg),
        map(tag_no_case("MIN"), |_| AggFunc::Min),
        map(tag_no_case("MAX"), |_| AggFunc::Max),
        map(tag_no_case("COLLECT"), |_| AggFunc::Collect),
        map(tag_no_case("COUNT"), |_| AggFunc::Count),
        map(tag_no_case("STDEV"), |_| AggFunc::StDev),
        map(tag_no_case("STDEVP"), |_| AggFunc::StDev),
        map(tag_no_case("PERCENTILECONT"), |_| AggFunc::PercentileCont),
        map(tag_no_case("PERCENTILEDISC"), |_| AggFunc::PercentileDisc),
    ))(input)
}

// 解析百分位数参数: 0.5
fn parse_percentile_param(input: &str) -> IResult<&str, f64> {
    ws(double)(input)
}

// 解析百分位数聚合函数: PERCENTILECONT(n.prop, 0.5)
fn percentile_agg(input: &str) -> IResult<&str, ReturnItem> {
    map(
        tuple((
            alt((
                map(tag_no_case("PERCENTILECONT"), |_| AggFunc::PercentileCont),
                map(tag_no_case("PERCENTILEDISC"), |_| AggFunc::PercentileDisc),
            )),
            ws(char('(')),
            ws(identifier),
            ws(char('.')),
            ws(identifier),
            ws(char(',')),
            parse_percentile_param,
            ws(char(')')),
            opt(preceded(ws(tag_no_case("AS")), ws(identifier))),
        )),
        |(func, _, var, _, prop, _, param, _, alias)| {
            if let Some(a) = alias {
                ReturnItem::AggregationWithParamAs(func, var, prop, param, a)
            } else {
                ReturnItem::AggregationWithParam(func, var, prop, param)
            }
        },
    )(input)
}

fn return_item(input: &str) -> IResult<&str, ReturnItem> {
    alt((
        map(tag_no_case("COUNT(*)"), |_| ReturnItem::Count),
        // Percentile functions: PERCENTILECONT(n.prop, 0.5)
        percentile_agg,
        // Aggregation: COUNT(u) or COUNT(n.prop)
        map(
            tuple((
                agg_func,
                ws(char('(')),
                ws(identifier),
                // 可选的 .prop 部分
                opt(preceded(ws(char('.')), ws(identifier))),
                ws(char(')')),
                opt(preceded(ws(tag_no_case("AS")), ws(identifier))),
            )),
            |(func, _, var, prop, _, alias)| {
                let prop_str = prop.unwrap_or_default();
                if let Some(a) = alias {
                    ReturnItem::AggregationAs(func, var, prop_str.clone(), a)
                } else {
                    ReturnItem::Aggregation(func, var, prop_str)
                }
            },
        ),
        // var.prop AS alias
        map(
            tuple((
                identifier,
                ws(char('.')),
                identifier,
                ws(tag_no_case("AS")),
                ws(identifier),
            )),
            |(var, _, prop, _, alias)| ReturnItem::PropertyAs(var, prop, alias),
        ),
        // var.prop
        map(
            tuple((
                identifier,
                ws(char('.')),
                identifier,
            )),
            |(var, _, prop)| ReturnItem::Property(var, prop),
        ),
        // var AS alias
        map(
            tuple((
                identifier,
                ws(tag_no_case("AS")),
                ws(identifier),
            )),
            |(var, _, alias)| ReturnItem::VariableAs(var, alias),
        ),
        // Simple variable
        map(identifier, ReturnItem::Variable),
    ))(input)
}

fn func_str(func: &AggFunc) -> &str {
    match func {
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::Count => "count",
        AggFunc::Collect => "collect",
        AggFunc::StDev => "stdev",
        AggFunc::PercentileCont => "percentileCont",
        AggFunc::PercentileDisc => "percentileDisc",
    }
}

fn order_by_item(input: &str) -> IResult<&str, OrderByItem> {
    // 匹配 COUNT(*)
    fn parse_count_star(input: &str) -> IResult<&str, (String, String)> {
        let (input, _) = tag_no_case("COUNT")(input)?;
        let (input, _) = ws(char('('))(input)?;
        let (input, _) = ws(char('*'))(input)?;
        let (input, _) = ws(char(')'))(input)?;
        Ok((input, ("count".to_string(), "*".to_string())))
    }

    // 匹配聚合函数如 MIN(u.age)
    fn parse_agg_func_order(input: &str) -> IResult<&str, (String, String)> {
        let (input, func) = agg_func(input)?;
        let (input, _) = ws(char('('))(input)?;
        let (input, var) = ws(identifier)(input)?;
        let (input, prop) = opt(preceded(ws(char('.')), ws(identifier)))(input)?;
        let (input, _) = ws(char(')'))(input)?;

        let prop = prop.unwrap_or_else(|| String::new());
        let var_prop = if prop.is_empty() {
            format!("{}({})", func_str(&func), var)
        } else {
            format!("{}({}.{})", func_str(&func), var, prop)
        };

        Ok((input, (var_prop, String::new())))
    }

    // 匹配 var.prop
    fn parse_var_prop(input: &str) -> IResult<&str, (String, String)> {
        let (input, var) = ws(identifier)(input)?;
        let (input, _) = ws(char('.'))(input)?;
        let (input, prop) = ws(identifier)(input)?;
        Ok((input, (var, prop)))
    }

    // 匹配简单变量
    fn parse_simple_var(input: &str) -> IResult<&str, (String, String)> {
        let (input, var) = ws(identifier)(input)?;
        Ok((input, (var, String::new())))
    }

    let (input, (var, prop)) = alt((
        parse_count_star,
        parse_agg_func_order,
        parse_var_prop,
        parse_simple_var,
    ))(input)?;

    let (input, dir) = opt(ws(alt((tag_no_case("ASC"), tag_no_case("DESC")))))(input)?;
    let ascending = dir.map(|d| d.to_uppercase() != "DESC").unwrap_or(true);

    Ok((
        input,
        OrderByItem {
            var,
            prop,
            ascending,
        },
    ))
}

fn order_by(input: &str) -> IResult<&str, OrderBy> {
    let (input, _) = ws(tag_no_case("ORDER BY"))(input)?;
    let (input, items) = separated_list1(ws(char(',')), order_by_item)(input)?;

    Ok((input, OrderBy { items }))
}

fn return_clause(input: &str) -> IResult<&str, ReturnClause> {
    let (input, _) = ws(tag_no_case("RETURN"))(input)?;
    let (input, items) = separated_list1(ws(char(',')), ws(return_item))(input)?;

    // 尝试解析 GROUP BY (在 ORDER BY 之前)
    let (input, group_by) = opt(parse_group_by)(input)?;

    let (input, order) = opt(order_by)(input)?;
    let (input, skip_val) = opt(preceded(
        ws(tag_no_case("SKIP")),
        map(ws(digit1), |s: &str| s.parse().unwrap()),
    ))(input)?;
    let (input, limit_val) = opt(preceded(
        ws(tag_no_case("LIMIT")),
        map(ws(digit1), |s: &str| s.parse().unwrap()),
    ))(input)?;

    Ok((
        input,
        ReturnClause {
            items,
            order_by: order,
            skip: skip_val,
            limit: limit_val,
            group_by,
        },
    ))
}

/// 解析 GROUP BY 子句
/// 支持两种格式：
/// 1. 简单变量：GROUP BY u
/// 2. 属性路径：GROUP BY u.city, u.age
fn parse_group_by(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = ws(tag_no_case("GROUP"))(input)?;
    let (input, _) = ws(tag_no_case("BY"))(input)?;

    // 解析属性路径 var.prop
    fn parse_prop_path(input: &str) -> IResult<&str, String> {
        let (input, var) = ws(identifier)(input)?;
        let (input, _) = ws(char('.'))(input)?;
        let (input, prop) = ws(identifier)(input)?;
        Ok((input, format!("{}.{}", var, prop)))
    }

    // 解析 GROUP BY 项，可以是 var 或 var.prop
    fn parse_group_by_item(input: &str) -> IResult<&str, String> {
        alt((
            // 先尝试 var.prop 格式
            parse_prop_path,
            // 回退到简单变量
            map(identifier, |v| v),
        ))(input)
    }

    let (input, vars) = separated_list1(ws(char(',')), parse_group_by_item)(input)?;
    Ok((input, vars))
}

fn with_clause(input: &str) -> IResult<&str, WithClause> {
    let (input, _) = ws(tag_no_case("WITH"))(input)?;
    let (input, items) = separated_list1(ws(char(',')), ws(return_item))(input)?;
    let (input, where_c) = opt(where_clause)(input)?;

    Ok((input, WithClause {
        items,
        where_clause: where_c,
    }))
}

fn create_clause(input: &str) -> IResult<&str, CreateClause> {
    let (input, _) = ws(tag_no_case("CREATE"))(input)?;
    let (input, pat) = ws(pattern)(input)?;
    Ok((input, CreateClause { pattern: pat }))
}

fn delete_statement(input: &str) -> IResult<&str, DeleteStatement> {
    let (input, match_c) = match_clause(input)?;
    let (input, where_c) = opt(where_clause)(input)?;
    let (input, _) = ws(tag_no_case("DELETE"))(input)?;
    let (input, vars) = separated_list1(ws(char(',')), ws(identifier))(input)?;

    Ok((
        input,
        DeleteStatement {
            match_clause: match_c,
            where_clause: where_c,
            variables: vars,
        },
    ))
}

fn set_assignment(input: &str) -> IResult<&str, SetAssignment> {
    let (input, var) = ws(identifier)(input)?;
    let (input, _) = ws(char('.'))(input)?;
    let (input, prop) = ws(identifier)(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, val) = ws(property_value)(input)?;

    Ok((
        input,
        SetAssignment {
            var,
            prop,
            value: val,
        },
    ))
}

fn set_statement(input: &str) -> IResult<&str, SetStatement> {
    let (input, match_c) = match_clause(input)?;
    let (input, where_c) = opt(where_clause)(input)?;
    let (input, _) = ws(tag_no_case("SET"))(input)?;
    let (input, assignments) = separated_list1(ws(char(',')), set_assignment)(input)?;

    Ok((
        input,
        SetStatement {
            match_clause: match_c,
            where_clause: where_c,
            assignments,
        },
    ))
}

// MERGE 语句解析
fn merge_statement(input: &str) -> IResult<&str, MergeStatement> {
    let (input, _) = ws(tag_no_case("MERGE"))(input)?;
    let (input, pattern) = ws(pattern)(input)?;

    // 解析可选的 ON CREATE SET 子句
    let on_create = opt(preceded(
        ws(tag_no_case("ON CREATE")),
        preceded(
            ws(tag_no_case("SET")),
            separated_list1(ws(char(',')), set_assignment)
        )
    ))(input)?;

    let (input, on_create) = match on_create {
        (input, Some(assignments)) => (input, Some(assignments)),
        (input, None) => (input, None),
    };

    // 解析可选的 ON MATCH SET 子句
    let on_match = opt(preceded(
        ws(tag_no_case("ON MATCH")),
        preceded(
            ws(tag_no_case("SET")),
            separated_list1(ws(char(',')), set_assignment)
        )
    ))(input)?;

    let (input, on_match) = match on_match {
        (input, Some(assignments)) => (input, Some(assignments)),
        (input, None) => (input, None),
    };

    Ok((
        input,
        MergeStatement {
            pattern,
            on_create,
            on_match,
        },
    ))
}

fn foreach_statement(input: &str) -> IResult<&str, ForeachStatement> {
    let (input, _) = ws(tag_no_case("FOREACH"))(input)?;
    let (input, _) = ws(char('('))(input)?;

    // 解析循环变量
    let (input, variable) = ws(identifier)(input)?;
    let (input, _) = ws(tag_no_case("IN"))(input)?;

    // 解析列表表达式（简化版：只支持列表字面量）
    let (input, list_expr) = alt((
        list_literal,  // list_literal 已经返回 Expression::List
        map(identifier, |var| Expression::Property(var, "*".to_string())),
    ))(input)?;

    let (input, _) = ws(char('|'))(input)?;
    let (input, _) = opt(ws(tag_no_case("SET")))(input)?;

    // 解析 SET 操作
    let (input, updates) = separated_list1(ws(char(',')), set_assignment)(input)?;

    let (input, _) = ws(char(')'))(input)?;

    Ok((
        input,
        ForeachStatement {
            variable,
            list_expr,
            updates,
        },
    ))
}

// CALL 子查询解析：CALL { <subquery> }
// 支持两种形式：
// 1. CALL { MATCH ... RETURN ... } RETURN ...
// 2. CALL { MATCH ... RETURN ... } IN (var1, var2, ...) RETURN ...
// 3. 支持 WITH 子句在内层查询中
fn call_statement(input: &str) -> IResult<&str, CallStatement> {
    let (input, _) = ws(tag_no_case("CALL"))(input)?;
    let (input, _) = ws(char('{'))(input)?;

    // 解析内层子查询：MATCH ... WHERE ... RETURN ...
    // 注意：内层查询可以包含 WITH 子句
    let (input, inner_match) = opt(match_clause)(input)?;
    let (input, inner_with) = opt(with_clause)(input)?;
    let (input, inner_where) = opt(where_clause)(input)?;
    let (input, inner_return) = return_clause(input)?;

    let (input, _) = ws(char('}'))(input)?;

    // 解析 IN 子句（可选）：IN (var1, var2, ...)
    let (input, has_in) = opt(ws(tag_no_case("IN")))(input)?;
    let (input, input_vars) = if has_in.is_some() {
        // 解析变量列表：(var1, var2, ...)
        let (input, _) = ws(tag("("))(input)?;
        let (input, vars) = separated_list0(
            ws(char(',')),
            ws(identifier),
        )(input)?;
        let (input, _) = ws(tag(")"))(input)?;
        (input, vars)
    } else {
        (input, vec![])
    };

    // 解析外层查询的 RETURN 子句
    let (input, outer_return) = return_clause(input)?;

    // 从 outer_return 提取返回的变量
    let with_returns = outer_return.items.clone();

    Ok((
        input,
        CallStatement {
            outer_query: CypherQuery {
                match_clause: None,
                with_clause: None,
                where_clause: None,
                return_clause: outer_return,
            },
            inner_query: CypherQuery {
                match_clause: inner_match,
                with_clause: inner_with,
                where_clause: inner_where,
                return_clause: inner_return,
            },
            input_vars,
            with_returns,
        },
    ))
}

// UNION ALL 解析：MATCH ... RETURN ... UNION ALL MATCH ... RETURN ...
fn union_statement(input: &str) -> IResult<&str, UnionStatement> {
    // 解析左侧查询：MATCH ... WHERE ... RETURN ...
    let (input, left_match) = opt(match_clause)(input)?;
    let (input, left_with) = opt(with_clause)(input)?;
    let (input, left_where) = opt(where_clause)(input)?;
    let (input, left_return) = return_clause(input)?;

    // 解析 UNION 或 UNION ALL
    let (input, _) = ws(tag_no_case("UNION"))(input)?;
    let (input, has_all) = opt(ws(tag_no_case("ALL")))(input)?;
    let all = has_all.is_some();

    // 解析右侧查询：MATCH ... WHERE ... RETURN ...
    let (input, right_match) = opt(match_clause)(input)?;
    let (input, right_with) = opt(with_clause)(input)?;
    let (input, right_where) = opt(where_clause)(input)?;
    let (input, right_return) = return_clause(input)?;

    Ok((
        input,
        UnionStatement {
            left: CypherQuery {
                match_clause: left_match,
                with_clause: left_with,
                where_clause: left_where,
                return_clause: left_return,
            },
            right: CypherQuery {
                match_clause: right_match,
                with_clause: right_with,
                where_clause: right_where,
                return_clause: right_return,
            },
            all,
        },
    ))
}

pub fn cypher_statement(input: &str) -> IResult<&str, CypherStatement> {
    // 先检查是否有特殊关键字（用于区分语句类型）
    let input_lower = input.to_lowercase();
    let has_delete = input_lower.contains("delete");
    let has_set = input_lower.contains("set");
    let has_merge = input_lower.contains("merge");
    let has_foreach = input_lower.contains("foreach");
    let has_call = input_lower.contains("call");
    let has_union = input_lower.contains("union");

    // 检查事务控制语句
    let trimmed = input.trim().to_lowercase();
    if trimmed.starts_with("begin") || trimmed.starts_with("start") {
        // BEGIN 或 START TRANSACTION
        if let Ok((rest, _)) = parse_begin_transaction(input) {
            return Ok((rest, CypherStatement::BeginTransaction));
        }
    }
    if trimmed.starts_with("commit") {
        if let Ok((rest, _)) = parse_commit_transaction(input) {
            return Ok((rest, CypherStatement::CommitTransaction));
        }
    }
    if trimmed.starts_with("rollback") {
        if let Ok((rest, _)) = parse_rollback_transaction(input) {
            return Ok((rest, CypherStatement::RollbackTransaction));
        }
    }

    // 尝试 UNION ALL（必须在其他检查之前，因为它包含多个查询）
    if has_union {
        if let Ok((rest, stmt)) = union_statement(input) {
            return Ok((rest, CypherStatement::Union(stmt)));
        }
    }

    // 先尝试 CREATE（以 CREATE 开头的语句）
    if let Ok((rest, stmt)) = create_clause(input) {
        return Ok((rest, CypherStatement::Create(stmt)));
    }

    // 尝试 FOREACH
    if has_foreach {
        if let Ok((rest, stmt)) = foreach_statement(input) {
            return Ok((rest, CypherStatement::Foreach(stmt)));
        }
    }

    // 尝试 CALL 子查询
    if has_call {
        if let Ok((rest, stmt)) = call_statement(input) {
            return Ok((rest, CypherStatement::Call(stmt)));
        }
    }

    // 尝试 DELETE
    if has_delete {
        if let Ok((rest, stmt)) = delete_statement(input) {
            return Ok((rest, CypherStatement::Delete(stmt)));
        }
    }

    // 尝试 SET
    if has_set {
        if let Ok((rest, stmt)) = set_statement(input) {
            return Ok((rest, CypherStatement::Set(stmt)));
        }
    }

    // 尝试 MERGE
    if has_merge {
        if let Ok((rest, stmt)) = merge_statement(input) {
            return Ok((rest, CypherStatement::Merge(stmt)));
        }
    }

    // 否则是查询（带 RETURN）
    let (input, match_c) = opt(match_clause)(input)?;
    let (input, with_c) = opt(with_clause)(input)?;
    let (input, where_c) = opt(where_clause)(input)?;
    let (input, return_c) = return_clause(input)?;

    Ok((input, CypherStatement::Query(CypherQuery {
        match_clause: match_c,
        with_clause: with_c,
        where_clause: where_c,
        return_clause: return_c,
    })))
}

pub fn parse_cypher(input: &str) -> Result<CypherStatement, String> {
    // 首先去除前后的空白
    let input = input.trim();
    match cypher_statement(input) {
        Ok((rest, stmt)) => {
            // 确保剩余的只有空白（或分号+空白）
            let rest = rest.trim();
            if rest.is_empty() || rest.starts_with(';') {
                Ok(stmt)
            } else {
                Err(format!("Unexpected trailing input: {}", rest))
            }
        }
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}

/// 解析 BEGIN TRANSACTION 语句
fn parse_begin_transaction(input: &str) -> IResult<&str, ()> {
    let (input, _) = alt((
        tag_no_case("BEGIN TRANSACTION"),
        tag_no_case("START TRANSACTION"),
        tag_no_case("BEGIN"),
        tag_no_case("START"),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(char(';'))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, ()))
}

/// 解析 COMMIT 语句
fn parse_commit_transaction(input: &str) -> IResult<&str, ()> {
    let (input, _) = alt((
        tag_no_case("COMMIT TRANSACTION"),
        tag_no_case("COMMIT"),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(char(';'))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, ()))
}

/// 解析 ROLLBACK 语句
fn parse_rollback_transaction(input: &str) -> IResult<&str, ()> {
    let (input, _) = alt((
        tag_no_case("ROLLBACK TRANSACTION"),
        tag_no_case("ROLLBACK"),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(char(';'))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, ()))
}
