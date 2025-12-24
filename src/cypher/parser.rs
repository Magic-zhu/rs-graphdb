use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{alpha1, alphanumeric1, char, digit1, multispace0},
    combinator::{map, opt, recognize},
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
    let (input, _) = char('"')(input)?;
    let (input, s) = take_while1(|c| c != '"')(input)?;
    let (input, _) = char('"')(input)?;
    Ok((input, s.to_string()))
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
        map(
            tuple((
                ws(tag("-[")),
                opt(preceded(ws(char(':')), ws(identifier))),
                ws(tag("]->")),
            )),
            |(_, rel_type, _)| RelPattern {
                var: None,
                rel_type,
                direction: Direction::Outgoing,
            },
        ),
        map(
            tuple((
                ws(tag("<-")),
                opt(delimited(ws(char('[')), opt(preceded(ws(char(':')), ws(identifier))), ws(char(']')))),
                ws(tag("-")),
            )),
            |(_, rel_type, _)| RelPattern {
                var: None,
                rel_type: rel_type.flatten(),
                direction: Direction::Incoming,
            },
        ),
        map(
            tuple((
                ws(tag("-[")),
                opt(preceded(ws(char(':')), ws(identifier))),
                ws(tag("]-")),
            )),
            |(_, rel_type, _)| RelPattern {
                var: None,
                rel_type,
                direction: Direction::Both,
            },
        ),
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
    ))(input)
}

fn condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = expression(input)?;
    let (input, op) = ws(alt((tag("="), tag(">"), tag("<"))))(input)?;
    let (input, right) = expression(input)?;

    let cond = match op {
        "=" => Condition::Eq(left, right),
        ">" => Condition::Gt(left, right),
        "<" => Condition::Lt(left, right),
        _ => unreachable!(),
    };

    Ok((input, cond))
}

fn where_clause(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = ws(tag_no_case("WHERE"))(input)?;
    let (input, conds) = separated_list1(ws(tag_no_case("AND")), condition)(input)?;
    Ok((input, WhereClause { conditions: conds }))
}

// RETURN clause parsing
fn agg_func(input: &str) -> IResult<&str, AggFunc> {
    alt((
        map(tag_no_case("SUM"), |_| AggFunc::Sum),
        map(tag_no_case("AVG"), |_| AggFunc::Avg),
        map(tag_no_case("MIN"), |_| AggFunc::Min),
        map(tag_no_case("MAX"), |_| AggFunc::Max),
    ))(input)
}

fn return_item(input: &str) -> IResult<&str, ReturnItem> {
    alt((
        map(tag_no_case("COUNT(*)"), |_| ReturnItem::Count),
        map(
            tuple((
                agg_func,
                ws(char('(')),
                ws(identifier),
                ws(char('.')),
                ws(identifier),
                ws(char(')')),
            )),
            |(func, _, var, _, prop, _)| ReturnItem::Aggregation(func, var, prop),
        ),
        map(identifier, ReturnItem::Variable),
    ))(input)
}

fn order_by(input: &str) -> IResult<&str, OrderBy> {
    let (input, _) = ws(tag_no_case("ORDER BY"))(input)?;
    let (input, var) = ws(identifier)(input)?;
    let (input, _) = ws(char('.'))(input)?;
    let (input, prop) = ws(identifier)(input)?;
    let (input, dir) = opt(ws(alt((tag_no_case("ASC"), tag_no_case("DESC")))))(input)?;

    let ascending = dir.map(|d| d.to_uppercase() != "DESC").unwrap_or(true);

    Ok((
        input,
        OrderBy {
            var,
            prop,
            ascending,
        },
    ))
}

fn return_clause(input: &str) -> IResult<&str, ReturnClause> {
    let (input, _) = ws(tag_no_case("RETURN"))(input)?;
    let (input, items) = separated_list1(ws(char(',')), ws(return_item))(input)?;
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
        },
    ))
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

pub fn cypher_statement(input: &str) -> IResult<&str, CypherStatement> {
    let (input, stmt) = alt((
        map(create_clause, CypherStatement::Create),
        map(delete_statement, CypherStatement::Delete),
        map(set_statement, CypherStatement::Set),
        map(
            tuple((
                opt(match_clause),
                opt(where_clause),
                return_clause,
            )),
            |(match_c, where_c, return_c)| {
                CypherStatement::Query(CypherQuery {
                    match_clause: match_c,
                    where_clause: where_c,
                    return_clause: return_c,
                })
            },
        ),
    ))(input)?;

    Ok((input, stmt))
}

pub fn parse_cypher(input: &str) -> Result<CypherStatement, String> {
    match cypher_statement(input) {
        Ok((_, stmt)) => Ok(stmt),
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}
