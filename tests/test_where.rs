use nom::bytes::complete::tag_no_case;
use nom::IResult;

fn test_where(input: &str) -> IResult<&str, &str> {
    tag_no_case("WHERE")(input)
}

fn main() {
    let input = "WHERE n.name = 'Alice'";
    match test_where(input) {
        Ok((rest, matched)) => println!("Matched: '{}', Rest: '{}'", matched, rest),
        Err(e) => println!("Error: {:?}", e),
    }
}
