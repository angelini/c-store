#[derive(Debug)]
pub enum Scalar {
    Int(i64),
    Float(f64),
    Str(String),
}

#[derive(Debug)]
pub enum List<'a> {
    Ints(&'a [i64]),
    Floats(&'a [f64]),
    Strs(&'a str, &'a [usize]),
}

#[derive(Debug)]
pub enum Test {
    Equal,
    Greater,
    Less,
}

pub type Predicate = (Test, Scalar);
