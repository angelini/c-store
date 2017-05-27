use std::cmp;
use std::fmt;
use types::{List, Predicate, Scalar, Test};

macro_rules! select_by_predicate {
    ( $items: expr, $predicate:expr, $scalar:path ) => {{
        match *$predicate {
            (Test::Equal, $scalar(s)) => $items.iter().map(|&i| i == s).collect(),
            (Test::Greater, $scalar(s)) => $items.iter().map(|&i| i > s).collect(),
            (Test::Less, $scalar(s)) => $items.iter().map(|&i| i < s).collect(),
            _ => panic!("Invalid predicate type: {:?}", $predicate),
        }
    }};
}

pub trait Column {
    fn select(&self, &Predicate) -> Vec<bool>;
    fn mask(&self, &[bool]) -> Box<Column>;
    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize>;
    fn sorted_by(&self, ranks: &[usize], primary_sort: bool) -> Box<Column>;
    fn is_sorted(&self) -> bool;
    fn join<'a>(&self, list: List<'a>) -> Vec<usize>;
    fn joined_by(&self, join_idxs: &[usize]) -> Box<Column>;
    fn clone(&self) -> Box<Column>;
    fn as_list<'a>(&'a self) -> List<'a>;
    fn to_string(&self) -> String;
}

fn sort_column<T>(items: &[T], partial_sort: Option<Vec<usize>>) -> Vec<usize>
    where T: PartialOrd + fmt::Debug
{
    let mut keyed_by_rank = items
        .iter()
        .zip(partial_sort.unwrap_or_else(|| (0..items.len()).map(|_| 0).collect()))
        .enumerate()
        .map(|(idx, (value, rank))| (rank, idx, value))
        .collect::<Vec<(usize, usize, &T)>>();

    keyed_by_rank
        .as_mut_slice()
        .sort_by(|&(ref left_rank, _, ref left_value), &(ref right_rank, _, ref right_value)| {
            if left_rank == right_rank {
                left_value
                    .partial_cmp(right_value)
                    .unwrap_or(cmp::Ordering::Equal)
            } else {
                left_rank.cmp(right_rank)
            }
        });

    let mut rank = 0;
    let mut previous = None;
    let mut ranks = (0..items.len()).collect::<Vec<usize>>();
    for (_, idx, value) in keyed_by_rank {
        if previous != Some(value) {
            rank += 1;
            previous = Some(value);
        }
        ranks[idx] = rank;
    }

    ranks
}

fn sort_column_by<T>(items: Vec<T>, ranks: &[usize]) -> Vec<T>
    where T: PartialOrd
{
    let mut items = items
        .into_iter()
        .enumerate()
        .collect::<Vec<(usize, T)>>();
    items
        .as_mut_slice()
        .sort_by(|&(ref left_idx, _), &(ref right_idx, _)| {
                     ranks[*left_idx].cmp(&ranks[*right_idx])
                 });
    items.into_iter().map(|(_, value)| value).collect()
}

pub struct IntColumn {
    items: Vec<i64>,
    sorted: bool,
}

impl IntColumn {
    pub fn new(items: Vec<i64>, sorted: bool) -> IntColumn {
        IntColumn { items, sorted }
    }
}

impl Column for IntColumn {
    fn select(&self, predicate: &Predicate) -> Vec<bool> {
        select_by_predicate!(self.items, predicate, Scalar::Int)
    }

    fn mask(&self, bitstring: &[bool]) -> Box<Column> {
        let masked = self.items
            .iter()
            .zip(bitstring)
            .filter(|&(_, &bit)| bit)
            .map(|(value, _)| value)
            .cloned()
            .collect();
        box IntColumn::new(masked, self.sorted)
    }

    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize> {
        sort_column(&self.items, partial_sort)
    }

    fn sorted_by(&self, ranks: &[usize], primary_sort: bool) -> Box<Column> {
        box IntColumn::new(sort_column_by(self.items.clone(), ranks), primary_sort)
    }

    fn is_sorted(&self) -> bool {
        self.sorted
    }

    fn join<'a>(&self, list: List<'a>) -> Vec<usize> {
        if !self.is_sorted() {
            panic!("join only supports sorted columns");
        }
        match list {
            List::Ints(ints) => {
                let mut join_idxs = vec![];
                let mut ints_iter = ints.iter().enumerate();
                let mut idx_int = ints_iter.next();
                for item in &self.items {
                    loop {
                        if let Some((idx, int_val)) = idx_int {
                            if item == int_val {
                                join_idxs.push(idx);
                                break;
                            }
                            idx_int = ints_iter.next();
                        } else {
                            panic!("join only supports fully matched joins");
                        }
                    }
                }
                join_idxs
            }
            _ => panic!("Invalid list type for join: {:?}", list),
        }
    }

    fn joined_by(&self, join_idxs: &[usize]) -> Box<Column> {
        let joined = join_idxs
            .iter()
            .map(|&join_idx| self.items[join_idx])
            .collect();
        box IntColumn::new(joined, self.sorted)
    }

    fn clone(&self) -> Box<Column> {
        box IntColumn::new(self.items.clone(), self.sorted)
    }

    fn as_list<'a>(&'a self) -> List<'a> {
        List::Ints(&self.items)
    }

    fn to_string(&self) -> String {
        self.items
            .iter()
            .map(|i| format!("{}", i))
            .collect::<Vec<String>>()
            .join(", ")
    }
}

pub struct FloatColumn {
    items: Vec<f64>,
    sorted: bool,
}

impl FloatColumn {
    pub fn new(items: Vec<f64>, sorted: bool) -> FloatColumn {
        FloatColumn { items, sorted }
    }
}

impl Column for FloatColumn {
    fn select(&self, predicate: &Predicate) -> Vec<bool> {
        select_by_predicate!(self.items, predicate, Scalar::Float)
    }

    fn mask(&self, bitstring: &[bool]) -> Box<Column> {
        let masked = self.items
            .iter()
            .zip(bitstring)
            .filter(|&(_, &bit)| bit)
            .map(|(value, _)| value)
            .cloned()
            .collect();
        box FloatColumn::new(masked, self.sorted)
    }

    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize> {
        sort_column(&self.items, partial_sort)
    }

    fn sorted_by(&self, ranks: &[usize], primary_sort: bool) -> Box<Column> {
        box FloatColumn::new(sort_column_by(self.items.clone(), ranks), primary_sort)
    }

    fn is_sorted(&self) -> bool {
        self.sorted
    }

    fn join<'a>(&self, list: List<'a>) -> Vec<usize> {
        if !self.is_sorted() {
            panic!("join only supports sorted ancestor");
        }
        match list {
            List::Floats(floats) => {
                let mut join_idxs = vec![];
                let mut floats_iter = floats.iter().enumerate();
                let mut idx_float = floats_iter.next();
                for item in &self.items {
                    loop {
                        if let Some((idx, float_val)) = idx_float {
                            if item == float_val {
                                join_idxs.push(idx);
                                break;
                            }
                            idx_float = floats_iter.next();
                        } else {
                            panic!("join only supports fully matched joins");
                        }
                    }
                }
                join_idxs
            }
            _ => panic!("Invalid list type for join: {:?}", list),
        }
    }

    fn joined_by(&self, join_idxs: &[usize]) -> Box<Column> {
        let joined = join_idxs
            .iter()
            .map(|&join_idx| self.items[join_idx])
            .collect();
        box FloatColumn::new(joined, self.sorted)
    }

    fn clone(&self) -> Box<Column> {
        box FloatColumn::new(self.items.clone(), self.sorted)
    }

    fn as_list<'a>(&'a self) -> List<'a> {
        List::Floats(&self.items)
    }

    fn to_string(&self) -> String {
        self.items
            .iter()
            .map(|i| format!("{}", i))
            .collect::<Vec<String>>()
            .join(", ")
    }
}

pub struct StrColumn {
    string: String,
    offsets: Vec<usize>,
    sorted: bool,
}

struct StrColumnIter<'a> {
    column: &'a StrColumn,
    idx: usize,
}

impl<'a> Iterator for StrColumnIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.column.offsets.len() {
            return None;
        }
        let s = self.column.get(self.idx);
        self.idx += 1;
        Some(s)
    }
}

impl StrColumn {
    pub fn new(items: Vec<&str>, sorted: bool) -> StrColumn {
        let mut string = String::new();
        let mut offsets = vec![];
        let mut offset = 0;
        for item in items {
            offsets.push(offset);
            offset += item.len();
            string.push_str(item);
        }
        StrColumn {
            string,
            offsets,
            sorted,
        }
    }

    fn iter<'a>(&'a self) -> StrColumnIter<'a> {
        StrColumnIter {
            column: self,
            idx: 0,
        }
    }

    fn get(&self, idx: usize) -> &str {
        let start = self.offsets[idx];
        let end = if idx == self.offsets.len() - 1 {
            self.string.len()
        } else {
            self.offsets[idx + 1]
        };
        &self.string[start..end]
    }
}

impl Column for StrColumn {
    fn select(&self, predicate: &Predicate) -> Vec<bool> {
        match *predicate {
            (Test::Equal, Scalar::Str(ref s)) => self.iter().map(|i| i == s).collect(),
            (Test::Greater, Scalar::Str(ref s)) => self.iter().map(|i| i > s).collect(),
            (Test::Less, Scalar::Str(ref s)) => self.iter().map(|i| i < s).collect(),
            _ => panic!("Invalid predicate type: {:?}", predicate),
        }
    }

    fn mask(&self, bitstring: &[bool]) -> Box<Column> {
        let (string, offsets) = self.iter()
            .zip(bitstring)
            .filter(|&(_, &bit)| bit)
            .map(|(value, _)| value)
            .fold((String::new(), vec![]), |mut acc, item| {
                let previous = if acc.1.len() == 0 {
                    0
                } else {
                    *acc.1.get(acc.1.len() - 1).unwrap()
                };
                acc.0.push_str(item);
                acc.1.push(previous + item.len());
                (acc.0, acc.1)
            });
        box StrColumn {
                string: string,
                offsets: offsets,
                sorted: self.sorted,
            }
    }

    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize> {
        sort_column(&self.iter().collect::<Vec<&str>>(), partial_sort)
    }

    fn sorted_by(&self, ranks: &[usize], primary_sort: bool) -> Box<Column> {
        box StrColumn::new(sort_column_by(self.iter().collect(), ranks), primary_sort)
    }

    fn is_sorted(&self) -> bool {
        self.sorted
    }

    fn join<'a>(&self, list: List<'a>) -> Vec<usize> {
        if !self.is_sorted() {
            panic!("join only supports sorted ancestor");
        }
        match list {
            List::Strs(string, lengths) => {
                let mut join_idxs = vec![];
                let mut str_idx = 0;
                let mut lengths_idx = 0;
                for item in self.iter() {
                    loop {
                        if lengths_idx == lengths.len() {
                            panic!("join only supports fully matched joins");
                        }
                        if item == &string[str_idx..str_idx + lengths[lengths_idx]] {
                            join_idxs.push(lengths_idx);
                            break;
                        }
                        str_idx += lengths[lengths_idx];
                        lengths_idx += 1;
                    }
                }
                join_idxs
            }
            _ => panic!("Invalid list type for join: {:?}", list),
        }
    }

    fn joined_by(&self, join_idxs: &[usize]) -> Box<Column> {
        let joined = join_idxs
            .iter()
            .map(|&join_idx| self.get(join_idx))
            .collect();
        box StrColumn::new(joined, self.sorted)
    }

    fn as_list<'a>(&'a self) -> List<'a> {
        List::Strs(&self.string, &self.offsets)
    }

    fn clone(&self) -> Box<Column> {
        box StrColumn {
                string: self.string.clone(),
                offsets: self.offsets.clone(),
                sorted: self.sorted,
            }
    }

    fn to_string(&self) -> String {
        self.iter()
            .map(|s| format!("\"{}\"", s))
            .collect::<Vec<String>>()
            .join(", ")
    }
}


#[macro_export]
macro_rules! build_column {
    ( Int => $values:expr ) => {{
        box IntColumn::new($values, false)
    }};
    ( Float => $values:expr ) => {{
        box FloatColumn::new($values, false)
    }};
    ( Str => $values:expr ) => {{
        box StrColumn::new($values, false)
    }};
}

#[macro_export]
macro_rules! build_projection {
    ( $sort_cols:expr, $( $name:expr, ($type:tt, $values:expr) ),* ) => {{
        let mut cols: HashMap<String, Box<Column>> = HashMap::new();
        $(
            cols.insert(String::from($name), build_column!($type => $values));
        )*
        Projection::new(cols, $sort_cols.iter().map(|&s| String::from(s)).collect())
    }};
}
