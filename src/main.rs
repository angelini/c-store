#![feature(box_syntax, box_patterns)]

use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::rc::Rc;

#[derive(Debug)]
enum Scalar {
    Int(i64),
    Float(f64),
    Str(String),
}

#[derive(Debug)]
enum Test {
    Equal,
    Greater,
    Less,
}

type Predicate = (Test, Scalar);

trait Column {
    fn select(&self, &Predicate) -> Vec<bool>;
    fn mask(&self, &[bool]) -> Box<Column>;
    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize>;
    fn sorted_by(&mut self, ranks: &[usize]) -> Box<Column>;
    fn join(&self, other: Box<Column>) -> Vec<usize>;
    fn clone(&self) -> Box<Column>;
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
                    .unwrap_or(std::cmp::Ordering::Equal)
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
        .map(|(idx, val)| (idx, val))
        .collect::<Vec<(usize, T)>>();
    items
        .as_mut_slice()
        .sort_by(|&(ref left_idx, _), &(ref right_idx, _)| {
                     ranks[*left_idx].cmp(&ranks[*right_idx])
                 });
    items.into_iter().map(|(_, value)| value).collect()
}

struct IntColumn {
    items: Vec<i64>,
}

impl IntColumn {
    fn new(items: Vec<i64>) -> IntColumn {
        IntColumn { items: items }
    }
}

impl Column for IntColumn {
    fn select(&self, predicate: &Predicate) -> Vec<bool> {
        match *predicate {
            (Test::Equal, Scalar::Int(s)) => self.items.iter().map(|&i| i == s).collect(),
            (Test::Greater, Scalar::Int(s)) => self.items.iter().map(|&i| i > s).collect(),
            (Test::Less, Scalar::Int(s)) => self.items.iter().map(|&i| i < s).collect(),
            _ => panic!("Invalid predicate type: {:?}", predicate),
        }
    }

    fn mask(&self, bitstring: &[bool]) -> Box<Column> {
        let masked = self.items
            .iter()
            .zip(bitstring)
            .filter(|&(_, &bit)| bit)
            .map(|(value, _)| value)
            .cloned()
            .collect();
        box IntColumn { items: masked }
    }

    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize> {
        sort_column(&self.items, partial_sort)
    }

    fn sorted_by(&mut self, ranks: &[usize]) -> Box<Column> {
        box IntColumn::new(sort_column_by(self.items.clone(), ranks))
    }

    fn clone(&self) -> Box<Column> {
        box IntColumn::new(self.items.clone())
    }

    fn to_string(&self) -> String {
        self.items
            .iter()
            .map(|i| format!("{}", i))
            .collect::<Vec<String>>()
            .join(", ")
    }
}

struct FloatColumn {
    items: Vec<f64>,
}

impl FloatColumn {
    fn new(items: Vec<f64>) -> FloatColumn {
        FloatColumn { items: items }
    }
}

impl Column for FloatColumn {
    fn select(&self, predicate: &Predicate) -> Vec<bool> {
        match *predicate {
            (Test::Equal, Scalar::Float(s)) => self.items.iter().map(|&i| i == s).collect(),
            (Test::Greater, Scalar::Float(s)) => self.items.iter().map(|&i| i > s).collect(),
            (Test::Less, Scalar::Float(s)) => self.items.iter().map(|&i| i < s).collect(),
            _ => panic!("Invalid predicate type: {:?}", predicate),
        }
    }

    fn mask(&self, bitstring: &[bool]) -> Box<Column> {
        let masked = self.items
            .iter()
            .zip(bitstring)
            .filter(|&(_, &bit)| bit)
            .map(|(value, _)| value)
            .cloned()
            .collect();
        box FloatColumn::new(masked)
    }

    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize> {
        sort_column(&self.items, partial_sort)
    }

    fn sorted_by(&mut self, ranks: &[usize]) -> Box<Column> {
        box FloatColumn::new(sort_column_by(self.items.clone(), ranks))
    }

    fn clone(&self) -> Box<Column> {
        box FloatColumn::new(self.items.clone())
    }

    fn to_string(&self) -> String {
        self.items
            .iter()
            .map(|i| format!("{}", i))
            .collect::<Vec<String>>()
            .join(", ")
    }
}

struct StrColumn {
    string: String,
    lengths: Vec<usize>,
}

struct StrColumnIter<'a> {
    column: &'a StrColumn,
    str_idx: usize,
    len_idx: usize,
}

impl<'a> Iterator for StrColumnIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len_idx == self.column.lengths.len() {
            return None;
        }
        let len = self.column.lengths[self.len_idx];
        let s = &self.column.string[self.str_idx..self.str_idx + len];
        self.str_idx += len;
        self.len_idx += 1;
        Some(s)
    }
}

impl StrColumn {
    fn new(items: Vec<&str>) -> StrColumn {
        let mut string = String::new();
        let mut lengths = vec![];
        for item in items {
            lengths.push(item.len());
            string.push_str(item);
        }
        StrColumn {
            string: string,
            lengths: lengths,
        }
    }

    fn iter<'a>(&'a self) -> StrColumnIter<'a> {
        StrColumnIter {
            column: self,
            str_idx: 0,
            len_idx: 0,
        }
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
        let (string, lengths) = self.iter()
            .zip(bitstring)
            .filter(|&(_, &bit)| bit)
            .map(|(value, _)| value)
            .fold((String::new(), vec![]), |mut acc, item| {
                acc.0.push_str(item);
                acc.1.push(item.len());
                (acc.0, acc.1)
            });
        box StrColumn {
                string: string,
                lengths: lengths,
            }
    }

    fn sort(&self, partial_sort: Option<Vec<usize>>) -> Vec<usize> {
        sort_column(&self.iter().collect::<Vec<&str>>(), partial_sort)
    }

    fn sorted_by(&mut self, ranks: &[usize]) -> Box<Column> {
        box StrColumn::new(sort_column_by(self.iter().collect(), ranks))
    }

    fn clone(&self) -> Box<Column> {
        box StrColumn {
                string: self.string.clone(),
                lengths: self.lengths.clone(),
            }
    }

    fn to_string(&self) -> String {
        self.iter()
            .map(|s| format!("\"{}\"", s))
            .collect::<Vec<String>>()
            .join(", ")
    }
}

struct Projection {
    columns: HashMap<String, Rc<Box<Column>>>,
    sort_cols: Vec<String>,
}

impl Projection {
    fn new(mut columns: HashMap<String, Box<Column>>, sort_cols: Vec<String>) -> Projection {
        let mut ranks_opt = None;
        for sort_col in &sort_cols {
            let col = columns
                .get(sort_col)
                .expect(&format!("Sort column not in projection: {:?}", sort_col));
            ranks_opt = Some(col.sort(ranks_opt));
        }
        if let Some(ranks) = ranks_opt {
            let sorted_columns = columns
                .iter_mut()
                .map(|(name, ref mut col)| (name.clone(), Rc::new((*col).sorted_by(&ranks))))
                .collect();
            Projection {
                columns: sorted_columns,
                sort_cols: sort_cols,
            }
        } else {
            panic!("At least one sort column is required")
        }
    }

    fn select(&self, column_name: &str, predicate: &Predicate) -> Vec<bool> {
        let col = self.columns
            .get(column_name)
            .expect(&format!("Selected column not in projection: {:?}", column_name));
        col.select(predicate)
    }

    fn mask(&self, bitstring: &[bool]) -> Projection {
        let masked = self.columns
            .iter()
            .map(|(name, col)| (name.clone(), Rc::new(col.mask(bitstring))))
            .collect();
        Projection {
            columns: masked,
            sort_cols: self.sort_cols.clone(),
        }
    }

    fn project(&self, keep_cols: &[&str]) -> Projection {
        let projected = self.columns
            .iter()
            .filter(|&(ref name, _)| keep_cols.contains(&name.as_str()))
            .map(|(name, col)| (name.clone(), col.clone()))
            .collect();
        Projection {
            columns: projected,
            sort_cols: self.sort_cols.clone(),
        }
    }

    fn sort(&self, sort_cols: &[&str]) -> Projection {
        let cloned = self.columns
            .iter()
            .map(|(name, col)| (name.clone(), (*col.deref()).clone()))
            .collect();
        Projection::new(cloned, sort_cols.iter().map(|&s| String::from(s)).collect())
    }
}

impl fmt::Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\n")?;
        for (name, column) in &self.columns {
            write!(f, "{}: {}\n", name, column.to_string())?
        }
        Ok(())
    }
}

fn main() {
    let int_column = IntColumn::new(vec![1, 4, 3, 5, 5]);
    let float_column = FloatColumn::new(vec![1.1, 6.6, 2.2, 3.3, 1.1]);
    let str_column = StrColumn::new(vec!["foo", "barr", "baöopp", "baz", "quuux"]);
    let mut cols: HashMap<String, Box<Column>> = HashMap::new();
    cols.insert(String::from("ints"), box int_column);
    cols.insert(String::from("floats"), box float_column);
    cols.insert(String::from("strings"), box str_column);

    let projection = Projection::new(cols, vec![String::from("ints"), String::from("floats")]);
    println!("projection: {}", projection);

    let masked = projection.mask(&projection.select("ints", &(Test::Equal, Scalar::Int(5))));
    println!("masked: {}", masked);

    let projected = masked.project(&["strings"]);
    println!("projected: {}", projected);

    let sorted = projected.sort(&["strings"]);
    println!("sorted: {}", sorted);
}
