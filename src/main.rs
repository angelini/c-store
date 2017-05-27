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
enum List<'a> {
    Ints(&'a [i64]),
    Floats(&'a [f64]),
    Strs(&'a str, &'a [usize]),
}

#[derive(Debug)]
enum Test {
    Equal,
    Greater,
    Less,
}

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

type Predicate = (Test, Scalar);

trait Column {
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
    sorted: bool,
}

impl IntColumn {
    fn new(items: Vec<i64>, sorted: bool) -> IntColumn {
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

struct FloatColumn {
    items: Vec<f64>,
    sorted: bool,
}

impl FloatColumn {
    fn new(items: Vec<f64>, sorted: bool) -> FloatColumn {
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

struct StrColumn {
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
    fn new(items: Vec<&str>, sorted: bool) -> StrColumn {
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
                .map(|(name, ref mut col)| {
                         let is_primary = &sort_cols[0] == name;
                         (name.clone(), Rc::new((*col).sorted_by(&ranks, is_primary)))
                     })
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

    fn join(&self, other: &Projection, self_col_name: &str, other_col_name: &str) -> Projection {
        let mut self_ranks = None;
        let mut other_ranks = None;
        let mut self_col = self.columns
            .get(self_col_name)
            .expect(&format!("Column not found: {}", self_col_name))
            .clone();
        let mut other_col = other
            .columns
            .get(other_col_name)
            .expect(&format!("Column not found: {}", other_col_name))
            .clone();

        if !self_col.is_sorted() {
            let ranks = self_col.sort(None);
            self_col = Rc::new(self_col.sorted_by(&ranks, true));
            self_ranks = Some(ranks);
        }
        if !other_col.is_sorted() {
            let ranks = other_col.sort(None);
            other_col = Rc::new(other_col.sorted_by(&ranks, true));
            other_ranks = Some(ranks);
        }

        let other_list = other_col.as_list();
        let join_idxs = self_col.join(other_list);

        let mut new_cols = self.columns.clone();
        if let &Some(ref self_ranks) = &self_ranks {
            for (name, column) in new_cols.iter_mut() {
                *column = Rc::new(column.sorted_by(&self_ranks, name == self_col_name));
            }
        };
        for (name, column) in &other.columns {
            if let &Some(ref other_ranks) = &other_ranks {
                new_cols.insert(name.clone(),
                                Rc::new(column
                                            .sorted_by(&other_ranks, name == other_col_name)
                                            .joined_by(&join_idxs)));
            } else {
                new_cols.insert(name.clone(), Rc::new(column.joined_by(&join_idxs)));
            }
        }

        let sort_cols = if self_ranks.is_some() {
            vec![String::from(self_col_name)]
        } else {
            self.sort_cols.clone()
        };
        Projection {
            columns: new_cols,
            sort_cols: sort_cols,
        }
    }
}

impl fmt::Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\n")?;
        for (name, column) in &self.columns {
            write!(f,
                   "{} ({}): {}\n",
                   name,
                   column.is_sorted(),
                   column.to_string())?
        }
        Ok(())
    }
}

fn gen_column_map(prefix: &str,
                  ints: Vec<i64>,
                  floats: Vec<f64>,
                  strs: Vec<&str>)
                  -> HashMap<String, Box<Column>> {
    let int_column = IntColumn::new(ints, false);
    let float_column = FloatColumn::new(floats, false);
    let str_column = StrColumn::new(strs, false);
    let mut cols: HashMap<String, Box<Column>> = HashMap::new();
    cols.insert(format!("{}ints", prefix), box int_column);
    cols.insert(format!("{}floats", prefix), box float_column);
    cols.insert(format!("{}strings", prefix), box str_column);
    cols
}

fn main() {
    let cols = gen_column_map("",
                              vec![1, 4, 3, 5, 5],
                              vec![1.1, 4.4, 3.3, 5.5, 5.5],
                              vec!["one", "four", "three", "five", "five"]);
    let projection = Projection::new(cols, vec![String::from("strings"), String::from("floats")]);
    println!("projection: {}", projection);

    let masked = projection.mask(&projection.select("ints", &(Test::Equal, Scalar::Int(5))));
    println!("masked: {}", masked);

    let projected = masked.project(&["strings"]);
    println!("projected: {}", projected);

    let sorted = projected.sort(&["strings"]);
    println!("sorted: {}", sorted);

    let join_cols = gen_column_map("join_",
                                   vec![6, 5, 4, 3, 2, 1],
                                   vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
                                   vec!["six", "five", "four", "three", "two", "one"]);
    let join_projection = Projection::new(join_cols, vec![String::from("join_strings")]);
    println!("join_projection: {}", join_projection);

    let joined = projection.join(&join_projection, "ints", "join_ints");
    println!("joined: {}", joined);
}
