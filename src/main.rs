#![feature(box_syntax, box_patterns, trace_macros)]

#[macro_use]
mod column;
mod types;

use column::{Column, IntColumn, FloatColumn, StrColumn};
use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::rc::Rc;
use types::{Predicate, Scalar, Test};

// trace_macros!(true);

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

fn main() {
    let projection = build_projection!(vec!["strings", "floats"],
                                       "ints",
                                       (Int, vec![1, 4, 3, 5, 5]),
                                       "floats",
                                       (Float, vec![1.1, 4.4, 3.3, 5.5, 5.5]),
                                       "strings",
                                       (Str, vec!["one", "four", "three", "five", "five"]));
    println!("projection: {}", projection);

    let masked = projection.mask(&projection.select("ints", &(Test::Equal, Scalar::Int(5))));
    println!("masked: {}", masked);

    let projected = masked.project(&["strings"]);
    println!("projected: {}", projected);

    let sorted = projected.sort(&["strings"]);
    println!("sorted: {}", sorted);

    let join_projection = build_projection!(vec!["join_strings"],
                                            "join_ints",
                                            (Int, vec![6, 5, 4, 3, 2, 1]),
                                            "join_floats",
                                            (Float, vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0]),
                                            "join_strings",
                                            (Str,
                                             vec!["six", "five", "four", "three", "two", "one"]));
    println!("join_projection: {}", join_projection);

    let joined = projection.join(&join_projection, "ints", "join_ints");
    println!("joined: {}", joined);
}
