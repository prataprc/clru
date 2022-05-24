#![feature(total_cmp)]

use std::{error, fmt, result};

/// Short form to compose Error values.
///
/// Here are few possible ways:
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, msg: "bad argument");
/// ```
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, std::io::read(buf));
/// ```
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, std::fs::read(file_path), "read failed");
/// ```
///
#[macro_export]
macro_rules! err_at {
    ($v:ident, msg: $($arg:expr),+) => {{
        let prefix = format!("{}:{}", file!(), line!());
        Err(Error::$v(prefix, format!($($arg),+)))
    }};
    ($v:ident, $e:expr) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                Err(Error::$v(prefix, format!("{}", err)))
            }
        }
    }};
    ($v:ident, $e:expr, $($arg:expr),+) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                let msg = format!($($arg),+);
                Err(Error::$v(prefix, format!("{} {}", err, msg)))
            }
        }
    }};
}

/// Error variants that can be returned by this package's API.
///
/// Each variant carries a prefix, typically identifying the
/// error location.
pub enum Error {
    Fatal(String, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use Error::*;

        match self {
            Fatal(p, msg) => write!(f, "{} Fatal: {}", p, msg),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

impl error::Error for Error {}

/// Type alias for Result return type, used by this package.
pub type Result<T> = result::Result<T, Error>;

mod evictor;
mod list;
mod lru;

pub use lru::{Lru, LruBuilder};

use std::sync::atomic::{AtomicPtr, Ordering::SeqCst};

const MAX_ENTRIES: usize = 1_000_000; // maximum 1 million entries in cache.

pub struct Value<K, V> {
    value: V,
    access: AtomicPtr<list::Node<K>>,
}

impl<K, V> Clone for Value<K, V>
where
    V: Clone,
{
    fn clone(&self) -> Self {
        Value {
            value: self.value.clone(),
            access: AtomicPtr::new(self.access.load(SeqCst)),
        }
    }
}
