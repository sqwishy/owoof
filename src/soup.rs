/// A proof/receipt that a value exists in the database so it can be used in a triple
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Encoded<T> {
    pub(crate) rowid: i64,
    p: std::marker::PhantomData<T>,
}

impl<T> Encoded<T> {
    pub(crate) fn from_rowid(rowid: i64) -> Self {
        let p = Default::default();
        Encoded { rowid, p }
    }
}
