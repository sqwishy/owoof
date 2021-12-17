/// A proof/receipt that a value exists in the database so it can be used in a triple
#[derive(Debug, PartialEq)]
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

impl<T> Clone for Encoded<T> {
    fn clone(&self) -> Self {
        Self::from_rowid(self.rowid)
    }
}

impl<T> Copy for Encoded<T> {}
