use crate::network::{GenericNetwork, Ordering, TriplesField};
use std::ops::Deref;

/// TODO call this Shape or Gather to sound less SQL?
#[derive(Debug, Clone, PartialEq)]
pub struct Select<'n, V> {
    pub(crate) network: &'n GenericNetwork<V>,
    pub(crate) selection: Vec<TriplesField>,
    pub(crate) order_by: Vec<(TriplesField, Ordering)>,
    pub(crate) limit: i64,
}

impl<'n, V> From<&'n GenericNetwork<V>> for Select<'n, V> {
    fn from(network: &'n GenericNetwork<V>) -> Self {
        Select { network, selection: vec![], order_by: vec![], limit: 0 }
    }
}

impl<'n, V> GenericNetwork<V> {
    pub fn select(&'n self) -> Select<'n, V> {
        Select::from(self)
    }
}

impl<'n, V> Deref for Select<'n, V> {
    type Target = GenericNetwork<V>;

    fn deref(&self) -> &Self::Target {
        &self.network
    }
}

impl<'n, V> Select<'n, V> {
    pub fn fields(&self) -> &[TriplesField] {
        self.selection.as_slice()
    }

    pub fn field(&mut self, field: TriplesField) -> &mut Self {
        self.selection.push(field);
        self
    }

    pub fn limit(&mut self, limit: i64) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn order_by(&mut self, ord: (TriplesField, Ordering)) -> &mut Self {
        self.order_by.push(ord);
        self
    }
}
