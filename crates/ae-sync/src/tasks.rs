use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

type FutureBox<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(crate) struct Tasks<K, T> {
    entries: Vec<(K, FutureBox<T>)>,
    cursor: usize,
}
impl<K: PartialEq, T> Tasks<K, T> {
    pub fn new() -> Self {
        Self {
            entries: vec![],
            cursor: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn insert(&mut self, key: K, future: impl Future<Output = T> + Send + 'static) {
        self.entries.push((key, Box::pin(future)));
    }
    pub fn remove(&mut self, key: &K) {
        if let Some(index) = self.entries.iter().position(|(k, _)| k == key) {
            drop(self.entries.swap_remove(index));
        }
    }
    pub fn clear(&mut self) {
        self.entries.clear();
    }
    pub async fn next(&mut self) -> (K, T) {
        poll_fn(|cx| {
            let length = self.entries.len();
            if length == 0 {
                return Poll::Pending;
            }
            for offset in 0..length {
                let index = (self.cursor + offset) % length;
                if let Poll::Ready(value) = self.entries[index].1.as_mut().poll(cx) {
                    let (key, _) = self.entries.swap_remove(index);
                    self.cursor = index;
                    return Poll::Ready((key, value));
                }
            }
            self.cursor = (self.cursor + 1) % length;
            Poll::Pending
        })
        .await
    }
}
