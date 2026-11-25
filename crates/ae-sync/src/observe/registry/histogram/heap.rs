use super::number::Number;

#[derive(Debug)]
struct Node {
    key: Number,
    time: i128,
    rank: usize,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}
type Tree = Option<Box<Node>>;
fn node(key: Number, time: i128, mut left: Tree, mut right: Tree) -> Box<Node> {
    if left.is_none()
        || left
            .as_ref()
            .zip(right.as_ref())
            .is_some_and(|(l, r)| l.rank < r.rank)
    {
        std::mem::swap(&mut left, &mut right);
    }
    let rank = right.as_ref().or(left.as_ref()).map_or(1, |n| n.rank + 1);
    Box::new(Node {
        key,
        time,
        rank,
        left,
        right,
    })
}
fn meld(a: Tree, b: Tree) -> Tree {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => {
            let (root, other) = if a.key.cmp(&b.key).is_lt() {
                (a, b)
            } else {
                (b, a)
            };
            let Node {
                key,
                time,
                left,
                right,
                ..
            } = *root;
            Some(node(key, time, left, meld(right, Some(other))))
        }
    }
}
#[derive(Debug, Default)]
pub(super) struct Heap {
    root: Tree,
    pub len: usize,
    pub bytes: usize,
}
impl Heap {
    pub fn projected(&self, value: &Number, limit: usize) -> usize {
        if self.len == limit {
            let min = &self.root.as_ref().expect("nonzero limit").key;
            if !value.cmp(min).is_gt() {
                return self.bytes;
            }
            self.bytes - min.bytes() + value.bytes()
        } else {
            self.bytes + 64 + value.bytes()
        }
    }
    pub fn insert(&mut self, value: Number, time: i128, limit: usize) {
        let bytes = self.projected(&value, limit);
        if self.len == limit {
            let root = self.root.as_ref().expect("full heap");
            if !value.cmp(&root.key).is_gt() {
                return;
            }
            let root = self.root.take().expect("full heap");
            self.root = meld(root.left, root.right);
        } else {
            self.len += 1;
        }
        self.root = meld(Some(node(value, time, None, None)), self.root.take());
        self.bytes = bytes;
    }
    pub fn values(&self, since: Option<i128>, into: &mut Vec<Number>) {
        let mut pending = vec![self.root.as_deref()];
        while let Some(Some(n)) = pending.pop() {
            if since.is_none_or(|t| n.time >= t) {
                into.push(n.key.clone());
            }
            // The original traversal is root, left, right.
            if n.right.is_some() {
                pending.push(n.right.as_deref());
            }
            if n.left.is_some() {
                pending.push(n.left.as_deref());
            }
        }
    }
}
