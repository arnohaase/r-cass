
use crate::io::{CassWrite, CassSerializer};
use std::io::{BufWriter, Write, Seek};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::borrow::Borrow;
use std::marker::PhantomData;


const ID_LEAF_NODE: u8 = 0;
const ID_BRANCH_NODE: u8 = 1;

pub struct IndexFileCreator<'a, K,V,W,SK>
        where W: Write+Seek, K: Copy, SK: CassSerializer<K> {
    state: IndexFileCreatorState<K,V>,
    io: IndexFileCreatorIo<'a, K,SK,W>,
}

struct IndexFileCreatorState<K,V> {
    arity: usize,
    cur_leaf: Option<CreatorLeafNode<K,V>>,
    branch_stack: Vec<CreatorBranchNode<K>>, // current hierarchy of partially filled branches. Root goes first, deepest branch goes last
}

struct IndexFileCreatorIo<'a, K,SK,W> where W: Write+Seek, K: Copy, SK: CassSerializer<K> {
    out: CassWrite<W>,
    ser_key: &'a SK,
    _sk: PhantomData<K>,
}

//TODO K: Copy?!

impl <'a, K,SK,W> IndexFileCreatorIo<'a, K,SK,W> where W: Write+Seek, K: Copy, SK: CassSerializer<K> {
    fn write_branch(&mut self, n: &CreatorBranchNode<K>) -> std::io::Result<(K, u64)> {
        let result = out.position()?;

        out.write_u8(ID_BRANCH_NODE)?;
        out.write_u16(n.kvs.len() as u16)?;
        for (k,v) in n.kvs.iter() {
//            self.ser_key.ser(out, k)?;
            //TODO out.write k --> adapter of some kind
            out.write_u64(*v)?; //TODO adapter of some kind to support fixed vs var length, u32, ...?
        }

        let (k,v) = n.kvs.first().unwrap();
        Ok((*k, result))
    }

    fn write_leaf(out: &mut CassWrite<W>, n: &CreatorLeafNode<K,V>) -> std::io::Result<(K, u64)> {
        let result = out.position()?;

        out.write_u8(ID_LEAF_NODE)?;
        out.write_u16(n.kvs.len() as u16)?;
        for (k,v) in n.kvs.iter() {
            //TODO out.write k --> adapter of some kind
            //TODO out.write v --> adapter of some kind
        }

        let (k,v) = n.kvs.first().unwrap();
        Ok((*k, result))
    }

}

impl <'a, K,V,W,SK> IndexFileCreator<'a, K,V,W,SK> where W:Write+Seek, K: Copy, SK: CassSerializer<K> {
    pub fn new(arity: usize, out: W, ser_key: &'a SK) -> IndexFileCreator<K,V,W,SK> {
        assert!(arity >= 2 && arity <= std::u16::MAX as usize);
        IndexFileCreator {
            state: IndexFileCreatorState {
                arity,
                cur_leaf: None,
                branch_stack: Vec::new(),
            },
            io: IndexFileCreatorIo {
                out: CassWrite::new (out),
                ser_key,
                _sk: PhantomData,
            }
        }
    }

    pub fn add_entry(&mut self, key: K, value: V) -> std::io::Result<()> {
        let l = self.state.cur_leaf.as_mut();

        match l {
            None => {
                self.state.cur_leaf = Some (CreatorLeafNode { kvs: vec!((key, value)) });
            },
            Some(n) => {
                if n.kvs.len() >= self.state.arity {
                    // leaf node is full
                    self.flush_leaf();
                    self.add_entry(key, value)?;
                }
                else {
                    n.kvs.push((key,value));
                }
            },
        }

        Ok(())
    }

    /// returns the root node offset (None means 'empty index')
    pub fn finalize(mut self) -> std::io::Result<Option<u64>> {
        // flush all nodes to disk, even if they are not full yet
        let l = self.state.cur_leaf.as_ref();
        let mut cur_children: Vec<(K,u64)> = match l {
            None => {
                let br = self.state.branch_stack.pop();
                match &br  {
                    None => {
                        return Ok(None)
                    },
                    Some(ref bn) => {
                        vec!(IndexFileCreator::<K,V,W>::write_branch(&mut self.out, bn)?)
                    }
                }
            },
            Some(ln) => {
                vec!(IndexFileCreator::write_leaf(&mut self.out, ln)?)
            }
        };

        loop {
            let mut cur_branch = self.branch_stack.pop();
            match &mut cur_branch {
                None => {
                    // we reached the top
                    match cur_children.as_slice() {
                        [only_child] => {
                            // there is only one child, so we return that as the index' root
                            return Ok(Some(only_child.1));
                        },
                        _ => {
                            // more than one child -> create new node and return it as root
                            let root = CreatorBranchNode {
                                level: 1, // not used here -> arbitrary value
                                kvs: cur_children.clone()
                            };
                            let (k,offs) = IndexFileCreator::<K,V,W>::write_branch(&mut self.out, &root)?;
                            return Ok(Some(offs))
                        }
                    }
                },
                Some(branch) => {
                    while branch.kvs.len() < self.arity && !cur_children.is_empty() {
                        branch.kvs.push(cur_children.remove(0));
                    }

                    let mut new_children = vec!(IndexFileCreator::<K,V,W>::write_branch(&mut self.out, branch)?);
                    new_children.append(&mut cur_children);
                    cur_children = new_children;
                },
            }
        }
    }

    fn flush_leaf(&mut self) -> std::io::Result<()>{
        let l = self.cur_leaf.as_ref();

        let (mut cur_child_key, mut cur_child_offset) = match l {
            None => {
                return Ok(());
            },
            Some(ln) => {
                let kv = IndexFileCreator::write_leaf(&mut self.out, ln)?;
                self.cur_leaf = None;
                kv
            }
        };

        let mut push_later = Vec::new(); //TODO review this

        let mut cur_child_level: usize = 0;
        loop {
            let mut cur_branch = self.branch_stack.last_mut();
            match &mut cur_branch {
                None => {
                    // hierarchy below here is full --> new branch node
                    self.branch_stack.push(CreatorBranchNode {
                        level: cur_child_level+1,
                        kvs: vec!((cur_child_key, cur_child_offset))
                    });
                    break;
                },
                Some(branch) if branch.level != cur_child_level+1 => {
                    // hierarchy below here is full, no entry yet at this level --> new branch node
                    self.branch_stack.push(CreatorBranchNode {
                        level: cur_child_level+1,
                        kvs: vec!((cur_child_key, cur_child_offset)),
                    });
                    break;
                },
                Some(branch) if branch.kvs.len() < self.arity => {
                    // branch node at this level exists but is not full yet -> add child to existing node
                    branch.kvs.push((cur_child_key, cur_child_offset));
                    break;
                },
                Some(branch) => {
                    push_later.push(CreatorBranchNode {
                        level: cur_child_level+1,
                        kvs: vec!((cur_child_key, cur_child_offset)),
                    });

                    // branch node at this level exists but is full --> flush to disk, propagate up
                    match IndexFileCreator::<K,V,W>::write_branch(&mut self.out, branch)? {
                        (k,v) => {
                            cur_child_key = k;
                            cur_child_offset = v;
                        }
                    }
                    cur_child_level += 1;
                    self.branch_stack.pop();
                }
            }
        }

        push_later.reverse();
        self.branch_stack.append(&mut push_later);

        Ok(())
    }
}

struct CreatorLeafNode<K,V> {
    kvs: Vec<(K,V)>,
}
struct CreatorBranchNode<K> {
    level: usize,
    kvs: Vec<(K,u64)>,
}
