
use crate::io::{CassWrite, CassSerializer};
use std::io::{BufWriter, Write, Seek};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::borrow::Borrow;
use std::marker::PhantomData;


const ID_LEAF_NODE: u8 = 0;
const ID_BRANCH_NODE: u8 = 1;

pub struct IndexFileCreator<K,V,W,SK,SV,SO>
        where W: Write+Seek, K: Copy, SK: CassSerializer<K>, SV: CassSerializer<V>, SO: CassSerializer<u64> {
    state: IndexFileCreatorState<K,V>,
    io: IndexFileCreatorIo<K,V,SK,SV,SO,W>,
    _sk: PhantomData<SK>,
    _sv: PhantomData<SV>,
    _so: PhantomData<SO>,
}

struct IndexFileCreatorState<K,V> {
    arity: usize,
    cur_leaf: Option<CreatorLeafNode<K,V>>,
    branch_stack: Vec<CreatorBranchNode<K>>, // current hierarchy of partially filled branches. Root goes first, deepest branch goes last
}

struct IndexFileCreatorIo<K,V,SK,SV,SO,W> where W: Write+Seek, K: Copy,
                                                SK: CassSerializer<K>, SV: CassSerializer<V>, SO: CassSerializer<u64> {
    out: CassWrite<W>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    _sk: PhantomData<SK>,
    _sv: PhantomData<SV>,
    _so: PhantomData<SO>,
}

impl <K,V,SK,SV,SO,W> IndexFileCreatorIo<K,V,SK,SV,SO,W> where W: Write+Seek,
                                                               K: Copy,
                                                               SK: CassSerializer<K>,
                                                               SV: CassSerializer<V>,
                                                               SO: CassSerializer<u64>,
{
    fn write_branch(&mut self, n: &CreatorBranchNode<K>) -> std::io::Result<(K, u64)> {
        let result = self.out.position()?;

        self.out.write_u8(ID_BRANCH_NODE)?;
        self.out.write_u16(n.kvs.len() as u16)?;
        for (k,v) in n.kvs.iter() {
            SK::ser(&mut self.out, k)?;
            SO::ser(&mut self.out, v);
        }

        let (k,v) = n.kvs.first().unwrap();
        Ok((*k, result))
    }

    fn write_leaf(&mut self, n: &CreatorLeafNode<K,V>) -> std::io::Result<(K, u64)> {
        let result = self.out.position()?;

        self.out.write_u8(ID_LEAF_NODE)?;
        self.out.write_u16(n.kvs.len() as u16)?;
        for (k,v) in n.kvs.iter() {
            SK::ser(&mut self.out, k)?;
            SV::ser(&mut self.out, v)?;
        }

        let (k,v) = n.kvs.first().unwrap();
        Ok((*k, result))
    }

}

impl <K,V,W,SK,SV,SO> IndexFileCreator<K,V,W,SK,SV,SO> where W:Write+Seek,
                                                             K: Copy,
                                                             SK: CassSerializer<K>,
                                                             SV: CassSerializer<V>,
                                                             SO: CassSerializer<u64>, {
    pub fn new(arity: usize, out: W) -> IndexFileCreator<K,V,W,SK,SV,SO> {
        assert!(arity >= 2 && arity <= std::u16::MAX as usize);
        IndexFileCreator {
            state: IndexFileCreatorState {
                arity,
                cur_leaf: None,
                branch_stack: Vec::new(),
            },
            io: IndexFileCreatorIo {
                out: CassWrite::new (out),
                _k: PhantomData,
                _v: PhantomData,
                _sk: PhantomData,
                _sv: PhantomData,
                _so: PhantomData,
            },
            _sk: PhantomData,
            _sv: PhantomData,
            _so: PhantomData,
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
                        vec!(self.io.write_branch(bn)?)
                    }
                }
            },
            Some(ln) => {
                vec!(self.io.write_leaf(ln)?)
            }
        };

        loop {
            let mut cur_branch = self.state.branch_stack.pop();
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
                            let (k,offs) = self.io.write_branch(&root)?;
                            return Ok(Some(offs))
                        }
                    }
                },
                Some(branch) => {
                    while branch.kvs.len() < self.state.arity && !cur_children.is_empty() {
                        branch.kvs.push(cur_children.remove(0));
                    }

                    let mut new_children = vec!(self.io.write_branch(branch)?);
                    new_children.append(&mut cur_children);
                    cur_children = new_children;
                },
            }
        }
    }

    fn flush_leaf(&mut self) -> std::io::Result<()>{
        let cur_leaf = self.state.cur_leaf.as_ref();

        match cur_leaf {
            None => {
                Ok(())
            },
            Some(ln) => {
                let kv = self.io.write_leaf(ln)?;
                self.bubble_up_rec(0, &kv)
            }
        }
    }

    fn bubble_up_rec(&mut self, cur_child_level: usize, cur_child: &(K,u64)) -> std::io::Result<()> {
        let mut cur_branch = self.state.branch_stack.pop();
        match cur_branch {
            None => {
                // we have a child but no place to put the reference --> create a new root node
                self.state.branch_stack.push(CreatorBranchNode {
                    level: cur_child_level+1,
                    kvs: vec!(*cur_child),
                });
            },
            Some(mut branch) => {
                if branch.level != cur_child_level+1 {
                    // there is a gap in the hierarchy --> create missing node
                    assert!(branch.level > cur_child_level+1);
                    self.state.branch_stack.push(branch);
                    self.state.branch_stack.push(CreatorBranchNode {
                        level: cur_child_level+1,
                        kvs: vec!(*cur_child),
                    });
                }
                else if branch.kvs.len() < self.state.arity {
                    // there is room -> add to existing branch
                    branch.kvs.push(*cur_child);
                    self.state.branch_stack.push(branch);
                }
                else {
                    // current branch node is full -> flush to disk
                    let flushed_node = self.io.write_branch(&branch)?;
                    self.bubble_up_rec(cur_child_level+1, &flushed_node)?;

                    // now we add a new branch node for the new child
                    self.state.branch_stack.push(CreatorBranchNode {
                        level: cur_child_level+1,
                        kvs: vec!(*cur_child),
                    });
                }
            }
        }
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
