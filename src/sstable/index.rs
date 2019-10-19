
use crate::io::CassWrite;
use std::io::{BufWriter, Write, Seek};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::borrow::Borrow;


const ID_LEAF_NODE: u8 = 0;
const ID_BRANCH_NODE: u8 = 1;

pub struct IndexFileCreator<K,V,W> where W: Write+Seek, K: Copy {
    arity: usize,
    out: CassWrite<W>,
    cur_leaf: Option<CreatorLeafNode<K,V>>,
    branch_stack: Vec<CreatorBranchNode<K>>, // current hierarchy of partially filled branches. Root goes first, deepest branch goes last
}

impl <K,V,W> IndexFileCreator<K,V,W> where W:Write+Seek, K: Copy {
    pub fn new(arity: usize, out: W) -> IndexFileCreator<K,V,W> {
        assert!(arity >= 2 && arity <= std::u16::MAX as usize);
        IndexFileCreator {
            arity,
            out: CassWrite::new (out),
            cur_leaf: None,
            branch_stack: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, key: K, value: V) -> std::io::Result<()> {
        let l = self.cur_leaf.as_mut();

        match l {
            None => {
                self.cur_leaf = Some (CreatorLeafNode { kvs: vec!((key, value)) });
            },
            Some(n) => {
                if n.kvs.len() >= self.arity {
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
        let l = self.cur_leaf.as_ref();
        let (mut cur_child_key, mut cur_child_offset) = match l {
            None => {
                let br = self.branch_stack.pop();
                match &br  {
                    None => return Ok(None),
                    Some(ref bn) => {
                        IndexFileCreator::<K,V,W>::write_branch(&mut self.out, bn)?
                    }
                }
            },
            Some(ln) => {
                let kv = IndexFileCreator::write_leaf(&mut self.out, ln)?;
                (kv.0, kv.1)
            }
        };

        loop {
            let mut cur_branch = self.branch_stack.last_mut();
            match &mut cur_branch {
                None => {
                    // we reached the top - there is no node above 'current child', so it is the root
                    return Ok(Some(cur_child_offset));
                },
                Some(branch) if branch.kvs.len() < self.arity => {
                    // no need to check for child level - it is ok to skip levels. The index is immutable, so uniformity brings no benefit

                    branch.kvs.push((cur_child_key, cur_child_offset));
                    let (new_key, new_offset)= IndexFileCreator::<K,V,W>::write_branch(&mut self.out, branch)?;
                    cur_child_key = new_key;
                    cur_child_offset = new_offset;

                    self.branch_stack.pop();
                },
                Some(branch) => {
                    // branch node at this level exists but is full --> flush to disk, propagate up

//                    let kv = IndexFileCreator::<K,V,W>::write_branch(&mut self.out, bn)?;

                    //TODO change to recursion?


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

        let mut push_later = Vec::new(); //TODO replace with (double) recursion

        let mut cur_child_level: usize = 0;
        loop {
            let mut cur_branch = self.branch_stack.last_mut();
            match &mut cur_branch {
                None => {
                    // hierarchy below here is full --> new branch node
                    self.branch_stack.push(CreatorBranchNode {
                        level: cur_child_level +1,
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

    fn write_branch(out: &mut CassWrite<W>, n: &CreatorBranchNode<K>) -> std::io::Result<(K, u64)> {
        let result = out.position()?;

        out.write_u8(ID_BRANCH_NODE)?;
        out.write_u16(n.kvs.len() as u16)?;
        for (k,v) in n.kvs.iter() {
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

struct CreatorLeafNode<K,V> {
    kvs: Vec<(K,V)>,
}
struct CreatorBranchNode<K> {
    level: usize,
    kvs: Vec<(K,u64)>,
}