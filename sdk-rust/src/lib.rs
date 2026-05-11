//! Torna SDK — types, PDA helpers, and CPI builders for invoking Torna from
//! another Solana program (or building txs from a Rust client).
//!
//! Torna is a multi-account B+ tree implemented in C/SBF. This crate exposes
//! its on-chain layout and instruction format as Rust types so consumer
//! programs can `cpi::insert_fast(...)` / `cpi::find(...)` etc.
//!
//! Mirrors `src/torna_btree/torna_btree.c`. Keep in sync if you change the
//! on-chain program.
//!
//! Status: v0.1 — minimal CPI helpers + on-chain struct definitions.
//! Future work: IDL artifact, an Anchor-compatible wrapper, an event log
//! interface.

#![allow(clippy::too_many_arguments)]

use bytemuck::{Pod, Zeroable};
use solana_program::{
    instruction::{AccountMeta, Instruction},
    program::invoke_signed,
    pubkey::Pubkey,
};

/// Magic for header validation: "TBT0" little-endian.
pub const TORNA_MAGIC: u32 = 0x3054_4254;

pub const KEY_SIZE: usize = 32;
pub const VAL_SIZE: usize = 32;
pub const KEYS_PER_NODE_MAX: usize = 64;
pub const KEYS_PER_NODE_MIN: usize = KEYS_PER_NODE_MAX / 2;

pub const NODE_HEADER_SIZE: usize = 16;
pub const NODE_ACCOUNT_DATA_SIZE: usize = 8192;
pub const TREE_HEADER_SIZE: usize = 80;

pub const HEADER_SEED: &[u8] = b"torna_hdr";
pub const NODE_SEED: &[u8] = b"torna";

/// Instruction discriminators. Keep aligned with `torna_btree.c`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ix {
    InitTree = 0,
    Insert = 2,
    Find = 3,
    RangeScan = 4,
    Stats = 5,
    InsertFast = 6,
    DeleteFast = 7,
    Delete = 8,
    BulkInsertFast = 9,
    BulkDeleteFast = 10,
}

/// On-account header (packed). 80 bytes.
#[repr(C, packed)]
#[derive(Clone, Copy, Pod, Zeroable, Debug)]
pub struct TreeHeader {
    pub magic: u32,
    pub tree_id: u32,
    pub root_node_idx: u32,
    pub height: u32,
    pub node_count: u32,
    pub leftmost_leaf_idx: u32,
    pub key_size: u16,
    pub value_size: u16,
    pub total_entries: u64,
    pub authority: [u8; 32],
    pub reserved: [u8; 12],
}

/// On-account node header (packed). 16 bytes.
#[repr(C, packed)]
#[derive(Clone, Copy, Pod, Zeroable, Debug)]
pub struct NodeHeader {
    pub is_leaf: u8,
    pub initialized: u8,
    pub key_count: u16,
    pub node_idx: u32,
    pub parent_idx: u32,
    pub next_leaf_idx: u32,
}

// -----------------------------------------------------------------------------
// PDA derivation
// -----------------------------------------------------------------------------

/// Derive the tree header's PDA + bump.
pub fn derive_header_pda(program_id: &Pubkey, tree_id: u32) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[HEADER_SEED, &tree_id.to_le_bytes()], program_id)
}

/// Derive the node PDA + bump for a given (treeId, nodeIdx).
pub fn derive_node_pda(program_id: &Pubkey, tree_id: u32, node_idx: u32) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[NODE_SEED, &tree_id.to_le_bytes(), &node_idx.to_le_bytes()],
        program_id,
    )
}

// -----------------------------------------------------------------------------
// Instruction builders (for off-chain tx construction)
// -----------------------------------------------------------------------------

pub mod ix {
    use super::*;

    pub fn init_tree(
        program_id: &Pubkey,
        payer: &Pubkey,
        header_pda: &Pubkey,
        tree_id: u32,
        header_bump: u8,
        rent_lamports: u64,
    ) -> Instruction {
        let mut data = Vec::with_capacity(1 + 4 + 1 + 8);
        data.push(Ix::InitTree as u8);
        data.extend_from_slice(&tree_id.to_le_bytes());
        data.push(header_bump);
        data.extend_from_slice(&rent_lamports.to_le_bytes());

        Instruction {
            program_id: *program_id,
            accounts: vec![
                AccountMeta::new(*payer, true),
                AccountMeta::new(*header_pda, false),
                AccountMeta::new_readonly(solana_program::system_program::ID, false),
            ],
            data,
        }
    }

    pub fn find(
        program_id: &Pubkey,
        tree_header: &Pubkey,
        key: &[u8; KEY_SIZE],
        path: &[Pubkey],
    ) -> Instruction {
        let mut data = Vec::with_capacity(1 + KEY_SIZE + 1);
        data.push(Ix::Find as u8);
        data.extend_from_slice(key);
        data.push(path.len() as u8);

        let mut accounts = vec![AccountMeta::new_readonly(*tree_header, false)];
        for p in path {
            accounts.push(AccountMeta::new_readonly(*p, false));
        }
        Instruction { program_id: *program_id, accounts, data }
    }

    pub fn insert_fast(
        program_id: &Pubkey,
        tree_header: &Pubkey,
        authority: &Pubkey,
        key: &[u8; KEY_SIZE],
        value: &[u8; VAL_SIZE],
        path: &[Pubkey], // root → leaf, leaf is writable
    ) -> Instruction {
        let mut data = Vec::with_capacity(1 + KEY_SIZE + VAL_SIZE + 1);
        data.push(Ix::InsertFast as u8);
        data.extend_from_slice(key);
        data.extend_from_slice(value);
        data.push(path.len() as u8);

        let mut accounts = vec![
            AccountMeta::new_readonly(*tree_header, false),
            AccountMeta::new_readonly(*authority, true),
        ];
        let last = path.len() - 1;
        for (i, p) in path.iter().enumerate() {
            if i == last {
                accounts.push(AccountMeta::new(*p, false));
            } else {
                accounts.push(AccountMeta::new_readonly(*p, false));
            }
        }
        Instruction { program_id: *program_id, accounts, data }
    }

    pub fn delete_fast(
        program_id: &Pubkey,
        tree_header: &Pubkey,
        authority: &Pubkey,
        key: &[u8; KEY_SIZE],
        path: &[Pubkey],
    ) -> Instruction {
        let mut data = Vec::with_capacity(1 + KEY_SIZE + 1);
        data.push(Ix::DeleteFast as u8);
        data.extend_from_slice(key);
        data.push(path.len() as u8);

        let mut accounts = vec![
            AccountMeta::new_readonly(*tree_header, false),
            AccountMeta::new_readonly(*authority, true),
        ];
        let last = path.len() - 1;
        for (i, p) in path.iter().enumerate() {
            if i == last {
                accounts.push(AccountMeta::new(*p, false));
            } else {
                accounts.push(AccountMeta::new_readonly(*p, false));
            }
        }
        Instruction { program_id: *program_id, accounts, data }
    }
}

// -----------------------------------------------------------------------------
// CPI helpers (for on-chain consumer programs)
// -----------------------------------------------------------------------------

/// CPI helpers — call Torna instructions from another Solana program.
///
/// Each helper assumes the caller already has the relevant AccountInfos in
/// hand and passes them in the same order the off-chain ix would use.
pub mod cpi {
    use super::*;
    use solana_program::account_info::AccountInfo;
    use solana_program::entrypoint::ProgramResult;
    use solana_program::program::invoke;

    /// Invoke Torna's IX_INSERT_FAST.
    ///
    /// `path` must be ordered root → leaf; the last entry is the writable leaf.
    /// `authority` must be a signer of the parent transaction.
    pub fn insert_fast<'a>(
        torna_program: &AccountInfo<'a>,
        tree_header: &AccountInfo<'a>,
        authority: &AccountInfo<'a>,
        path: &[AccountInfo<'a>],
        key: &[u8; KEY_SIZE],
        value: &[u8; VAL_SIZE],
    ) -> ProgramResult {
        let path_pubkeys: Vec<Pubkey> = path.iter().map(|a| *a.key).collect();
        let ix = ix::insert_fast(
            torna_program.key,
            tree_header.key,
            authority.key,
            key,
            value,
            &path_pubkeys,
        );

        let mut infos = vec![tree_header.clone(), authority.clone()];
        for p in path {
            infos.push(p.clone());
        }
        invoke(&ix, &infos)
    }

    /// Invoke Torna's IX_DELETE_FAST.
    pub fn delete_fast<'a>(
        torna_program: &AccountInfo<'a>,
        tree_header: &AccountInfo<'a>,
        authority: &AccountInfo<'a>,
        path: &[AccountInfo<'a>],
        key: &[u8; KEY_SIZE],
    ) -> ProgramResult {
        let path_pubkeys: Vec<Pubkey> = path.iter().map(|a| *a.key).collect();
        let ix = ix::delete_fast(
            torna_program.key,
            tree_header.key,
            authority.key,
            key,
            &path_pubkeys,
        );

        let mut infos = vec![tree_header.clone(), authority.clone()];
        for p in path {
            infos.push(p.clone());
        }
        invoke(&ix, &infos)
    }

    /// Same as `insert_fast` but signs for a PDA authority (e.g., when the
    /// consumer program owns the Torna tree).
    pub fn insert_fast_signed<'a>(
        torna_program: &AccountInfo<'a>,
        tree_header: &AccountInfo<'a>,
        authority: &AccountInfo<'a>,
        path: &[AccountInfo<'a>],
        key: &[u8; KEY_SIZE],
        value: &[u8; VAL_SIZE],
        signer_seeds: &[&[&[u8]]],
    ) -> ProgramResult {
        let path_pubkeys: Vec<Pubkey> = path.iter().map(|a| *a.key).collect();
        let ix = ix::insert_fast(
            torna_program.key,
            tree_header.key,
            authority.key,
            key,
            value,
            &path_pubkeys,
        );

        let mut infos = vec![tree_header.clone(), authority.clone()];
        for p in path {
            infos.push(p.clone());
        }
        invoke_signed(&ix, &infos, signer_seeds)
    }
}

// -----------------------------------------------------------------------------
// Account decoding helpers
// -----------------------------------------------------------------------------

/// Read the `TreeHeader` from raw account data. Caller must ensure
/// `data.len() >= TREE_HEADER_SIZE`.
pub fn read_header(data: &[u8]) -> Option<&TreeHeader> {
    if data.len() < TREE_HEADER_SIZE {
        return None;
    }
    bytemuck::try_from_bytes::<TreeHeader>(&data[..core::mem::size_of::<TreeHeader>()]).ok()
}

/// Read the `NodeHeader` from raw account data.
pub fn read_node_header(data: &[u8]) -> Option<&NodeHeader> {
    if data.len() < NODE_HEADER_SIZE {
        return None;
    }
    bytemuck::try_from_bytes::<NodeHeader>(&data[..core::mem::size_of::<NodeHeader>()]).ok()
}
