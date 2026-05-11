//! Mini orderbook — example consumer program for Torna.
//!
//! Demonstrates how a Solana program can use Torna as its sorted-storage
//! backend via CPI. The orderbook maps:
//!     key = (price_be_u64 || side_u8 || padding) ⇒ value = order_id_pubkey
//!
//! Operations:
//!   - PlaceOrder(price, side, order_id) → CPI insert_fast
//!   - CancelOrder(price, side)          → CPI delete_fast
//!
//! Real orderbook implementations would do much more (matching, settlement,
//! token transfers, etc.). This is purposefully minimal: it shows the
//! Torna CPI surface and demonstrates that the SDK works.
//!
//! Build with: cargo build --release
//! Deploy to test validator alongside Torna.

#![allow(clippy::result_large_err)]

use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint,
    entrypoint::ProgramResult,
    msg,
    program_error::ProgramError,
    pubkey::Pubkey,
};
use torna_sdk::{KEY_SIZE, VAL_SIZE};

entrypoint!(process_instruction);

/// Instruction discriminator. First byte of instruction data.
#[repr(u8)]
pub enum Ix {
    /// data: [u8 disc=0][u64 price BE][u8 side][32 order_id]
    /// accounts: [authority(s), torna_program, torna_header, path..., leaf]
    PlaceOrder = 0,
    /// data: [u8 disc=1][u64 price BE][u8 side]
    /// accounts: [authority(s), torna_program, torna_header, path..., leaf]
    CancelOrder = 1,
}

pub fn process_instruction(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    if data.is_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }

    match data[0] {
        0 => place_order(accounts, &data[1..]),
        1 => cancel_order(accounts, &data[1..]),
        _ => Err(ProgramError::InvalidInstructionData),
    }
}

fn encode_key(price_be: u64, side: u8) -> [u8; KEY_SIZE] {
    // 32-byte composite key: 8 bytes price (BE for lex ordering), 1 byte side,
    // then zero-padding. The BE encoding ensures higher prices > lower prices
    // under bytewise comparison.
    let mut k = [0u8; KEY_SIZE];
    k[0..8].copy_from_slice(&price_be.to_be_bytes());
    k[8] = side;
    k
}

fn encode_value(order_id: &Pubkey) -> [u8; VAL_SIZE] {
    // Pubkey is exactly 32 bytes — fits in VAL_SIZE.
    let mut v = [0u8; VAL_SIZE];
    v.copy_from_slice(order_id.as_ref());
    v
}

/// PlaceOrder: insert (price, side) → order_id into the Torna tree via CPI.
fn place_order(accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    if data.len() < 8 + 1 + 32 {
        return Err(ProgramError::InvalidInstructionData);
    }
    let price = u64::from_be_bytes(data[0..8].try_into().unwrap());
    let side = data[8];
    let order_id = Pubkey::new_from_array(data[9..9 + 32].try_into().unwrap());

    msg!("place_order: price={} side={} order_id={}", price, side, order_id);

    let iter = &mut accounts.iter();
    let authority = next_account_info(iter)?;
    let torna_program = next_account_info(iter)?;
    let tree_header = next_account_info(iter)?;
    // remaining = path accounts (root → leaf)
    let path: Vec<AccountInfo> = iter.cloned().collect();
    if path.is_empty() {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let key = encode_key(price, side);
    let value = encode_value(&order_id);

    torna_sdk::cpi::insert_fast(
        torna_program,
        tree_header,
        authority,
        &path,
        &key,
        &value,
    )?;

    msg!("place_order ok");
    Ok(())
}

/// CancelOrder: delete the (price, side) entry from Torna via CPI.
fn cancel_order(accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    if data.len() < 8 + 1 {
        return Err(ProgramError::InvalidInstructionData);
    }
    let price = u64::from_be_bytes(data[0..8].try_into().unwrap());
    let side = data[8];
    msg!("cancel_order: price={} side={}", price, side);

    let iter = &mut accounts.iter();
    let authority = next_account_info(iter)?;
    let torna_program = next_account_info(iter)?;
    let tree_header = next_account_info(iter)?;
    let path: Vec<AccountInfo> = iter.cloned().collect();
    if path.is_empty() {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let key = encode_key(price, side);
    torna_sdk::cpi::delete_fast(torna_program, tree_header, authority, &path, &key)?;
    msg!("cancel_order ok");
    Ok(())
}
