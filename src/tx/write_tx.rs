// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(all(feature = "single_writer_tx", feature = "ssi_tx"))]
compile_error!("Either single_writer_tx or ssi_tx can be enabled at once");

#[cfg(feature = "single_writer_tx")]
/// A single-writer (serialized) cross-partition transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the partition(s).
///
/// Drop the transaction to rollback changes.
pub type WriteTransaction<'a> = super::write::single_writer::WriteTransaction<'a>;
#[cfg(feature = "ssi_tx")]
/// A SSI (Serializable Snapshot Isolation) cross-partition transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the partition(s).
///
/// Drop the transaction to rollback changes.
pub type WriteTransaction = super::write::ssi::WriteTransaction;
