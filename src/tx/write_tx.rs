// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(all(feature = "single_writer_tx", feature = "ssi_tx"))]
compile_error!("Either single_writer_tx or ssi_tx can be enabled at once");

#[cfg(feature = "single_writer_tx")]
pub use super::write::single_writer::WriteTransaction;

#[cfg(feature = "ssi_tx")]
pub use super::write::ssi::WriteTransaction;

// TODO:
// use https://github.com/rust-lang/rust/issues/43781
// when stable
//
// #[doc(cfg(feature = "single_writer_tx"))]
//
// #[doc(cfg(feature = "ssi_tx"))]
