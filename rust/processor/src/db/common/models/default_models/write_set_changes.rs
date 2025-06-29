// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use super::{
    move_modules::MoveModule,
    move_resources::MoveResource,
    move_tables::{CurrentTableItem, TableItem, TableMetadata},
    transactions::Transaction,
};
use crate::{
    schema::write_set_changes,
    utils::util::{standardize_address, standardize_address_from_bytes},
};
use aptos_protos::transaction::v1::{
    write_set_change::{Change as WriteSetChangeEnum, Type as WriteSetChangeTypeEnum},
    WriteSetChange as WriteSetChangePB,
};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(
    Associations, Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize,
)]
#[diesel(belongs_to(Transaction, foreign_key = transaction_version))]
#[diesel(primary_key(transaction_version, index))]
#[diesel(table_name = write_set_changes)]
pub struct WriteSetChange {
    pub transaction_version: i64,
    pub index: i64,
    pub hash: String,
    transaction_block_height: i64,
    pub type_: String,
    pub address: String,
}

impl WriteSetChange {
    pub fn from_write_set_change(
        write_set_change: &WriteSetChangePB,
        index: i64,
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> Option<(Self, WriteSetChangeDetail)> {
        let type_ = Self::get_write_set_change_type(write_set_change);

        match write_set_change.change.as_ref() {
            Some(WriteSetChangeEnum::WriteModule(inner)) => Some((
                Self {
                    transaction_version,
                    hash: standardize_address_from_bytes(inner.state_key_hash.as_slice()),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address),
                    index,
                },
                WriteSetChangeDetail::Module(MoveModule::from_write_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )),
            )),
            Some(WriteSetChangeEnum::DeleteModule(inner)) => Some((
                Self {
                    transaction_version,
                    hash: standardize_address_from_bytes(inner.state_key_hash.as_slice()),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address),
                    index,
                },
                WriteSetChangeDetail::Module(MoveModule::from_delete_module(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )),
            )),
            Some(WriteSetChangeEnum::WriteResource(inner)) => Some((
                Self {
                    transaction_version,
                    hash: standardize_address_from_bytes(inner.state_key_hash.as_slice()),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address),
                    index,
                },
                WriteSetChangeDetail::Resource(MoveResource::from_write_resource(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )),
            )),
            Some(WriteSetChangeEnum::DeleteResource(inner)) => Some((
                Self {
                    transaction_version,
                    hash: standardize_address_from_bytes(inner.state_key_hash.as_slice()),
                    transaction_block_height,
                    type_,
                    address: standardize_address(&inner.address),
                    index,
                },
                WriteSetChangeDetail::Resource(MoveResource::from_delete_resource(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                )),
            )),
            Some(WriteSetChangeEnum::WriteTableItem(inner)) => {
                let (ti, cti) = TableItem::from_write_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                Some((
                    Self {
                        transaction_version,
                        hash: standardize_address_from_bytes(inner.state_key_hash.as_slice()),
                        transaction_block_height,
                        type_,
                        address: String::default(),
                        index,
                    },
                    WriteSetChangeDetail::Table(
                        ti,
                        cti,
                        Some(TableMetadata::from_write_table_item(inner)),
                    ),
                ))
            },
            Some(WriteSetChangeEnum::DeleteTableItem(inner)) => {
                let (ti, cti) = TableItem::from_delete_table_item(
                    inner,
                    index,
                    transaction_version,
                    transaction_block_height,
                );
                Some((
                    Self {
                        transaction_version,
                        hash: standardize_address_from_bytes(inner.state_key_hash.as_slice()),
                        transaction_block_height,
                        type_,
                        address: String::default(),
                        index,
                    },
                    WriteSetChangeDetail::Table(ti, cti, None),
                ))
            },
            None => None,
        }
    }

    pub fn from_write_set_changes(
        write_set_changes: &[WriteSetChangePB],
        transaction_version: i64,
        transaction_block_height: i64,
    ) -> (Vec<Self>, Vec<WriteSetChangeDetail>) {
        write_set_changes
            .iter()
            .enumerate()
            .filter_map(|(index, write_set_change)| {
                Self::from_write_set_change(
                    write_set_change,
                    index as i64,
                    transaction_version,
                    transaction_block_height,
                )
            })
            .collect::<Vec<(Self, WriteSetChangeDetail)>>()
            .into_iter()
            .unzip()
    }

    fn get_write_set_change_type(t: &WriteSetChangePB) -> String {
        match WriteSetChangeTypeEnum::try_from(t.r#type)
            .expect("WriteSetChange must have a valid type.")
        {
            WriteSetChangeTypeEnum::DeleteModule => "delete_module".to_string(),
            WriteSetChangeTypeEnum::DeleteResource => "delete_resource".to_string(),
            WriteSetChangeTypeEnum::DeleteTableItem => "delete_table_item".to_string(),
            WriteSetChangeTypeEnum::WriteModule => "write_module".to_string(),
            WriteSetChangeTypeEnum::WriteResource => "write_resource".to_string(),
            WriteSetChangeTypeEnum::WriteTableItem => "write_table_item".to_string(),
            WriteSetChangeTypeEnum::Unspecified => "unspecified_writeset".to_string(),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub enum WriteSetChangeDetail {
    Module(MoveModule),
    Resource(MoveResource),
    Table(TableItem, CurrentTableItem, Option<TableMetadata>),
}

// Prevent conflicts with other things named `WriteSetChange`
pub type WriteSetChangeModel = WriteSetChange;
