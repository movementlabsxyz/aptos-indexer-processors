// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::{
    schema::nft_points,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        util::{
            get_clean_payload, get_entry_function_from_user_request, parse_timestamp,
            standardize_address,
        },
    },
};
use aptos_protos::transaction::v1::{transaction::TxnData, Transaction};
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version))]
#[diesel(table_name = nft_points)]
pub struct NftPoints {
    pub transaction_version: i64,
    pub owner_address: String,
    pub token_name: String,
    pub point_type: String,
    pub amount: BigDecimal,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

impl NftPoints {
    pub fn from_transaction(
        transaction: &Transaction,
        nft_points_contract: Option<String>,
    ) -> Option<Self> {
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["NftPoints"])
                    .inc();
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                return None;
            },
        };
        let version = transaction.version as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        if let Some(contract) = nft_points_contract {
            if let TxnData::User(user_txn) = txn_data {
                let user_request = user_txn
                    .request
                    .as_ref()
                    .expect("Sends is not present in user txn");

                let payload = match user_request.payload.as_ref() {
                    Some(payload) => payload,
                    None => {
                        tracing::warn!(transaction_version = version, "Payload is empty.");
                        return None;
                    },
                };
                let entry_function_id_str =
                    get_entry_function_from_user_request(user_request).unwrap_or_default();

                // If failed transaction, end
                if !transaction_info.success {
                    return None;
                }
                if entry_function_id_str == contract {
                    let payload_cleaned = get_clean_payload(payload, version).unwrap();
                    let args = payload_cleaned["arguments"]
                        .as_array()
                        .or_else(|| {
                            tracing::error!(
                                transaction_version = version,
                                payload = ?payload_cleaned,
                                "Failed to get arguments from nft_points transaction"
                            );
                            None
                        })?
                        .iter()
                        .map(|x| {
                            unescape::unescape(x.as_str().or_else(|| {
                                tracing::error!(
                                    transaction_version = version,
                                    payload = ?payload_cleaned,
                                    "Failed to parse arguments from nft_points transaction"
                                );
                                None
                            })?)
                            .or_else(|| {
                                tracing::error!(
                                    transaction_version = version,
                                    payload = ?payload_cleaned,
                                    "Failed to escape arguments from nft_points transaction"
                                );
                                None
                            })
                        })
                        .collect::<Option<Vec<String>>>()?;
                    let owner_address = standardize_address(&args[0]);
                    let amount = args[2].parse().map_or_else(
                        |_| {
                            tracing::error!(
                                transaction_version = version,
                                argument = &args[2],
                                "Failed to parse amount from nft_points transaction"
                            );
                            None
                        },
                        Some,
                    )?;
                    let transaction_timestamp = parse_timestamp(timestamp, version);
                    return Some(Self {
                        transaction_version: version,
                        owner_address,
                        token_name: args[1].clone(),
                        point_type: args[3].clone(),
                        amount,
                        transaction_timestamp,
                    });
                }
            }
        }
        None
    }
}
