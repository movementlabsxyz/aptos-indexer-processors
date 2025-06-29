// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::ans_models::{
        ans_lookup::{AnsLookup, AnsPrimaryName, CurrentAnsLookup, CurrentAnsPrimaryName},
        ans_lookup_v2::{
            AnsLookupV2, AnsPrimaryNameV2, CurrentAnsLookupV2, CurrentAnsPrimaryNameV2,
        },
        ans_utils::{RenewNameEvent, SubdomainExtV2},
    },
    gap_detectors::ProcessingResult,
    schema,
    utils::{
        counters::PROCESSOR_UNKNOWN_TYPE_COUNT,
        database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
        util::standardize_address,
    },
    worker::TableFlags,
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChange, Transaction,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnsProcessorConfig {
    pub ans_v1_primary_names_table_handle: String,
    pub ans_v1_name_records_table_handle: String,
    pub ans_v2_contract_address: String,
}

pub struct AnsProcessor {
    connection_pool: ArcDbPool,
    config: AnsProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
    deprecated_tables: TableFlags,
}

impl AnsProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: AnsProcessorConfig,
        per_table_chunk_sizes: AHashMap<String, usize>,
        deprecated_tables: TableFlags,
    ) -> Self {
        tracing::info!(
            ans_v1_primary_names_table_handle = config.ans_v1_primary_names_table_handle,
            ans_v1_name_records_table_handle = config.ans_v1_name_records_table_handle,
            ans_v2_contract_address = config.ans_v2_contract_address,
            "init AnsProcessor"
        );
        Self {
            connection_pool,
            config,
            per_table_chunk_sizes,
            deprecated_tables,
        }
    }
}

impl Debug for AnsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "AnsProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    current_ans_lookups: &[CurrentAnsLookup],
    ans_lookups: &[AnsLookup],
    current_ans_primary_names: &[CurrentAnsPrimaryName],
    ans_primary_names: &[AnsPrimaryName],
    current_ans_lookups_v2: &[CurrentAnsLookupV2],
    ans_lookups_v2: &[AnsLookupV2],
    current_ans_primary_names_v2: &[CurrentAnsPrimaryNameV2],
    ans_primary_names_v2: &[AnsPrimaryNameV2],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    let cal = execute_in_chunks(
        conn.clone(),
        insert_current_ans_lookups_query,
        current_ans_lookups,
        get_config_table_chunk_size::<CurrentAnsLookup>(
            "current_ans_lookup",
            per_table_chunk_sizes,
        ),
    );
    let al = execute_in_chunks(
        conn.clone(),
        insert_ans_lookups_query,
        ans_lookups,
        get_config_table_chunk_size::<AnsLookup>("ans_lookup", per_table_chunk_sizes),
    );
    let capn = execute_in_chunks(
        conn.clone(),
        insert_current_ans_primary_names_query,
        current_ans_primary_names,
        get_config_table_chunk_size::<CurrentAnsPrimaryName>(
            "current_ans_primary_name",
            per_table_chunk_sizes,
        ),
    );
    let apn = execute_in_chunks(
        conn.clone(),
        insert_ans_primary_names_query,
        ans_primary_names,
        get_config_table_chunk_size::<AnsPrimaryName>("ans_primary_name", per_table_chunk_sizes),
    );
    let cal_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_ans_lookups_v2_query,
        current_ans_lookups_v2,
        get_config_table_chunk_size::<CurrentAnsLookupV2>(
            "current_ans_lookup_v2",
            per_table_chunk_sizes,
        ),
    );
    let al_v2 = execute_in_chunks(
        conn.clone(),
        insert_ans_lookups_v2_query,
        ans_lookups_v2,
        get_config_table_chunk_size::<AnsLookupV2>("ans_lookup_v2", per_table_chunk_sizes),
    );
    let capn_v2 = execute_in_chunks(
        conn.clone(),
        insert_current_ans_primary_names_v2_query,
        current_ans_primary_names_v2,
        get_config_table_chunk_size::<CurrentAnsPrimaryNameV2>(
            "current_ans_primary_name_v2",
            per_table_chunk_sizes,
        ),
    );
    let apn_v2 = execute_in_chunks(
        conn,
        insert_ans_primary_names_v2_query,
        ans_primary_names_v2,
        get_config_table_chunk_size::<AnsPrimaryNameV2>(
            "ans_primary_name_v2",
            per_table_chunk_sizes,
        ),
    );

    let (cal_res, al_res, capn_res, apn_res, cal_v2_res, al_v2_res, capn_v2_res, apn_v2_res) =
        tokio::join!(cal, al, capn, apn, cal_v2, al_v2, capn_v2, apn_v2);

    for res in vec![
        cal_res,
        al_res,
        capn_res,
        apn_res,
        cal_v2_res,
        al_v2_res,
        capn_v2_res,
        apn_v2_res,
    ] {
        res?;
    }

    Ok(())
}

fn insert_current_ans_lookups_query(
    item_to_insert: Vec<CurrentAnsLookup>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_ans_lookup::dsl::*;

    (
        diesel::insert_into(schema::current_ans_lookup::table)
            .values(item_to_insert)
            .on_conflict((domain, subdomain))
            .do_update()
            .set((
                registered_address.eq(excluded(registered_address)),
                expiration_timestamp.eq(excluded(expiration_timestamp)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                token_name.eq(excluded(token_name)),
                is_deleted.eq(excluded(is_deleted)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_ans_lookup.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_ans_lookups_query(
    item_to_insert: Vec<AnsLookup>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::ans_lookup::dsl::*;

    (
        diesel::insert_into(schema::ans_lookup::table)
            .values(item_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_current_ans_primary_names_query(
    item_to_insert: Vec<CurrentAnsPrimaryName>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_ans_primary_name::dsl::*;

    (
        diesel::insert_into(schema::current_ans_primary_name::table)
            .values(item_to_insert)
            .on_conflict(registered_address)
            .do_update()
            .set((
                domain.eq(excluded(domain)),
                subdomain.eq(excluded(subdomain)),
                token_name.eq(excluded(token_name)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_ans_primary_name.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_ans_primary_names_query(
    item_to_insert: Vec<AnsPrimaryName>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::ans_primary_name::dsl::*;

    (
        diesel::insert_into(schema::ans_primary_name::table)
            .values(item_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_current_ans_lookups_v2_query(
    item_to_insert: Vec<CurrentAnsLookupV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_ans_lookup_v2::dsl::*;

    (
        diesel::insert_into(schema::current_ans_lookup_v2::table)
            .values(item_to_insert)
            .on_conflict((domain, subdomain, token_standard))
            .do_update()
            .set((
                registered_address.eq(excluded(registered_address)),
                expiration_timestamp.eq(excluded(expiration_timestamp)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                token_name.eq(excluded(token_name)),
                is_deleted.eq(excluded(is_deleted)),
                inserted_at.eq(excluded(inserted_at)),
                subdomain_expiration_policy.eq(excluded(subdomain_expiration_policy)),
            )),
        Some(" WHERE current_ans_lookup_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_ans_lookups_v2_query(
    item_to_insert: Vec<AnsLookupV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::ans_lookup_v2::dsl::*;

    (
        diesel::insert_into(schema::ans_lookup_v2::table)
            .values(item_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                subdomain_expiration_policy.eq(excluded(subdomain_expiration_policy)),
            )),
        None,
    )
}

fn insert_current_ans_primary_names_v2_query(
    item_to_insert: Vec<CurrentAnsPrimaryNameV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_ans_primary_name_v2::dsl::*;

    (
        diesel::insert_into(schema::current_ans_primary_name_v2::table)
            .values(item_to_insert)
            .on_conflict((registered_address, token_standard))
            .do_update()
            .set((
                domain.eq(excluded(domain)),
                subdomain.eq(excluded(subdomain)),
                token_name.eq(excluded(token_name)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_ans_primary_name_v2.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_ans_primary_names_v2_query(
    items_to_insert: Vec<AnsPrimaryNameV2>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::ans_primary_name_v2::dsl::*;

    (
        diesel::insert_into(schema::ans_primary_name_v2::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for AnsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::AnsProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _db_chain_id: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let (
            mut all_current_ans_lookups,
            mut all_ans_lookups,
            mut all_current_ans_primary_names,
            mut all_ans_primary_names,
            all_current_ans_lookups_v2,
            all_ans_lookups_v2,
            all_current_ans_primary_names_v2,
            mut all_ans_primary_names_v2,
        ) = parse_ans(
            &transactions,
            self.config.ans_v1_primary_names_table_handle.clone(),
            self.config.ans_v1_name_records_table_handle.clone(),
            self.config.ans_v2_contract_address.clone(),
        );

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        if self
            .deprecated_tables
            .contains(TableFlags::ANS_PRIMARY_NAME)
        {
            all_ans_primary_names.clear();
        }
        if self
            .deprecated_tables
            .contains(TableFlags::ANS_PRIMARY_NAME_V2)
        {
            all_ans_primary_names_v2.clear();
        }
        if self.deprecated_tables.contains(TableFlags::ANS_LOOKUP) {
            all_ans_lookups.clear();
        }
        if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_ANS_LOOKUP)
        {
            all_current_ans_lookups.clear();
        }
        if self
            .deprecated_tables
            .contains(TableFlags::CURRENT_ANS_PRIMARY_NAME)
        {
            all_current_ans_primary_names.clear();
        }

        // Insert values to db
        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &all_current_ans_lookups,
            &all_ans_lookups,
            &all_current_ans_primary_names,
            &all_ans_primary_names,
            &all_current_ans_lookups_v2,
            &all_ans_lookups_v2,
            &all_current_ans_primary_names_v2,
            &all_ans_primary_names_v2,
            &self.per_table_chunk_sizes,
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        match tx_result {
            Ok(_) => Ok(ProcessingResult::DefaultProcessingResult(
                DefaultProcessingResult {
                    start_version,
                    end_version,
                    processing_duration_in_secs,
                    db_insertion_duration_in_secs,
                    last_transaction_timestamp,
                },
            )),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}

fn parse_ans(
    transactions: &[Transaction],
    ans_v1_primary_names_table_handle: String,
    ans_v1_name_records_table_handle: String,
    ans_v2_contract_address: String,
) -> (
    Vec<CurrentAnsLookup>,
    Vec<AnsLookup>,
    Vec<CurrentAnsPrimaryName>,
    Vec<AnsPrimaryName>,
    Vec<CurrentAnsLookupV2>,
    Vec<AnsLookupV2>,
    Vec<CurrentAnsPrimaryNameV2>,
    Vec<AnsPrimaryNameV2>,
) {
    let mut all_current_ans_lookups = AHashMap::new();
    let mut all_ans_lookups = vec![];
    let mut all_current_ans_primary_names = AHashMap::new();
    let mut all_ans_primary_names = vec![];
    let mut all_current_ans_lookups_v2 = AHashMap::new();
    let mut all_ans_lookups_v2 = vec![];
    let mut all_current_ans_primary_names_v2 = AHashMap::new();
    let mut all_ans_primary_names_v2 = vec![];

    for transaction in transactions {
        let txn_version = transaction.version as i64;
        let txn_data = match transaction.txn_data.as_ref() {
            Some(data) => data,
            None => {
                PROCESSOR_UNKNOWN_TYPE_COUNT
                    .with_label_values(&["AnsProcessor"])
                    .inc();
                tracing::warn!(
                    transaction_version = txn_version,
                    "Transaction data doesn't exist",
                );
                continue;
            },
        };
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");

        // Extracts from user transactions. Other transactions won't have any ANS changes

        if let TxnData::User(user_txn) = txn_data {
            // TODO: Use the v2_renew_name_events to preserve metadata once we switch to a single ANS table to store everything
            let mut v2_renew_name_events = vec![];
            let mut v2_address_to_subdomain_ext = AHashMap::new();

            // Parse V2 ANS Events. We only care about the following events:
            // 1. RenewNameEvents: helps to fill in metadata for name records with updated expiration time
            // 2. SetReverseLookupEvents: parse to get current_ans_primary_names
            for (event_index, event) in user_txn.events.iter().enumerate() {
                if let Some(renew_name_event) =
                    RenewNameEvent::from_event(event, &ans_v2_contract_address, txn_version)
                        .unwrap()
                {
                    v2_renew_name_events.push(renew_name_event);
                }
                if let Some((current_ans_lookup_v2, ans_lookup_v2)) =
                    CurrentAnsPrimaryNameV2::parse_v2_primary_name_record_from_event(
                        event,
                        txn_version,
                        event_index as i64,
                        &ans_v2_contract_address,
                    )
                    .unwrap()
                {
                    all_current_ans_primary_names_v2
                        .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                    all_ans_primary_names_v2.push(ans_lookup_v2);
                }
            }

            // Parse V2 ANS subdomain exts
            for wsc in transaction_info
                .changes
                .iter()
                .filter(|wsc| wsc.change.is_some())
            {
                match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteResource(write_resource) => {
                        if let Some(subdomain_ext) = SubdomainExtV2::from_write_resource(
                            write_resource,
                            &ans_v2_contract_address,
                            txn_version,
                        )
                        .unwrap()
                        {
                            // Track resource account -> SubdomainExt to create the full subdomain ANS later
                            v2_address_to_subdomain_ext.insert(
                                standardize_address(write_resource.address.as_str()),
                                subdomain_ext,
                            );
                        }
                    },
                    _ => continue,
                }
            }

            // Parse V1 ANS write set changes
            for (wsc_index, wsc) in transaction_info
                .changes
                .iter()
                .filter(|wsc| wsc.change.is_some())
                .enumerate()
            {
                match wsc.change.as_ref().unwrap() {
                    WriteSetChange::WriteTableItem(table_item) => {
                        if let Some((current_ans_lookup, ans_lookup)) =
                            CurrentAnsLookup::parse_name_record_from_write_table_item_v1(
                                table_item,
                                &ans_v1_name_records_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .map_err(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 name record from write table item"
                                );
                                anyhow::anyhow!(
                                    "Error parsing ANS v1 name record from write table item"
                                )
                            })
                            .ok()
                            .flatten()
                        {
                            all_current_ans_lookups
                                .insert(current_ans_lookup.pk(), current_ans_lookup.clone());
                            all_ans_lookups.push(ans_lookup.clone());

                            // Include all v1 lookups in v2 data
                            let (current_ans_lookup_v2, ans_lookup_v2) =
                                CurrentAnsLookupV2::get_v2_from_v1(current_ans_lookup, ans_lookup);
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                        if let Some((current_primary_name, primary_name)) =
                            CurrentAnsPrimaryName::parse_primary_name_record_from_write_table_item_v1(
                                table_item,
                                &ans_v1_primary_names_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .map_err(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 primary name from write table item"
                                );
                                anyhow::anyhow!(
                                    "Error parsing ANS v1 primary name from write table item"
                                )
                            })
                            .ok()
                            .flatten()
                        {
                            all_current_ans_primary_names
                                .insert(current_primary_name.pk(), current_primary_name.clone());
                            all_ans_primary_names.push(primary_name.clone());

                            // Include all v1 primary names in v2 data
                            let (current_primary_name_v2, primary_name_v2) =
                                CurrentAnsPrimaryNameV2::get_v2_from_v1(current_primary_name.clone(), primary_name.clone());
                            all_current_ans_primary_names_v2
                                .insert(current_primary_name_v2.pk(), current_primary_name_v2);
                            all_ans_primary_names_v2.push(primary_name_v2);
                        }
                    },
                    WriteSetChange::DeleteTableItem(table_item) => {
                        if let Some((current_ans_lookup, ans_lookup)) =
                            CurrentAnsLookup::parse_name_record_from_delete_table_item_v1(
                                table_item,
                                &ans_v1_name_records_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .map_err(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 name record from delete table item"
                                );
                                anyhow::anyhow!(
                                    "Error parsing ANS v1 name record from delete table item"
                                )
                            })
                            .ok()
                            .flatten()
                        {
                            all_current_ans_lookups
                                .insert(current_ans_lookup.pk(), current_ans_lookup.clone());
                            all_ans_lookups.push(ans_lookup.clone());

                            // Include all v1 lookups in v2 data
                            let (current_ans_lookup_v2, ans_lookup_v2) =
                                CurrentAnsLookupV2::get_v2_from_v1(current_ans_lookup, ans_lookup);
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                        if let Some((current_primary_name, primary_name)) =
                            CurrentAnsPrimaryName::parse_primary_name_record_from_delete_table_item_v1(
                                table_item,
                                &ans_v1_primary_names_table_handle,
                                txn_version,
                                wsc_index as i64,
                            )
                            .map_err(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v1 primary name from delete table item"
                                );
                                anyhow::anyhow!(
                                    "Error parsing ANS v1 primary name from delete table item"
                                )
                            })
                            .ok()
                            .flatten()
                        {
                            all_current_ans_primary_names
                                .insert(current_primary_name.pk(), current_primary_name.clone());
                            all_ans_primary_names.push(primary_name.clone());

                            // Include all v1 primary names in v2 data
                            let (current_primary_name_v2, primary_name_v2) =
                                CurrentAnsPrimaryNameV2::get_v2_from_v1(current_primary_name, primary_name);
                            all_current_ans_primary_names_v2
                                .insert(current_primary_name_v2.pk(), current_primary_name_v2);
                            all_ans_primary_names_v2.push(primary_name_v2);
                        }
                    },
                    WriteSetChange::WriteResource(write_resource) => {
                        if let Some((current_ans_lookup_v2, ans_lookup_v2)) =
                            CurrentAnsLookupV2::parse_name_record_from_write_resource_v2(
                                write_resource,
                                &ans_v2_contract_address,
                                txn_version,
                                wsc_index as i64,
                                &v2_address_to_subdomain_ext,
                            )
                            .map_err(|e| {
                                error!(
                                    error = ?e,
                                    "Error parsing ANS v2 name record from write resource"
                                );
                                anyhow::anyhow!(
                                    "Error parsing ANS v2 name record from write resource"
                                )
                            })
                            .ok()
                            .flatten()
                        {
                            all_current_ans_lookups_v2
                                .insert(current_ans_lookup_v2.pk(), current_ans_lookup_v2);
                            all_ans_lookups_v2.push(ans_lookup_v2);
                        }
                    },
                    // For ANS V2, there are no delete resource changes
                    // 1. Unsetting a primary name will show up as a ReverseRecord write resource with empty fields
                    // 2. Name record v2 tokens are never deleted
                    _ => continue,
                }
            }
        }
    }
    // Boilerplate after this for diesel
    // Sort ans lookup values for postgres insert
    let mut all_current_ans_lookups = all_current_ans_lookups
        .into_values()
        .collect::<Vec<CurrentAnsLookup>>();
    let mut all_current_ans_primary_names = all_current_ans_primary_names
        .into_values()
        .collect::<Vec<CurrentAnsPrimaryName>>();
    let mut all_current_ans_lookups_v2 = all_current_ans_lookups_v2
        .into_values()
        .collect::<Vec<CurrentAnsLookupV2>>();
    let mut all_current_ans_primary_names_v2 = all_current_ans_primary_names_v2
        .into_values()
        .collect::<Vec<CurrentAnsPrimaryNameV2>>();

    all_current_ans_lookups.sort();
    all_current_ans_primary_names.sort();
    all_current_ans_lookups_v2.sort();
    all_current_ans_primary_names_v2.sort();
    (
        all_current_ans_lookups,
        all_ans_lookups,
        all_current_ans_primary_names,
        all_ans_primary_names,
        all_current_ans_lookups_v2,
        all_ans_lookups_v2,
        all_current_ans_primary_names_v2,
        all_ans_primary_names_v2,
    )
}
