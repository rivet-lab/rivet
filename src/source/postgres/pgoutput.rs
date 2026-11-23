//! The `pgoutput` logical-decoding message format — PURE bytes-in, message-out.

#![allow(dead_code)]

use crate::error::Result;
use crate::source::cdc::tx_buffer::{SpooledTail, TxBuffer, replay};

/// An LSN as PostgreSQL prints it (`X/X`, hex halves).
fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// One cell of a row image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cell {
    Null,
    /// The column was not transmitted because its TOASTed value did not change.
    ToastUnchanged,
    /// `t` — the type's TEXT output.
    Text(Vec<u8>),
    /// `b` — the type's BINARY output, decodable with `postgres_types::FromSql` against the OID `Relation` gave for this column.
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Column {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    /// Part of the replica identity — the wire says so, so rivet never has to ask the catalog which columns identify a row.
    pub is_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    Begin {
        final_lsn: u64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
    },
    Relation {
        relation_id: u32,
        namespace: String,
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        relation_id: u32,
        after: Vec<Cell>,
    },
    Update {
        relation_id: u32,
        /// Present only when the table's replica identity supplies one (`FULL`, or `DEFAULT` when a key column changed).
        before: Option<Vec<Cell>>,
        after: Vec<Cell>,
    },
    Delete {
        relation_id: u32,
        /// The key under `DEFAULT`, the whole row under `FULL`.
        before: Vec<Cell>,
    },
    Truncate {
        relation_ids: Vec<u32>,
    },
    /// A logical message — `pg_logical_emit_message()`; read with `messages=true`, a non-transactional one only once flushed.
    Logical {
        transactional: bool,
        prefix: String,
        content: Vec<u8>,
    },
    /// `O`rigin, `Y` type, and the streaming variants.
    Other(char),
}

struct Reader<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.i + n > self.b.len() {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                "pgoutput: message truncated — wanted {n} byte(s) at offset {}, have {}",
                self.i,
                self.b.len()
            );
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("2")))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8")))
    }
    /// A NUL-terminated string.
    fn cstr(&mut self) -> Result<String> {
        let start = self.i;
        while self.i < self.b.len() && self.b[self.i] != 0 {
            self.i += 1;
        }
        if self.i >= self.b.len() {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                "pgoutput: unterminated string at offset {start}"
            );
        }
        let s = String::from_utf8_lossy(&self.b[start..self.i]).into_owned();
        self.i += 1;
        Ok(s)
    }
    fn tuple(&mut self) -> Result<Vec<Cell>> {
        let n = self.u16()? as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => Cell::Null,
                b'u' => Cell::ToastUnchanged,
                tag @ (b't' | b'b') => {
                    let len = self.i32()?;
                    if len < 0 {
                        crate::rivet_bail!(
                            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                            "pgoutput: negative value length {len}"
                        );
                    }
                    let raw = self.take(len as usize)?.to_vec();
                    if tag == b'b' {
                        Cell::Binary(raw)
                    } else {
                        Cell::Text(raw)
                    }
                }
                other => crate::rivet_bail!(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "pgoutput: unknown tuple cell tag {:?} — the protocol version \
                     may be newer than this decoder",
                    other as char
                ),
            });
        }
        Ok(out)
    }
}

/// Decode ONE `pgoutput` message.
pub(crate) fn decode(bytes: &[u8]) -> Result<Message> {
    let mut r = Reader::new(bytes);
    Ok(match r.u8()? {
        b'B' => {
            let final_lsn = r.u64()?;
            let _commit_time = r.u64()?;
            Message::Begin {
                final_lsn,
                xid: r.u32()?,
            }
        }
        b'C' => {
            let _flags = r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            Message::Commit {
                commit_lsn,
                end_lsn,
            }
        }
        b'R' => {
            let relation_id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let _replica_identity = r.u8()?;
            let n = r.u16()? as usize;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                let flags = r.u8()?;
                columns.push(Column {
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    type_modifier: r.i32()?,
                    is_key: flags & 1 == 1,
                });
            }
            Message::Relation {
                relation_id,
                namespace,
                name,
                columns,
            }
        }
        b'I' => {
            let relation_id = r.u32()?;
            expect_tag(&mut r, b'N')?;
            Message::Insert {
                relation_id,
                after: r.tuple()?,
            }
        }
        b'U' => {
            let relation_id = r.u32()?;
            let mut before = None;
            let mut tag = r.u8()?;
            if tag == b'K' || tag == b'O' {
                before = Some(r.tuple()?);
                tag = r.u8()?;
            }
            if tag != b'N' {
                crate::rivet_bail!(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "pgoutput: UPDATE without a new tuple (tag {:?})",
                    tag as char
                );
            }
            Message::Update {
                relation_id,
                before,
                after: r.tuple()?,
            }
        }
        b'D' => {
            let relation_id = r.u32()?;
            let tag = r.u8()?;
            if tag != b'K' && tag != b'O' {
                crate::rivet_bail!(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "pgoutput: DELETE without an old tuple (tag {:?})",
                    tag as char
                );
            }
            Message::Delete {
                relation_id,
                before: r.tuple()?,
            }
        }
        b'T' => {
            let n = r.u32()? as usize;
            let _flags = r.u8()?;
            let mut relation_ids = Vec::with_capacity(n);
            for _ in 0..n {
                relation_ids.push(r.u32()?);
            }
            Message::Truncate { relation_ids }
        }
        b'M' => {
            let transactional = r.u8()? != 0;
            let _lsn = r.u64()?;
            let prefix = r.cstr()?;
            let len = r.u32()? as usize;
            Message::Logical {
                transactional,
                prefix,
                content: r.take(len)?.to_vec(),
            }
        }
        other => Message::Other(other as char),
    })
}

fn expect_tag(r: &mut Reader<'_>, want: u8) -> Result<()> {
    let got = r.u8()?;
    if got != want {
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
            "pgoutput: expected tuple tag {:?}, got {:?}",
            want as char,
            got as char
        );
    }
    Ok(())
}

/// Turn one BINARY `pgoutput` cell into a [`RivetValue`], dispatching on the type OID the `Relation` message carried.
pub(crate) fn value_from_binary(
    oid: u32,
    raw: &[u8],
) -> Result<crate::source::cdc::value::RivetValue> {
    use crate::source::cdc::value::RivetValue as V;
    use postgres_types::{FromSql, Type};

    let Some(ty) = Type::from_oid(oid) else {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
            "pgoutput: no PostgreSQL type for OID {oid}"
        );
    };
    fn de<'a, T: FromSql<'a>>(ty: &Type, raw: &'a [u8], what: &str) -> Result<T> {
        T::from_sql(ty, raw).map_err(|e| {
            crate::error::coded(
                crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
                format!("pgoutput: decoding {what} (oid {}): {e}", ty.oid()),
            )
        })
    }
    Ok(match oid {
        16 => V::Bool(de::<bool>(&ty, raw, "bool")?),
        20 => V::Int(de::<i64>(&ty, raw, "int8")?),
        21 => V::Int(de::<i16>(&ty, raw, "int2")? as i64),
        23 => V::Int(de::<i32>(&ty, raw, "int4")? as i64),
        700 => V::Float(de::<f32>(&ty, raw, "float4")? as f64),
        701 => V::Float(de::<f64>(&ty, raw, "float8")?),
        1184 => {
            V::DateTime(de::<chrono::DateTime<chrono::Utc>>(&ty, raw, "timestamptz")?.naive_utc())
        }
        1114 => V::DateTime(de::<chrono::NaiveDateTime>(&ty, raw, "timestamp")?),
        1082 => V::DateTime(
            de::<chrono::NaiveDate>(&ty, raw, "date")?
                .and_hms_opt(0, 0, 0)
                .expect("midnight is a valid time"),
        ),
        2950 => V::Bytes(de::<uuid::Uuid>(&ty, raw, "uuid")?.as_bytes().to_vec()),
        17 => V::Bytes(de::<Vec<u8>>(&ty, raw, "bytea")?),
        114 | 3802 => V::Bytes(
            de::<super::arrow_convert::PgJsonRawText>(&ty, raw, "json")?
                .0
                .as_bytes()
                .to_vec(),
        ),
        19 | 25 | 1042 | 1043 => V::Bytes(de::<String>(&ty, raw, "text")?.into_bytes()),
        26 => V::Int(de::<u32>(&ty, raw, "oid")? as i64),
        1700 => V::Bytes(numeric_text(raw)?.into_bytes()),
        1083 => V::TimeMicros(time_micros(de::<i64>(&Type::INT8, raw, "time")?)?),
        1186 => {
            let iv = de::<super::arrow_convert::PgInterval>(&ty, raw, "interval")?;
            V::Bytes(
                super::arrow_convert::pg_interval_to_iso8601(iv.months, iv.days, iv.microseconds)
                    .into_bytes(),
            )
        }
        1005 => array_of(de::<Vec<Option<i16>>>(&ty, raw, "int2[]")?, |v| {
            V::Int(v as i64)
        }),
        1007 => array_of(de::<Vec<Option<i32>>>(&ty, raw, "int4[]")?, |v| {
            V::Int(v as i64)
        }),
        1016 => array_of(de::<Vec<Option<i64>>>(&ty, raw, "int8[]")?, V::Int),
        1000 => array_of(de::<Vec<Option<bool>>>(&ty, raw, "bool[]")?, V::Bool),
        1021 => array_of(de::<Vec<Option<f32>>>(&ty, raw, "float4[]")?, |v| {
            V::Float(v as f64)
        }),
        1022 => array_of(de::<Vec<Option<f64>>>(&ty, raw, "float8[]")?, V::Float),
        1009 | 1014 | 1015 => array_of(de::<Vec<Option<String>>>(&ty, raw, "text[]")?, |v| {
            V::Bytes(v.into_bytes())
        }),
        _ => crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
            "pgoutput: column type `{}` (oid {oid}) has no binary decoder. Refusing \
             rather than passing the raw bytes through as text — an unmapped type \
             that ships as hex is a column every count and sum agrees with and no \
             consumer can read. Enums and domains have no fixed OID and are not \
             decoded here yet.",
            ty.name()
        ),
    })
}

/// Raw `time` microseconds, refusing 24:00:00 (a day-length value no time-of-day can hold).
fn time_micros(us: i64) -> Result<i64> {
    if !crate::types::is_time_of_day(us) {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
            "pgoutput: a `time` of {us} microseconds is outside 00:00:00..24:00:00 \
             (24:00:00 has no time-of-day reading); map it to a representable value \
             in the source"
        );
    }
    Ok(us)
}

fn array_of<T>(
    items: Vec<Option<T>>,
    f: impl Fn(T) -> crate::source::cdc::value::RivetValue,
) -> crate::source::cdc::value::RivetValue {
    use crate::source::cdc::value::RivetValue as V;
    V::Array(items.into_iter().map(|o| o.map_or(V::Null, &f)).collect())
}

/// PostgreSQL binary `numeric` -> the batch reader's decimal text (NaN/±Infinity by name).
fn numeric_text(raw: &[u8]) -> Result<String> {
    use crate::source::pg_numeric_wire::{
        numeric_wire_normalized_plain, numeric_wire_special_text,
    };
    numeric_wire_special_text(raw)
        .map(str::to_string)
        .or_else(|| numeric_wire_normalized_plain(raw))
        .ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                format!("pgoutput: malformed numeric payload ({} bytes)", raw.len()),
            )
        })
}

/// The prefix rivet stamps on its own logical messages, so a barrier emitted by something else on the same database is never mistaken for ours.
pub(crate) const BARRIER_PREFIX: &str = "rivet";

/// Is this message OUR barrier, carrying `nonce`?
pub(crate) fn is_our_barrier(msg: &Message, nonce: &str) -> bool {
    matches!(
        msg,
        Message::Logical { prefix, content, .. }
            if prefix == BARRIER_PREFIX && content == nonce.as_bytes()
    )
}

/// Fill an UPDATE's unchanged-TOAST cells from the pre-image, where it has them.
fn merge_unchanged_toast(after: &[Cell], before: Option<&[Cell]>) -> Vec<Cell> {
    let Some(before) = before else {
        return after.to_vec();
    };
    after
        .iter()
        .enumerate()
        .map(|(i, cell)| match (cell, before.get(i)) {
            (Cell::ToastUnchanged, Some(pre)) => pre.clone(),
            _ => cell.clone(),
        })
        .collect()
}

/// Assembles [`Message`]s into [`ChangeEvent`]s, holding the relation cache the protocol requires.
pub(crate) struct Assembler {
    relations: std::collections::HashMap<u32, RelationInfo>,
    /// The open transaction's events: in memory to the cap, then on disk, until its `Commit`.
    tx: TxBuffer,
    /// A refusal inside the CURRENT transaction poisons the rest of it.
    poisoned: Option<(crate::error::Code, String)>,
    /// The open transaction's commit LSN as text, for the spill log lines.
    at: String,
}

struct RelationInfo {
    schema: String,
    table: String,
    columns: Vec<Column>,
    /// Built once per relation, shared by every event that references it.
    names: std::sync::Arc<[String]>,
}

/// A committed transaction: its in-memory head, then a tail on disk when it crossed the cap.
pub(crate) struct Closed {
    pub head: Vec<crate::source::cdc::ChangeEvent>,
    pub tail: Option<SpooledTail>,
}

impl std::fmt::Debug for Closed {
    /// The head's length and whether a spilled tail follows, never the rows.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Closed {{ head: {} event(s), spilled: {} }}",
            self.head.len(),
            self.tail.is_some()
        )
    }
}

impl Default for Assembler {
    /// An assembler with no spill directory: a transaction past the cap is refused.
    fn default() -> Self {
        Self::new(None)
    }
}

/// The next event of a spilled tail, decoded; `None` once the tail is done.
pub(crate) fn next_spooled(
    tail: &mut Option<SpooledTail>,
) -> Result<Option<crate::source::cdc::ChangeEvent>> {
    replay(tail, crate::source::cdc::spill::decode_event)
}

impl Assembler {
    /// An assembler whose oversized transactions spill under `spill_dir` (`None` refuses them).
    pub(crate) fn new(spill_dir: Option<std::path::PathBuf>) -> Self {
        Self {
            relations: std::collections::HashMap::new(),
            tx: TxBuffer::new(crate::source::cdc::CdcEngine::Postgres, spill_dir),
            poisoned: None,
            at: String::new(),
        }
    }

    /// Feed one message. Returns the transaction when it COMMITS, and `None` before then.
    pub(crate) fn push(&mut self, msg: Message) -> Result<Option<Closed>> {
        if let Some((code, why)) = self.poisoned.clone()
            && !matches!(msg, Message::Begin { .. })
        {
            if matches!(msg, Message::Commit { .. }) {
                self.tx.clear();
            }
            crate::rivet_bail!(code, "{why}");
        }
        let out = self.push_inner(msg);
        if let Err(e) = &out {
            self.poisoned = Some((
                crate::error::code_of(e).unwrap_or(crate::error::codes::INTERNAL_SOURCE_INVARIANT),
                format!("{e:#}"),
            ));
            self.tx.clear();
        }
        out
    }

    fn push_inner(&mut self, msg: Message) -> Result<Option<Closed>> {
        use crate::source::cdc::ChangeOp;
        match msg {
            Message::Begin { final_lsn, .. } => {
                self.poisoned = None;
                let open = self.tx.head().len() + self.tx.spilled_rows().unwrap_or(0);
                if open != 0 {
                    crate::rivet_bail!(
                        crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                        "pgoutput: BEGIN with {open} row(s) still open from an uncommitted \
                         transaction — refusing rather than shipping a partial one"
                    );
                }
                self.at = lsn_text(final_lsn);
                Ok(None)
            }
            Message::Relation {
                relation_id,
                namespace,
                name,
                columns,
            } => {
                let names: std::sync::Arc<[String]> =
                    columns.iter().map(|c| c.name.clone()).collect();
                self.relations.insert(
                    relation_id,
                    RelationInfo {
                        schema: namespace,
                        table: name,
                        columns,
                        names,
                    },
                );
                Ok(None)
            }
            Message::Insert { relation_id, after } => {
                let after = self.decode_tuple(relation_id, &after)?;
                self.buffer(ChangeOp::Insert, relation_id, None, Some(after))
            }
            Message::Update {
                relation_id,
                before,
                after,
            } => {
                let after = merge_unchanged_toast(&after, before.as_deref());
                let before = before
                    .as_deref()
                    .map(|t| self.decode_tuple(relation_id, t))
                    .transpose()?;
                let after = self.decode_tuple(relation_id, &after)?;
                self.buffer(ChangeOp::Update, relation_id, before, Some(after))
            }
            Message::Delete {
                relation_id,
                before,
            } => {
                let before = self.decode_tuple(relation_id, &before)?;
                self.buffer(ChangeOp::Delete, relation_id, Some(before), None)
            }
            Message::Commit { commit_lsn, .. } => self.close(commit_lsn).map(Some),
            Message::Truncate { relation_ids } => {
                let named: Vec<String> = relation_ids
                    .iter()
                    .map(|id| {
                        self.relations.get(id).map_or_else(
                            || format!("oid {id}"),
                            |r| format!("{}.{}", r.schema, r.table),
                        )
                    })
                    .collect();
                Err(crate::source::cdc::checkpoint_identity::truncated(format!(
                    "pgoutput: TRUNCATE of {} — this reader cannot represent it as a \
                     change. Every row the truncate removed would sit in the \
                     destination with no DELETE to retract it. {}",
                    named.join(", "),
                    crate::source::cdc::checkpoint_identity::RECOVER
                )))
            }
            Message::Logical { .. } | Message::Other(_) => Ok(None),
        }
    }

    /// Decode-time event of the open transaction: the relation in force NOW names it, so a relation re-sent mid-transaction cannot rename a spilled row.
    fn buffer(
        &mut self,
        op: crate::source::cdc::ChangeOp,
        relation_id: u32,
        before: Option<Vec<crate::source::cdc::value::RivetValue>>,
        after: Option<Vec<crate::source::cdc::value::RivetValue>>,
    ) -> Result<Option<Closed>> {
        let rel = self.relations.get(&relation_id).ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                format!("pgoutput: relation {relation_id} vanished between decode and buffer"),
            )
        })?;
        let ev = crate::source::cdc::ChangeEvent {
            op,
            schema: rel.schema.clone(),
            table: rel.table.clone(),
            before,
            after,
            position: crate::source::cdc::Position::new(serde_json::Value::Null),
            committed: false,
            image_names: Some(rel.names.clone()),
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
            image_labels: None,
        };
        self.tx
            .push(ev, crate::source::cdc::spill::encode_event, &self.at)?;
        Ok(None)
    }

    /// Close the open transaction at `commit_lsn`: the position on every row, `committed` on the LAST only (on disk when it spilled).
    fn close(&mut self, commit_lsn: u64) -> Result<Closed> {
        let text = lsn_text(commit_lsn);
        let position = crate::source::cdc::Position::new(serde_json::json!({ "lsn": text }));
        let (head, tail) = self.tx.close_transaction(&position, &text)?;
        Ok(Closed { head, tail })
    }

    fn decode_tuple(
        &self,
        relation_id: u32,
        cells: &[Cell],
    ) -> Result<Vec<crate::source::cdc::value::RivetValue>> {
        use crate::source::cdc::value::RivetValue as V;
        let Some(rel) = self.relations.get(&relation_id) else {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                "pgoutput: a row referenced relation {relation_id} before its \
                 Relation message — the protocol sends one first, so this means a \
                 message was dropped"
            );
        };
        if cells.len() != rel.columns.len() {
            crate::rivet_bail!(
                crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                "pgoutput: {}.{}: a row image carries {} value(s) under {} column(s)",
                rel.schema,
                rel.table,
                cells.len(),
                rel.columns.len()
            );
        }
        cells
            .iter()
            .zip(&rel.columns)
            .map(|(cell, col)| match cell {
                Cell::Null => Ok(V::Null),
                Cell::Binary(raw) => value_from_binary(col.type_oid, raw),
                Cell::ToastUnchanged => crate::rivet_bail!(
                    crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
                    "pgoutput: {}.{}: column `{}` is an unchanged TOAST value this \
                     UPDATE did not carry, and no pre-image holds it. Set REPLICA \
                     IDENTITY FULL on the table so the old row travels with the \
                     change, then re-capture.",
                    rel.schema,
                    rel.table,
                    col.name
                ),
                Cell::Text(_) => crate::rivet_bail!(
                    crate::error::codes::INTERNAL_SOURCE_INVARIANT,
                    "pgoutput: {}.{}: column `{}` arrived in TEXT form. The slot must \
                     be read with `binary=true`; the text rendering is shaped by the \
                     session's datestyle/bytea_output/TimeZone, which is the defect \
                     class this reader exists to remove.",
                    rel.schema,
                    rel.table,
                    col.name
                ),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    /// Every event of a closed transaction in order, the spilled tail replayed after the head.
    fn events(closed: Option<Closed>) -> Vec<crate::source::cdc::ChangeEvent> {
        let Some(Closed { head, mut tail }) = closed else {
            return Vec::new();
        };
        let mut out = head;
        while let Some(ev) = next_spooled(&mut tail).expect("replay") {
            out.push(ev);
        }
        out
    }

    use super::*;

    /// Real bytes from a real server, not a hand-built fixture.
    fn fixture() -> Vec<Vec<u8>> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/messages.hex");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let out: Vec<Vec<u8>> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect()
            })
            .collect();
        assert!(
            out.len() >= 15,
            "the fixture is inert: {} messages is fewer than the capture recorded, \
             so the assertions below would grade almost nothing",
            out.len()
        );
        out
    }

    /// Every message in the capture decodes, and the SHAPE is what the wire said.
    #[test]
    fn every_captured_message_decodes_and_the_counts_match_the_capture() {
        let msgs: Vec<Message> = fixture()
            .iter()
            .map(|b| decode(b).unwrap_or_else(|e| panic!("decode {b:02x?}: {e}")))
            .collect();
        let count = |f: fn(&Message) -> bool| msgs.iter().filter(|m| f(m)).count();
        assert_eq!(count(|m| matches!(m, Message::Begin { .. })), 4);
        assert_eq!(count(|m| matches!(m, Message::Commit { .. })), 4);
        assert_eq!(count(|m| matches!(m, Message::Relation { .. })), 3);
        assert_eq!(count(|m| matches!(m, Message::Insert { .. })), 3);
        assert_eq!(count(|m| matches!(m, Message::Update { .. })), 2);
        assert_eq!(count(|m| matches!(m, Message::Delete { .. })), 1);
        assert_eq!(count(|m| matches!(m, Message::Truncate { .. })), 1);
        assert_eq!(
            count(|m| matches!(m, Message::Other(_))),
            0,
            "an unrecognised message in a capture of ordinary DML means the decoder \
             is behind the protocol, which is exactly how a bump becomes a gap"
        );
    }

    /// `Relation` carries the column TYPES and the replica-identity flags.
    #[test]
    fn relation_names_the_columns_their_type_oids_and_which_ones_are_the_key() {
        let rel = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .find_map(|m| match m {
                Message::Relation {
                    namespace,
                    name,
                    columns,
                    ..
                } => Some((namespace, name, columns)),
                _ => None,
            })
            .expect("the capture contains a Relation");
        assert_eq!(
            (rel.0.as_str(), rel.1.as_str()),
            ("public", "rivet_pgout_fx")
        );
        let names: Vec<&str> = rel.2.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "id", "txt", "arr", "ts", "u", "b", "n", "f", "ok", "d", "j", "big"
            ]
        );
        let oids: Vec<u32> = rel.2.iter().map(|c| c.type_oid).collect();
        assert_eq!(
            oids,
            [20, 25, 1007, 1184, 2950, 17, 1700, 701, 16, 1082, 3802, 25]
        );
        let keys: Vec<&str> = rel
            .2
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            keys,
            ["id"],
            "the wire states the replica identity; rivet asking the catalog for it \
             is the `names are labels, catalogs are truth` detour this removes"
        );
    }

    /// The two cases `test_decoding` renders as ordinary text, and so cannot tell from a column that genuinely holds that text.
    #[test]
    fn a_null_and_an_unchanged_toast_are_their_own_tags_not_strings() {
        let msgs: Vec<Message> = fixture().iter().filter_map(|b| decode(b).ok()).collect();

        let nulls = msgs.iter().any(|m| match m {
            Message::Insert { after, .. } => {
                after.iter().filter(|c| **c == Cell::Null).count() >= 7
            }
            _ => false,
        });
        assert!(
            nulls,
            "the capture's all-NULL INSERT must decode to `Cell::Null` cells. \
             `test_decoding` writes the string `null` here, indistinguishable from a \
             text column whose value is `null`"
        );

        let toast = msgs.iter().any(|m| match m {
            Message::Update { after, .. } => after.contains(&Cell::ToastUnchanged),
            _ => false,
        });
        assert!(
            toast,
            "the capture's DEFAULT-identity UPDATE must carry an unchanged-TOAST \
             cell. `test_decoding` writes the STRING `unchanged-toast-datum` here, \
             which a column holding that text produces identically — the collision \
             this format removes"
        );

        for m in &msgs {
            let cells: Vec<&Cell> = match m {
                Message::Insert { after, .. } => after.iter().collect(),
                Message::Update { before, after, .. } => {
                    before.iter().flatten().chain(after.iter()).collect()
                }
                Message::Delete { before, .. } => before.iter().collect(),
                _ => vec![],
            };
            for c in cells {
                if let Cell::Text(v) | Cell::Binary(v) = c {
                    assert!(
                        !String::from_utf8_lossy(v).contains("unchanged-toast-datum"),
                        "a VALUE cell must never carry the text marker"
                    );
                }
            }
        }
    }

    /// An UPDATE's pre-image is OPTIONAL, and absent is not empty.
    #[test]
    fn an_update_reports_a_missing_pre_image_as_none_not_as_an_empty_tuple() {
        let updates: Vec<Option<Vec<Cell>>> = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .filter_map(|m| match m {
                Message::Update { before, .. } => Some(before),
                _ => None,
            })
            .collect();
        assert_eq!(updates.len(), 2, "the capture has one UPDATE per identity");
        assert!(
            updates.iter().any(|b| b.is_none()),
            "the DEFAULT-identity UPDATE must report NO pre-image"
        );
        let full = updates
            .iter()
            .flatten()
            .next()
            .expect("the FULL-identity UPDATE must carry one");
        assert_eq!(
            full.len(),
            12,
            "REPLICA IDENTITY FULL sends every column, so the pre-image is the whole \
             row — an empty tuple here would read as `the row had no values`"
        );
    }

    /// A DELETE carries its values in the pre-image, and nothing after.
    #[test]
    fn a_delete_carries_only_a_before_image() {
        let del = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .find_map(|m| match m {
                Message::Delete { before, .. } => Some(before),
                _ => None,
            })
            .expect("the capture contains a DELETE");
        assert_eq!(del.len(), 12, "a tuple is always as wide as its relation");
        assert!(
            del.iter().filter(|c| **c == Cell::Null).count() >= 10,
            "under DEFAULT identity only the key carries a value; the rest are the \
             NULL tag, not absent cells"
        );
    }

    /// Truncation is its OWN message, not a DDL statement to be string-matched.
    #[test]
    fn truncate_is_a_typed_message_carrying_relation_ids() {
        let ids = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .find_map(|m| match m {
                Message::Truncate { relation_ids } => Some(relation_ids),
                _ => None,
            })
            .expect("the capture contains a TRUNCATE");
        assert_eq!(ids.len(), 1, "one table was truncated");
        assert_ne!(ids[0], 0, "a relation OID is never zero");
    }

    /// Every value type, decoded from the type's OWN BINARY FORM.
    #[test]
    fn every_binary_value_decodes_through_its_type_oid() {
        use crate::source::cdc::value::RivetValue as V;

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/binary_values.hex");
        let msgs: Vec<Message> = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let b: Vec<u8> = (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect();
                decode(&b).expect("decode")
            })
            .collect();

        let cols = msgs
            .iter()
            .find_map(|m| match m {
                Message::Relation { columns, .. } => Some(columns.clone()),
                _ => None,
            })
            .expect("a Relation");
        let inserts: Vec<&Vec<Cell>> = msgs
            .iter()
            .filter_map(|m| match m {
                Message::Insert { after, .. } => Some(after),
                _ => None,
            })
            .collect();
        assert_eq!(
            inserts.len(),
            3,
            "three rows in one transaction — see the framing test for why"
        );

        let decoded: Vec<V> = inserts[0]
            .iter()
            .zip(&cols)
            .map(|(cell, col)| match cell {
                Cell::Null => V::Null,
                Cell::Binary(raw) => value_from_binary(col.type_oid, raw)
                    .unwrap_or_else(|e| panic!("{}: {e}", col.name)),
                other => panic!("{}: expected a BINARY cell, got {other:?}", col.name),
            })
            .collect();

        let by = |name: &str| -> &V {
            let i = cols.iter().position(|c| c.name == name).expect("column");
            &decoded[i]
        };
        assert_eq!(
            by("id"),
            &V::Int(52),
            "the binary capture re-seeds with 52/53/54"
        );
        assert_eq!(by("txt"), &V::Bytes(br#"a,b "q""#.to_vec()));
        assert_eq!(by("ok"), &V::Bool(true));
        assert_eq!(by("f"), &V::Float(2.5));
        assert_eq!(
            by("arr"),
            &V::Array(vec![V::Int(1), V::Null, V::Int(3)]),
            "the array's inner NULL is a real NULL, not the four letters `NULL` that \
             the text form is indistinguishable from"
        );
        assert_eq!(
            by("u"),
            &V::Bytes(vec![
                0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55,
                0x55, 0x55
            ]),
            "16 raw bytes — the text form is 36 chars, which is what turned a whole \
             uuid column NULL when a builder guarded on length"
        );
        assert_eq!(
            by("b"),
            &V::Bytes(vec![0x00, 0xff]),
            "raw bytes, no `\\x` prefix"
        );
        assert_eq!(
            by("n"),
            &V::Bytes(b"-12345.6789".to_vec()),
            "numeric is exact decimal text, sign and scale preserved"
        );
        assert_eq!(
            by("j"),
            &V::Bytes(br#"{"k": [1, 2]}"#.to_vec()),
            "jsonb is the server's own text, as the batch reader emits it"
        );
        assert_eq!(
            by("ts"),
            &V::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 3, 1)
                    .unwrap()
                    .and_hms_opt(7, 0, 0)
                    .unwrap()
            )
        );
        assert_eq!(
            by("d"),
            &V::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 3, 2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            )
        );

        assert!(
            inserts[1].iter().skip(1).all(|c| *c == Cell::Null),
            "every nullable column of the all-NULL row must be `Cell::Null`"
        );

        let err = value_from_binary(3220, b"\x00\x00\x00\x01").expect_err("pg_lsn has no arm");
        assert!(
            format!("{err:#}").contains("no binary decoder"),
            "an unmapped type must name itself and refuse: {err:#}"
        );
    }

    /// Hand-built wire bytes against hand-written literals, per arm the fixture lacks.
    #[test]
    fn binary_arms_decode_hand_built_wire_bytes_to_literal_values() {
        use crate::source::cdc::value::RivetValue as V;
        let bytes = |oid: u32, raw: &[u8]| match value_from_binary(oid, raw).unwrap() {
            V::Bytes(b) => String::from_utf8(b).unwrap(),
            other => panic!("oid {oid}: expected Bytes, got {other:?}"),
        };

        assert_eq!(
            bytes(1700, &[0, 1, 0xFF, 0xFF, 0, 0, 0, 1, 0x13, 0x88]),
            "0.5"
        );
        assert_eq!(
            bytes(1700, &[0, 1, 0xFF, 0xFE, 0, 0, 0, 5, 0x13, 0x88]),
            "0.00005"
        );
        assert_eq!(bytes(1700, &[0, 0, 0, 0, 0xC0, 0, 0, 0]), "NaN");
        assert_eq!(
            bytes(1700, &[0, 2, 0, 0, 0, 0, 0, 2, 0, 1, 0x13, 0x88]),
            "1.50",
            "numeric keeps its display scale: 1.50 renders as 1.50"
        );
        assert!(
            value_from_binary(1700, &[0, 1]).is_err(),
            "truncated numeric"
        );

        let doc = br#"{"b":1,  "a":0.12345678901234567890}"#;
        assert_eq!(bytes(114, doc).as_bytes(), doc, "json is byte-identical");
        let mut jb = vec![1u8];
        jb.extend_from_slice(doc);
        assert_eq!(
            bytes(3802, &jb).as_bytes(),
            doc,
            "jsonb version byte stripped"
        );
        jb[0] = 2;
        assert!(
            value_from_binary(3802, &jb).is_err(),
            "jsonb version 2 refused"
        );

        let mut iv = vec![0u8; 8];
        iv.extend_from_slice(&3i32.to_be_bytes());
        iv.extend_from_slice(&14i32.to_be_bytes());
        assert_eq!(bytes(1186, &iv), "P1Y2M3D");

        assert_eq!(
            value_from_binary(1083, &43_200_500_000i64.to_be_bytes()).unwrap(),
            V::TimeMicros(43_200_500_000)
        );
        assert_eq!(
            value_from_binary(1083, &86_399_999_999i64.to_be_bytes()).unwrap(),
            V::TimeMicros(86_399_999_999)
        );
        let err = value_from_binary(1083, &86_400_000_000i64.to_be_bytes())
            .expect_err("24:00:00 must be refused, never wrapped to midnight");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_SOURCE_CDC_CELL_UNSUPPORTED"),
            "{err:#}"
        );
        assert_eq!(
            value_from_binary(26, &12345u32.to_be_bytes()).unwrap(),
            V::Int(12345)
        );
        assert_eq!(bytes(19, b"pg_class"), "pg_class");

        let mut arr = Vec::new();
        for w in [1u32, 0, 1043, 1, 1, 2] {
            arr.extend_from_slice(&w.to_be_bytes());
        }
        arr.extend_from_slice(b"xy");
        assert_eq!(
            value_from_binary(1015, &arr).unwrap(),
            V::Array(vec![V::Bytes(b"xy".to_vec())])
        );

        let mut f8 = Vec::new();
        for w in [1u32, 0, 701, 1, 1, 8] {
            f8.extend_from_slice(&w.to_be_bytes());
        }
        f8.extend_from_slice(&2.5f64.to_be_bytes());
        assert_eq!(
            value_from_binary(1022, &f8).unwrap(),
            V::Array(vec![V::Float(2.5)])
        );
    }

    /// A transaction yields nothing until its commit, and `committed` marks only its last event.
    #[test]
    fn a_transaction_yields_nothing_until_its_commit_and_marks_only_its_last_event() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/binary_values.hex");
        let msgs: Vec<Message> = std::fs::read_to_string(&path)
            .expect("fixture")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let b: Vec<u8> = (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect();
                decode(&b).expect("decode")
            })
            .collect();

        let mut a = Assembler::default();
        let mut yielded: Vec<Vec<crate::source::cdc::ChangeEvent>> = Vec::new();
        let mut refused: Vec<String> = Vec::new();
        for m in msgs {
            let is_commit = matches!(m, Message::Commit { .. });
            match a.push(m) {
                Ok(out) if is_commit => yielded.push(events(out)),
                Ok(out) => assert!(
                    out.is_none(),
                    "nothing may be yielded before the COMMIT — an event released \
                     early cannot carry the transaction's own commit position"
                ),
                Err(e) => refused.push(format!("{e:#}")),
            }
        }

        let causes: std::collections::BTreeSet<&str> = refused.iter().map(String::as_str).collect();
        assert_eq!(
            causes.len(),
            2,
            "two distinct refusals — the unchanged TOAST and the TRUNCATE: {causes:#?}"
        );
        assert!(
            causes.iter().any(|e| e.contains("unchanged TOAST")),
            "the DEFAULT-identity UPDATE must refuse, naming the column: {refused:?}"
        );
        assert!(
            causes.iter().any(|e| e.contains("TRUNCATE")),
            "a TRUNCATE has no per-row representation: {refused:?}"
        );

        let real: Vec<&Vec<crate::source::cdc::ChangeEvent>> =
            yielded.iter().filter(|t| !t.is_empty()).collect();
        assert_eq!(
            real.iter().map(|t| t.len()).collect::<Vec<_>>(),
            vec![3, 1],
            "MULTI-row transactions, deliberately: with one row per transaction \
             `i == last`, `true` and `i == 0` are the same answer and every one of \
             those mutants survives — measured, and the sixth time this session a \
             fixture had to cross a threshold before the assertion could bite"
        );

        for tx in &real {
            let flags: Vec<bool> = tx.iter().map(|e| e.committed).collect();
            let want: Vec<bool> = (0..tx.len()).map(|i| i == tx.len() - 1).collect();
            assert_eq!(
                flags, want,
                "exactly the LAST event of a transaction is `committed`; setting it \
                 on every event is what let a crash advance the slot past a commit \
                 whose tail was never flushed"
            );
            let first = &tx[0].position;
            assert!(
                tx.iter().all(|e| &e.position == first),
                "one transaction, one commit position — a per-event position would \
                 let the checkpoint land mid-transaction"
            );
        }

        let ev = &real[0][0];
        assert_eq!(
            (ev.schema.as_str(), ev.table.as_str()),
            ("public", "rivet_pgout_fx")
        );
        assert_eq!(
            ev.image_names.as_deref().map(|n| n.len()),
            Some(12),
            "the image is name-addressed from the Relation message, so the sink maps \
             by NAME and the positional-corruption class cannot arise"
        );
    }

    fn assembler_capped_at(
        spill_dir: Option<std::path::PathBuf>,
        caps: (usize, usize),
    ) -> Assembler {
        Assembler {
            tx: TxBuffer::with_caps(crate::source::cdc::CdcEngine::Postgres, spill_dir, caps),
            ..Assembler::new(None)
        }
    }

    fn rel_msg(names: &[&str]) -> Message {
        Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: names
                .iter()
                .enumerate()
                .map(|(i, n)| Column {
                    name: n.to_string(),
                    type_oid: if i == 0 { 20 } else { 25 },
                    type_modifier: -1,
                    is_key: i == 0,
                })
                .collect(),
        }
    }

    fn row_msg(id: i64, text: &str) -> Message {
        Message::Insert {
            relation_id: 1,
            after: vec![
                Cell::Binary(id.to_be_bytes().to_vec()),
                Cell::Binary(text.as_bytes().to_vec()),
            ],
        }
    }

    /// A transaction past the cap spills, replays in order, and keeps each row's own relation across a mid-transaction re-send.
    #[test]
    fn a_spilled_transaction_replays_in_order_under_the_relation_each_row_arrived_with() {
        let d = tempfile::tempdir().expect("dir");
        let mut a = assembler_capped_at(Some(d.path().to_path_buf()), (2, usize::MAX));
        a.push(rel_msg(&["id", "name"])).expect("relation");
        a.push(Message::Begin {
            final_lsn: 0x1_0000_00A0,
            xid: 7,
        })
        .expect("begin");
        a.push(row_msg(1, "a")).expect("row 1");
        a.push(row_msg(2, "b")).expect("row 2");
        a.push(rel_msg(&["id", "renamed"]))
            .expect("relation re-sent");
        a.push(row_msg(3, "c")).expect("row 3");
        a.push(row_msg(4, "d")).expect("row 4");
        let closed = a
            .push(Message::Commit {
                commit_lsn: 0x1_0000_00A0,
                end_lsn: 0x1_0000_00B0,
            })
            .expect("commit");
        assert!(
            closed.as_ref().is_some_and(|c| c.tail.is_some()),
            "four rows at a cap of two must spill: {closed:?}"
        );
        let got = events(closed);
        let seen: Vec<(i64, String, bool, String)> = got
            .iter()
            .map(|e| {
                let id = match e.after.as_ref().and_then(|v| v.first()) {
                    Some(crate::source::cdc::value::RivetValue::Int(i)) => *i,
                    other => panic!("id cell: {other:?}"),
                };
                (
                    id,
                    e.image_names.as_ref().expect("names")[1].clone(),
                    e.committed,
                    format!("{:?}", e.position),
                )
            })
            .collect();
        let pos = format!("{:?}", got[0].position);
        assert!(pos.contains("1/A0"), "{pos}");
        assert_eq!(
            seen,
            vec![
                (1, "name".to_string(), false, pos.clone()),
                (2, "name".to_string(), false, pos.clone()),
                (3, "renamed".to_string(), false, pos.clone()),
                (4, "renamed".to_string(), true, pos),
            ]
        );
    }

    /// Past the cap with no spill directory the transaction is refused naming the cap, and the next one assembles.
    #[test]
    fn a_transaction_past_the_cap_without_a_spill_dir_is_refused_then_the_next_assembles() {
        let mut a = assembler_capped_at(None, (1, usize::MAX));
        a.push(rel_msg(&["id", "name"])).expect("relation");
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        a.push(row_msg(1, "a")).expect("row 1");
        let err = a.push(row_msg(2, "b")).expect_err("past the cap");
        assert!(format!("{err:#}").contains("more than 1 rows"), "{err:#}");
        a.push(Message::Commit {
            commit_lsn: 1,
            end_lsn: 1,
        })
        .expect_err("a poisoned transaction refuses its commit");
        a.push(Message::Begin {
            final_lsn: 2,
            xid: 2,
        })
        .expect("begin");
        a.push(row_msg(3, "c")).expect("row");
        let got = events(
            a.push(Message::Commit {
                commit_lsn: 2,
                end_lsn: 2,
            })
            .expect("commit"),
        );
        assert_eq!(got.len(), 1);
        assert!(got[0].committed);
    }

    /// The relation cache is REQUIRED by the protocol, and its absence is an error.
    #[test]
    fn a_row_before_its_relation_message_is_refused_not_guessed() {
        let mut a = Assembler::default();
        let err = a
            .push(Message::Insert {
                relation_id: 999,
                after: vec![Cell::Null],
            })
            .expect_err("a row with no known relation must fail");
        assert!(
            format!("{err:#}").contains("before its Relation message"),
            "the error must name the cause; guessing a schema/table here routes \
             events to a relation nobody configured: {err:#}"
        );
    }

    /// An unchanged-TOAST cell is refused with the column named, not written out.
    #[test]
    fn an_unchanged_toast_cell_is_refused_and_names_the_column() {
        let mut a = Assembler::default();
        a.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![Column {
                name: "big".into(),
                type_oid: 25,
                type_modifier: -1,
                is_key: false,
            }],
        })
        .expect("relation");
        let err = a
            .push(Message::Update {
                relation_id: 1,
                before: None,
                after: vec![Cell::ToastUnchanged],
            })
            .expect_err("an unchanged TOAST value is not a value");
        let text = format!("{err:#}");
        assert!(
            text.contains("big") && text.contains("REPLICA IDENTITY FULL"),
            "{text}"
        );
    }

    /// A barrier is recognised by rivet's prefix and the run's nonce, and by nothing else.
    #[test]
    fn a_barrier_is_recognised_by_prefix_and_nonce_and_by_nothing_else() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/binary_values.hex");
        let msgs: Vec<Message> = std::fs::read_to_string(&path)
            .expect("fixture")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let b: Vec<u8> = (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect();
                decode(&b).expect("decode")
            })
            .collect();

        let barrier = msgs
            .iter()
            .find(|m| matches!(m, Message::Logical { .. }))
            .expect("the capture must carry a barrier — it is emitted with `messages=true`");
        let Message::Logical {
            transactional,
            prefix,
            content,
        } = barrier
        else {
            unreachable!()
        };
        assert!(
            !transactional,
            "a barrier is emitted non-transactionally, so it \
             lands at a definite point rather than at some commit's"
        );
        assert_eq!(prefix, BARRIER_PREFIX);
        assert_eq!(content, b"fixture-barrier-nonce");

        assert!(is_our_barrier(barrier, "fixture-barrier-nonce"));
        assert!(
            !is_our_barrier(barrier, "some-other-nonce"),
            "a PREVIOUS cycle's barrier must not end THIS drain — every cycle shares \
             the prefix, so the nonce is the only thing separating them"
        );
        assert!(
            !is_our_barrier(
                &Message::Logical {
                    transactional: false,
                    prefix: "someone_else".into(),
                    content: b"fixture-barrier-nonce".to_vec(),
                },
                "fixture-barrier-nonce"
            ),
            "the slot decodes the WHOLE database; another tool's marker carrying the \
             same bytes must not end a rivet drain"
        );
        assert!(
            msgs.iter()
                .filter(|m| !matches!(m, Message::Logical { .. }))
                .all(|m| !is_our_barrier(m, "fixture-barrier-nonce")),
            "only a logical message can be a barrier"
        );
    }

    /// A tuple whose width disagrees with the relation is refused.
    #[test]
    fn a_tuple_wider_or_narrower_than_its_relation_is_refused() {
        let rel = || Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "v".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        };
        let cell = |n: i64| Cell::Binary(n.to_be_bytes().to_vec());

        for (label, cells) in [
            ("too narrow", vec![cell(1)]),
            ("too wide", vec![cell(1), cell(2), cell(3)]),
        ] {
            let mut a = Assembler::default();
            a.push(rel()).expect("relation");
            let err = a
                .push(Message::Insert {
                    relation_id: 1,
                    after: cells,
                })
                .expect_err(
                    "a tuple that disagrees with its relation must be REFUSED — \
                     mapping it by position puts values into the wrong columns, \
                     which is the class a name-addressed image exists to remove",
                );
            let text = format!("{err:#}");
            assert!(
                text.contains("public.t") && text.contains("column(s)"),
                "{label}: the refusal must name the relation and both widths: {text}"
            );
        }
    }

    /// A pre-image cell that is ITSELF a marker is not a recovery source.
    #[test]
    fn a_pre_image_cell_that_is_also_a_marker_cannot_recover_anything() {
        let mut a = Assembler::default();
        a.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "big".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        })
        .expect("relation");
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");

        let id = Cell::Binary(7i64.to_be_bytes().to_vec());
        let err = a
            .push(Message::Update {
                relation_id: 1,
                before: Some(vec![id.clone(), Cell::ToastUnchanged]),
                after: vec![id, Cell::ToastUnchanged],
            })
            .expect_err("a marker cannot be recovered from a marker");
        let text = format!("{err:#}");
        assert!(
            text.contains("big") && text.contains("no pre-image holds it"),
            "the refusal must name the column and say the pre-image is no help — \
             copying the marker across reports a clean recovery and writes the \
             placeholder where data belongs: {text}"
        );

        let mut b = Assembler::default();
        b.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "big".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        })
        .expect("relation");
        b.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        let id = Cell::Binary(7i64.to_be_bytes().to_vec());
        b.push(Message::Update {
            relation_id: 1,
            before: Some(vec![id.clone(), Cell::Binary(b"real".to_vec())]),
            after: vec![id, Cell::ToastUnchanged],
        })
        .expect("a real pre-image value IS a recovery source");
        let out = events(
            b.push(Message::Commit {
                commit_lsn: 16,
                end_lsn: 16,
            })
            .expect("commit"),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].after.as_ref().and_then(|v| v.get(1)),
            Some(&crate::source::cdc::value::RivetValue::Bytes(
                b"real".to_vec()
            )),
            "the unchanged column takes the pre-image's REAL value"
        );
    }

    /// A BEGIN while rows are still open means a transaction never committed.
    #[test]
    fn a_begin_over_an_uncommitted_transaction_is_refused() {
        let mut a = Assembler::default();
        a.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
                is_key: true,
            }],
        })
        .expect("relation");
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        a.push(Message::Insert {
            relation_id: 1,
            after: vec![Cell::Binary(42i64.to_be_bytes().to_vec())],
        })
        .expect("insert");
        let err = a
            .push(Message::Begin {
                final_lsn: 2,
                xid: 2,
            })
            .expect_err("a second BEGIN with rows open must fail");
        assert!(
            format!("{err:#}").contains("still open"),
            "dropping the open rows silently ships a partial transaction: {err:#}"
        );
    }

    /// A BEGIN over a spilled open transaction counts the rows on disk too, and a closed one prints its shape.
    #[test]
    fn a_begin_over_a_spilled_transaction_counts_every_open_row() {
        let d = tempfile::tempdir().expect("dir");
        let mut a = assembler_capped_at(Some(d.path().to_path_buf()), (2, usize::MAX));
        a.push(rel_msg(&["id", "name"])).expect("relation");
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        for i in 0..5 {
            a.push(row_msg(i, "x")).expect("row");
        }
        let err = a
            .push(Message::Begin {
                final_lsn: 2,
                xid: 2,
            })
            .expect_err("a second BEGIN with rows open must fail");
        assert!(
            format!("{err:#}").contains("BEGIN with 5 row(s) still open"),
            "{err:#}"
        );
        let mut b = assembler_capped_at(Some(d.path().to_path_buf()), (2, usize::MAX));
        b.push(rel_msg(&["id", "name"])).expect("relation");
        b.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        for i in 0..5 {
            b.push(row_msg(i, "x")).expect("row");
        }
        let closed = b
            .push(Message::Commit {
                commit_lsn: 0x1_0000_0001,
                end_lsn: 0x1_0000_0002,
            })
            .expect("commit")
            .expect("closed");
        assert_eq!(
            format!("{closed:?}"),
            format!(
                "Closed {{ head: {} event(s), spilled: true }}",
                closed.head.len()
            )
        );
        assert!(!closed.head.is_empty());
    }

    /// Truncated input FAILS rather than returning a half-read message.
    #[test]
    fn a_truncated_message_is_an_error_not_a_partial_decode() {
        let full = &fixture()[0];
        for cut in 1..full.len().min(12) {
            let err = decode(&full[..cut]);
            if let Ok(m) = err {
                assert!(
                    matches!(m, Message::Other(_)),
                    "a {cut}-byte prefix decoded as {m:?}; a partial read is how a \
                     protocol desync becomes silent corruption"
                );
            }
        }
        assert!(decode(&[]).is_err(), "an empty message must error");
        assert!(
            decode(&[b'I', 0, 0, 0, 1, b'N', 0, 1, b't', 0x7f, 0xff, 0xff, 0xff]).is_err(),
            "a length that exceeds the buffer must error, never read out of bounds"
        );
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    /// A truncated message and a row before its Relation are reader invariants (exit 6), and a poisoned transaction re-raises the ORIGINAL cause with its code.
    #[test]
    fn protocol_invariants_and_a_poisoned_transaction_carry_their_codes() {
        let err = decode(&[b'B', 0, 0]).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_INTERNAL_SOURCE_INVARIANT", 6);
        let mut a = Assembler::default();
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .unwrap();
        let err = a
            .push(Message::Insert {
                relation_id: 7,
                after: vec![Cell::Null],
            })
            .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_INTERNAL_SOURCE_INVARIANT", 6);
        let again = a
            .push(Message::Commit {
                commit_lsn: 2,
                end_lsn: 3,
            })
            .unwrap_err();
        crate::error::assert_coded(&again, "RIVET_INTERNAL_SOURCE_INVARIANT", 6);
        assert_eq!(again.to_string(), format!("{err:#}"));
    }
}
