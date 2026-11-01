- **New: manifest `value_fingerprint` per part (`xxh3v1:`) is the warehouse dedup key.**
  It digests a part's schema and its row values in row order, independent of Parquet encoding, so the same
  rows give the same value across encodings and releases; `_rivet_exported_at` is excluded. `content_fingerprint` is unchanged: it still hashes
  the file bytes. The field is optional: a manifest written by 0.32.2 or earlier has none, and absent means
  unknown, not a mismatch; `validate` and `repair` read manifests with or without it. CDC parts do not carry it
  yet. Cost on a 1M-row `content_items` full export: +1.9% wall, +4.4% CPU (min of 3), RSS unchanged.
  **What to do:** dedup on `value_fingerprint` where present; keep `content_fingerprint` for file identity.
