"""Seeds still in patch form: a frozen ledger that only shrinks; a new seed is a `seeded!` switch (docs/seeded-recall.md)."""

PATCH_FORM = "2026-11-14: patch form, one build each; closes when its seed is converted to a `seeded!` switch and this line is deleted"

PATCH_SEEDS: dict[str, str] = {  # ratchet-pin: seeded-patch-form-seeds (review 2027-02-01) strings
    "ca-file-second-trust-anchor/mssql.patch": PATCH_FORM,
    "ca-file-second-trust-anchor/mysql.patch": PATCH_FORM,
    "mysql-binlog-negative-time-fraction/shared.patch": PATCH_FORM,
    "remedy-does-not-recover/shared.patch": PATCH_FORM,
}  # ratchet-pin: end
