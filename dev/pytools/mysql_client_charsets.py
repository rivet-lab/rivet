"""Measure what each client collation does to a statement's lexing (which lead bytes swallow the ASCII byte after them, which bytes are blanks, which open a `--` comment, which two-byte characters the server reads as another character than the encoding standard does), and write or check src/source/mysql/client_charsets.rs; needs pymysql and cargo."""
import argparse
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "src/source/mysql/client_charsets.rs"
HIGH = range(0x80, 0x100)
PROBED = list(range(0x00, 0x20)) + list(range(0x7F, 0x100))
ASCII_BLANKS = [9, 10, 11, 12, 13]
CLIENT_UNUSABLE = 1231


def run(container: str, charset: str, sql: bytes) -> tuple[bytes, bool]:
    """One mysql client session fed raw statement bytes; returns stdout and whether the client started."""
    p = subprocess.run(
        ["docker", "exec", "-i", container, "mysql", "-uroot", "-privet", "-N", "-B", "--force", f"--default-character-set={charset}"],
        input=sql, capture_output=True,
    )
    return p.stdout, b"Unsupported" not in p.stderr and b"cannot be used as client" not in p.stderr


def sql_text(container: str, text: str) -> list[list[str]]:
    out = subprocess.run(["docker", "exec", container, "mysql", "-uroot", "-privet", "-N", "-B", "-e", text], capture_output=True, text=True).stdout
    return [line.split("\t") for line in out.splitlines() if line]


def leads(container: str, charset: str, probe) -> list[int]:
    """The lead bytes whose pair swallows the ASCII byte after them (probe returns a statement that succeeds only then)."""
    def one(b: int) -> int | None:
        out, ok = run(container, charset, probe(b))
        return b if ok and out.strip() else None
    with ThreadPoolExecutor(8) as pool:
        return sorted(b for b in pool.map(one, HIGH) if b is not None)


def connect(container: str):
    """A raw-bytes connection to the server in `container`; statements are sent as bytes, never through the mysql client, which lexes `--` and `/*` itself."""
    import pymysql
    port = int(subprocess.run(["docker", "port", container, "3306"], capture_output=True, text=True).stdout.split()[0].rsplit(":", 1)[1])
    return pymysql.connect(host="127.0.0.1", port=port, user="root", password="rivet", use_unicode=False, autocommit=True)


def lexical(container: str, ids: list[int]) -> dict[int, dict]:
    """Per collation id the bytes the server lexes as a blank and the bytes after which `--` opens a comment (SELECT<b>1000 and SELECT 2000 --<b> newline)."""
    local = threading.local()

    def one(cid: int) -> tuple[int, dict | None]:
        if not hasattr(local, "cur"):
            local.cur = connect(container).cursor()
        cur = local.cur

        def answers(statement: bytes, want: int) -> bool:
            try:
                cur.execute(statement)
                return cur.fetchall() == ((want,),)
            except Exception:
                return False

        try:
            cur.execute(f"SET character_set_client = {cid}")
        except Exception as e:
            if e.args and e.args[0] == CLIENT_UNUSABLE:
                return cid, None
            raise
        blank = [b for b in PROBED if answers(b"SELECT" + bytes([b]) + b"1000", 1000)]
        comment = [b for b in PROBED if answers(b"SELECT 2000 --" + bytes([b]) + b"\n", 2000)]
        return cid, {"blank": blank, "comment": comment}

    with ThreadPoolExecutor(8) as pool:
        return {cid: entry for cid, entry in pool.map(one, ids) if entry is not None}


def signature(entry: dict) -> tuple[list[int], list[int], bool]:
    """The high blank bytes, the high non-blank bytes that open a comment, and whether 0x7F opens one; refuses a server that breaks the rules the reader assumes of bytes below 0x80."""
    blank, comment = entry["blank"], entry["comment"]
    if [b for b in blank if b < 0x80] != ASCII_BLANKS:
        raise SystemExit(f"blanks below 0x80 are not {ASCII_BLANKS}: {blank}")
    if [b for b in comment if b < 0x20] != list(range(0x20)):
        raise SystemExit(f"a byte below 0x20 does not open a comment: {comment}")
    high_blank = [b for b in blank if b >= 0x80]
    high_opener = [b for b in comment if b >= 0x80 and b not in high_blank]
    if [b for b in high_blank if b not in comment]:
        raise SystemExit(f"a high blank does not open a comment: {high_blank}")
    return high_blank, high_opener, 0x7F in comment


def rust_pairs() -> dict[str, dict[str, str]]:
    """Per character set the two-byte characters the reader's layout holds and the character the encoding standard decodes each to (`-` when the reader cannot place it)."""
    out = subprocess.run(
        ["cargo", "test", "--lib", "dump_pair_decodes", "--", "--ignored", "--nocapture"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    pairs: dict[str, dict[str, str]] = {}
    for line in out.splitlines():
        if line.startswith("PAIR "):
            _, name, key, read = line.split()
            pairs.setdefault(name, {})[key] = read
    return pairs


def server_pairs(container: str, charset: str) -> dict[str, str]:
    """The character the server converts each two-byte value with a high lead to, as hex of its UTF-8 (empty when it is no character of the set)."""
    cur = connect(container).cursor()
    cur.execute("SET SESSION cte_max_recursion_depth = 200000")
    cur.execute(
        f"WITH RECURSIVE r(n) AS (SELECT 32768 UNION ALL SELECT n+1 FROM r WHERE n < 65535) "
        f"SELECT HEX(n), HEX(CONVERT(CAST(UNHEX(HEX(n)) AS CHAR CHARACTER SET {charset}) USING utf8mb4)) FROM r"
    )
    return {k.decode(): (v or b"").decode() for k, v in cur.fetchall()}


NOT_BYTE_CHARSETS = {"utf8mb3", "utf8mb4", "utf16", "utf16le", "utf32", "ucs2", "binary", "ascii"}


def high_bytes(container: str, charset: str) -> str:
    """The character each byte 0x80..0xFF is in the set, as a string of 128 characters, U+FFFD for a byte that is none."""
    cur = connect(container).cursor()
    cur.execute(
        f"WITH RECURSIVE r(n) AS (SELECT 128 UNION ALL SELECT n+1 FROM r WHERE n < 255) "
        f"SELECT n, HEX(CONVERT(CAST(UNHEX(HEX(n)) AS CHAR CHARACTER SET {charset}) USING utf8mb4)) FROM r ORDER BY n"
    )
    out = []
    for _, hexed in cur.fetchall():
        text = bytes.fromhex((hexed or b"").decode()).decode("utf-8")
        out.append(text if len(text) == 1 and not text.isascii() and not text.isspace() else "\ufffd")
    return "".join(out)


def misreads(container: str, ours: dict[str, dict[str, str]]) -> list[list]:
    """Runs [charset, lead, first trail, last trail, code point of the first] (0 when the server's character cannot be placed) of the pairs the server reads as another character than the encoding standard; refuses a layout that is not the server's."""
    runs: list[list] = []
    for name, held in sorted(ours.items()):
        server = server_pairs(container, name)
        lead_set = {int(k[:2], 16) for k in held}
        keys: list[tuple[int, int, int]] = []
        for key, theirs in server.items():
            lead, trail = int(key[:2], 16), int(key[2:], 16)
            text = bytes.fromhex(theirs).decode("utf-8") if theirs else ""
            if key in held:
                if len(text) != 1:
                    raise SystemExit(f"{name} {key}: the layout holds a pair the server does not ({text!r})")
            elif lead in lead_set and len(text) == 1:
                raise SystemExit(f"{name} {key}: the server holds a pair the layout does not ({text!r})")
            if key not in held:
                continue
            cp = 0 if text.isascii() or text.isspace() or text == "\ufffd" else ord(text)
            if (f"{cp:X}" if cp else "-") != held[key]:
                keys.append((lead, trail, cp))
        for lead, trail, cp in sorted(keys):
            last = runs[-1] if runs else None
            if last and last[0] == name and last[1] == lead and last[3] == trail - 1 and (cp == 0) == (last[4] == 0) and (cp == 0 or cp == last[4] + trail - last[2]):
                last[3] = trail
            else:
                runs.append([name, lead, trail, trail, cp])
    return runs


def measure(container: str) -> dict:
    version = sql_text(container, "select version()")[0][0]
    rows = sql_text(container, "select id, character_set_name, is_default from information_schema.collations order by id")
    sets: dict[str, dict] = {}
    for cid, name, default in rows:
        sets.setdefault(name, {"ids": []})["ids"].append(int(cid))
    for name, entry in sets.items():
        entry["pair_leads"] = leads(container, name, lambda b: b"SELECT '" + bytes([b, 0x5C]) + b"';\n")
        entry["pair_leads_backtick"] = leads(container, name, lambda b: b"SELECT 1 AS `" + bytes([b, 0x60]) + b"` ;\n")
        print(name, len(entry["pair_leads"]), file=sys.stderr)
    ids = sorted(i for e in sets.values() for i in e["ids"])
    lex = {cid: signature(entry) for cid, entry in lexical(container, ids).items()}
    return {"server": version, "charsets": sets, "lexical": lex, "misread": misreads(container, rust_pairs()),
            "high": {n: high_bytes(container, n) for n in sorted(sets) if n not in NOT_BYTE_CHARSETS}}


def render(data: dict) -> str:
    sets = data["charsets"]
    ids = sorted((i, n) for n, e in sets.items() for i in e["ids"])
    lines = [f"//! Generated by dev/pytools/mysql_client_charsets.py from MySQL {data['server']}; do not edit.", ""]
    lines.append("pub(super) const COLLATION_CHARSET: &[(u16, &str)] = &[")
    lines += [f'    ({i}, "{n}"),' for i, n in ids]
    lines += ["];", "", "#[cfg(test)]", "pub(super) const PAIR_LEADS: &[(&str, &[u8])] = &["]
    lines += [f'    ("{n}", &{e["pair_leads"]}),' for n, e in sorted(sets.items()) if e["pair_leads"]]
    lines += ["];", ""]
    lines += ["/// (collation id, high bytes read as blanks, high non-blank bytes after which `--` opens a comment, whether 0x7F opens one); an id that is absent has none of the first two and opens on 0x7F.", "pub(super) const LEXICAL: &[(u16, &[u8], &[u8], bool)] = &["]
    for cid, (blank, opener, del_opens) in sorted(data["lexical"].items()):
        if blank or opener or not del_opens:
            lines.append(f"    ({cid}, &{blank}, &{opener}, {str(del_opens).lower()}),")
    lines += ["];", ""]
    lines += ["/// (character set, lead, first trail, last trail, code point of the character the first pair is, 0 when it is none the reader can place) of the two-byte characters the server reads as another character than the encoding standard does; the code point grows with the trail.", "pub(super) const PAIR_FIXES: &[(&str, u8, u8, u8, u32)] = &["]
    lines += [f'    ("{n}", 0x{lead:02X}, 0x{lo:02X}, 0x{hi:02X}, 0x{cp:X}),' for n, lead, lo, hi, cp in data["misread"]]
    lines += ["];", ""]
    lines += ["/// (character set, the character each byte 0x80..0xFF is in it, U+FFFD for a byte that is none).", "pub(super) const HIGH_BYTES: &[(&str, &str)] = &["]
    for n, chars in sorted(data["high"].items()):
        escaped = "".join(c if c.isascii() else "\\u{%X}" % ord(c) for c in chars)
        lines.append(f'    ("{n}", "{escaped}"),')
    lines += ["];", ""]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("mode", choices=["write", "check", "json"])
    ap.add_argument("container", nargs="+", help="MySQL containers; the lexical tables of all of them must agree")
    args = ap.parse_args()
    measured = [measure(c) for c in args.container]
    data = measured[0]
    for other, name in zip(measured[1:], args.container[1:]):
        if other["lexical"] != data["lexical"] or other["misread"] != data["misread"] or other["high"] != data["high"]:
            print(f"client charset table of {name} ({other['server']}) differs from {args.container[0]} ({data['server']})", file=sys.stderr)
            return 1
    if len(measured) > 1:
        data["server"] = " and ".join(m["server"] for m in measured)
    bad = [n for n, e in data["charsets"].items() if e["pair_leads"] != e["pair_leads_backtick"]]
    if bad:
        print("backslash and backtick pairs differ for", bad, file=sys.stderr)
        return 1
    if args.mode == "json":
        print(json.dumps(data))
        return 0
    text = render(data)
    if args.mode == "write":
        TARGET.write_text(text)
        return 0
    same = {l for l in TARGET.read_text().splitlines()[1:]} == set(text.splitlines()[1:])
    print("client charset table matches the server" if same else "client charset table differs from the server")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
