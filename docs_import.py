#!/opt/homebrew/bin/python3
"""Rescue episode scripts that were saved as Google Docs.

The script-writing skill is supposed to deliver a plain .md file into
`GK Daily/scripts/`. On 2026-09-13 it instead created a Google Doc named
`2026-09-13_pacing-the-frontier-stepping-stone.md` in My Drive root. The
filename matched the pipeline's contract perfectly, which is exactly why the
failure was invisible: the producer scans `scripts/*.md` for real markdown, a
Doc appears on disk as a .gdoc pointer with no readable body, and My Drive
root is not watched at all. The episode simply never existed as far as the
pipeline was concerned, with nothing in any log to say so.

This sweeps Drive for Docs whose names match the pipeline's
`YYYY-MM-DD_slug.md` contract, exports them to real markdown, and drops them
where the producer will find them. The skill should still be fixed — this is
the net, not the trapeze.

Docs' markdown export backslash-escapes punctuation, so `[pause]` comes back
as `\\[pause\\]` and the sources header as `\\--- SOURCES \\---`. Left alone the
episode renders as one unbroken block with no transition tones and reads its
own source list aloud, so unescaping is not cosmetic.

    docs_import.py            # sweep and import
    docs_import.py --dry-run  # report what it would do
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tower  # noqa: E402

log = logging.getLogger("docs.import")

DOC_MIME = "application/vnd.google-apps.document"
NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_[a-z0-9][a-z0-9-]*\.md$", re.I)


def gws(args: list, out: Path | None = None, timeout: int = 180):
    cmd = ["gws", *args] + (["-o", str(out)] if out else [])
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def find_stray_docs() -> list[dict]:
    """Google Docs named like pipeline scripts, anywhere in the user's Drive."""
    params = json.dumps({
        "q": f"mimeType = '{DOC_MIME}' and trashed = false and name contains '.md'",
        "fields": "files(id,name,modifiedTime,parents)",
        "orderBy": "modifiedTime desc",
        "pageSize": 50,
    })
    proc = gws(["drive", "files", "list", "--params", params])
    try:
        files = json.loads(proc.stdout or "{}").get("files") or []
    except json.JSONDecodeError:
        log.error("could not parse Drive listing: %s", (proc.stdout or "")[:200])
        return []
    return [f for f in files if NAME_RE.match(f["name"])]


def unescape(text: str) -> str:
    """Undo the Docs export's backslash escaping of pipeline punctuation."""
    text = re.sub(r"\\([\[\]\-\.\*_#])", r"\1", text)
    text = re.sub(r"^-{3,}\s*SOURCES\s*-{3,}\s*$", "--- SOURCES ---",
                  text, flags=re.M)
    return text if text.endswith("\n") else text + "\n"


def export_markdown(file_id: str) -> str | None:
    with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as tmp:
        out = Path(tmp.name)
    try:
        gws(["drive", "files", "export", "--params",
             json.dumps({"fileId": file_id, "mimeType": "text/markdown"})], out=out)
        text = out.read_text() if out.exists() else ""
    finally:
        out.unlink(missing_ok=True)
    return unescape(text) if text.strip() else None


def usable(text: str, name: str) -> str | None:
    """Reject anything the producer would choke on, with the reason."""
    if not re.match(r"^#\s+\S", text.lstrip()):
        return "no '# Title' first line"
    body = re.split(r"^---\s*SOURCES\s*---", text, flags=re.M)[0]
    if len(body.split()) < 500:
        return f"body too short ({len(body.split())} words)"
    if "[pause]" not in text:
        return "no [pause] markers survived the export"
    return None


def already_handled(cfg: dict, name: str) -> bool:
    scripts = cfg["drive_gk_daily"] / "scripts"
    if (scripts / name).exists() or (scripts / "processed" / name).exists():
        return True
    slug = name[11:-3].lower()
    date = name[:10]
    key = f"special-edition-{slug}-{date}.mp3"
    for path in (cfg["podcasts_root"] / "public/episodes/special_editions.json",
                 cfg["podcasts_root"] / "config/spotify_uploaded.json"):
        try:
            if key in json.loads(path.read_text()):
                return True
        except Exception:
            continue
    return False


def sweep(cfg: dict, dry_run: bool = False) -> list[str]:
    imported = []
    for doc in find_stray_docs():
        name = doc["name"]
        if already_handled(cfg, name):
            continue
        text = export_markdown(doc["id"])
        if text is None:
            log.warning("%s: export produced nothing", name)
            continue
        why = usable(text, name)
        if why:
            log.warning("%s: not importing — %s", name, why)
            continue
        dest = cfg["drive_gk_daily"] / "scripts" / name
        words = len(text.split())
        if dry_run:
            log.info("would import %s (%d words) -> %s", name, words, dest)
        else:
            dest.write_text(text)
            log.info("imported %s (%d words) into scripts/", name, words)
            tower.telegram(cfg,
                           f"📄 GK Daily: {name} was saved as a Google Doc "
                           "instead of a markdown file in GK Daily/scripts/. "
                           "Imported it for you — the episode will produce on "
                           "the next trigger. Worth fixing in the skill.")
        imported.append(name)
    return imported


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    found = sweep(tower.load_config(), dry_run=args.dry_run)
    print(f"{len(found)} stray Doc(s): {', '.join(found) or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
