from __future__ import annotations


def slugify(s: str) -> str:
    out: list[str] = []
    last_dash = False
    for ch in s.strip().lower():
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        else:
            if not last_dash and out:
                out.append("-")
                last_dash = True
    return "".join(out).strip("-") or "location"
