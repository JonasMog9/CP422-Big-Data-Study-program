#!/usr/bin/env python3
"""Build the CP422 Midterm Lab.

Inlines the scripts from src/ into one page and writes:
  index.html          a complete standalone page: double-click to open in any browser
  dist/artifact.html  the same page without the <html>/<head>/<body> wrapper, for
                      publishing as a claude.ai artifact (the publisher adds the wrapper)
"""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent
SRC = ROOT / "src"
SCRIPTS = ["content-theory.js", "content-code.js", "app.js"]

page = (SRC / "page.html").read_text(encoding="utf-8")
for name in SCRIPTS:
    js = (SRC / name).read_text(encoding="utf-8")
    assert "</script" not in js.lower(), f"{name} contains '</script', which would end the inline script early"
    marker = f"<!-- inline:{name} -->"
    assert marker in page, f"page.html is missing {marker}"
    page = page.replace(marker, f"<script>\n{js}\n</script>")

head, body = page.split("<!-- body -->", 1)

(ROOT / "dist").mkdir(exist_ok=True)
(ROOT / "dist" / "artifact.html").write_text(head + body, encoding="utf-8")

standalone = (
    "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
    "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,viewport-fit=cover\">\n"
    "<style>:root{padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}img{max-width:100%}</style>\n"
    + head + "</head>\n<body>\n" + body + "</body>\n</html>\n"
)
(ROOT / "index.html").write_text(standalone, encoding="utf-8")
print(f"wrote index.html ({len(standalone) // 1024} KB) and dist/artifact.html")
