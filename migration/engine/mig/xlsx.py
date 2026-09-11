"""Minimal xlsx reader/writer, stdlib only.

`mig` is deliberately dependency-free so it runs wherever the workflow files
live (SKILL.md: "stdlib-only Python 3.10+. No install, no network."). An .xlsx
is a zip of XML parts, so both directions are a few hundred lines rather than a
pip install -- which keeps `mig export` usable on a locked-down analyst box.

Writer: inline strings only (no shared-string table), one header style, frozen
header row and autofilter. Reader: enough to copy an existing workbook
sheet-for-sheet, preserving formulas as "=..." text so the cross-sheet
references in the Lakebridge report survive the merge.
"""
from __future__ import annotations

import re
import zipfile

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

CELL_LIMIT = 32767          # Excel's hard cap on characters in one cell
SHEET_NAME_LIMIT = 31       # and on sheet names
MAX_COL_WIDTH = 70

# Excel rejects most control characters even though XML 1.0 would allow a few.
_ILLEGAL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_BAD_SHEET_CHARS = re.compile(r"[\[\]:*?/\\]")


def col_letter(idx: int) -> str:
    """0-based column index -> 'A', 'B', ... 'AA'."""
    s = ""
    idx += 1
    while idx:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s


def _esc(text: str) -> str:
    text = _ILLEGAL.sub("", text)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def safe_sheet_name(name: str, taken: set) -> str:
    """Excel's sheet-name rules, plus de-duplication against names already used."""
    name = _BAD_SHEET_CHARS.sub("-", (name or "Sheet").strip()) or "Sheet"
    name = name[:SHEET_NAME_LIMIT]
    base, n = name, 2
    while name.lower() in taken:
        suffix = "~%d" % n
        name = base[:SHEET_NAME_LIMIT - len(suffix)] + suffix
        n += 1
    taken.add(name.lower())
    return name


class Sheet:
    """One worksheet: a name, an optional header row, and the data rows."""

    def __init__(self, name, rows, header=True, freeze=True, autofilter=True):
        self.name = name
        self.rows = rows
        self.header = header and bool(rows)
        self.freeze = freeze and self.header
        self.autofilter = autofilter and self.header


def _cell_xml(ref, value, style):
    """One <c>. Numbers stay numeric, a leading '=' makes a formula, rest is text."""
    s = ' s="%d"' % style if style else ""
    if isinstance(value, bool):
        return '<c r="%s"%s t="b"><v>%d</v></c>' % (ref, s, 1 if value else 0)
    if isinstance(value, (int, float)):
        return '<c r="%s"%s><v>%s</v></c>' % (ref, s, repr(value))
    text = str(value)
    if text.startswith("=") and len(text) > 1:
        return '<c r="%s"%s><f>%s</f></c>' % (ref, s, _esc(text[1:]))
    if len(text) > CELL_LIMIT:
        text = text[:CELL_LIMIT - 3] + "..."
    return ('<c r="%s"%s t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>'
            % (ref, s, _esc(text)))


def _widths(rows):
    """Column widths from the first 200 rows -- enough to look deliberate, cheap."""
    widths = {}
    for row in rows[:200]:
        for i, v in enumerate(row):
            if v is None:
                continue
            n = len(str(v))
            if n > widths.get(i, 0):
                widths[i] = n
    return {i: min(max(n + 2, 9), MAX_COL_WIDTH) for i, n in widths.items()}


def _sheet_xml(sheet):
    rows = sheet.rows
    ncols = max((len(r) for r in rows), default=1)
    nrows = max(len(rows), 1)
    out = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
           '<worksheet xmlns="%s" xmlns:r="%s">' % (MAIN_NS, REL_NS),
           '<dimension ref="A1:%s%d"/>' % (col_letter(max(ncols - 1, 0)), nrows)]
    if sheet.freeze:
        out.append('<sheetViews><sheetView workbookViewId="0">'
                   '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>'
                   '</sheetView></sheetViews>')
    else:
        out.append('<sheetViews><sheetView workbookViewId="0"/></sheetViews>')
    out.append('<sheetFormatPr defaultRowHeight="15"/>')

    widths = _widths(rows)
    if widths:
        out.append("<cols>")
        for i in sorted(widths):
            out.append('<col min="%d" max="%d" width="%d" customWidth="1"/>'
                       % (i + 1, i + 1, widths[i]))
        out.append("</cols>")

    out.append("<sheetData>")
    for r, row in enumerate(rows, start=1):
        style = 1 if (sheet.header and r == 1) else 0
        cells = [_cell_xml("%s%d" % (col_letter(c), r), v, style)
                 for c, v in enumerate(row) if v is not None and v != ""]
        out.append('<row r="%d">%s</row>' % (r, "".join(cells)) if cells
                   else '<row r="%d"/>' % r)
    out.append("</sheetData>")

    if sheet.autofilter and ncols:
        out.append('<autoFilter ref="A1:%s%d"/>' % (col_letter(ncols - 1), nrows))
    out.append("</worksheet>")
    return "".join(out)


_STYLES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="%s">
<fonts count="2">
<font><sz val="11"/><name val="Calibri"/></font>
<font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>
</fonts>
<fills count="3">
<fill><patternFill patternType="none"/></fill>
<fill><patternFill patternType="gray125"/></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FF1F3864"/><bgColor indexed="64"/></patternFill></fill>
</fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="2">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1"/>
</cellXfs>
</styleSheet>""" % MAIN_NS


def write(path, sheets):
    """Write `sheets` (a list of Sheet) to `path` as one workbook."""
    sheets = list(sheets) or [Sheet("Empty", [["(no data)"]], header=False)]
    types = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
             '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
             '<Default Extension="rels" ContentType="application/vnd.openxmlformats-'
             'package.relationships+xml"/>',
             '<Default Extension="xml" ContentType="application/xml"/>',
             '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-'
             'officedocument.spreadsheetml.sheet.main+xml"/>',
             '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-'
             'officedocument.spreadsheetml.styles+xml"/>']
    wb = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
          '<workbook xmlns="%s" xmlns:r="%s"><sheets>' % (MAIN_NS, REL_NS)]
    rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="%s">' % PKG_REL_NS]

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for i, sh in enumerate(sheets, start=1):
            part = "xl/worksheets/sheet%d.xml" % i
            types.append('<Override PartName="/%s" ContentType="application/vnd.openxmlformats-'
                         'officedocument.spreadsheetml.worksheet+xml"/>' % part)
            wb.append('<sheet name="%s" sheetId="%d" r:id="rId%d"/>' % (_esc(sh.name), i, i))
            rels.append('<Relationship Id="rId%d" Type="%s/worksheet" '
                        'Target="worksheets/sheet%d.xml"/>' % (i, REL_NS, i))
            z.writestr(part, _sheet_xml(sh))

        # Lakebridge sheets carry formulas; without fullCalcOnLoad Excel shows blanks.
        wb.append('</sheets><calcPr calcId="191029" fullCalcOnLoad="1"/></workbook>')
        rels.append('<Relationship Id="rId0" Type="%s/styles" Target="styles.xml"/>' % REL_NS)
        rels.append("</Relationships>")
        types.append("</Types>")

        z.writestr("[Content_Types].xml", "".join(types))
        z.writestr("_rels/.rels",
                   '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Relationships xmlns="%s"><Relationship Id="rId1" Type="%s/officeDocument" '
                   'Target="xl/workbook.xml"/></Relationships>' % (PKG_REL_NS, REL_NS))
        z.writestr("xl/workbook.xml", "".join(wb))
        z.writestr("xl/_rels/workbook.xml.rels", "".join(rels))
        z.writestr("xl/styles.xml", _STYLES)


# --------------------------------------------------------------------------- read


def _tag(el):
    return el.tag.rsplit("}", 1)[-1]


def _ref_col(ref):
    letters = "".join(ch for ch in ref if ch.isalpha())
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def read(path):
    """Read a workbook as [(sheet_name, [[cell, ...], ...]), ...].

    Formulas come back as "=<formula>" so they can be written straight back out;
    values are str/int/float/bool/None. Enough to copy a report, not a full parser.
    """
    import xml.etree.ElementTree as ET

    with zipfile.ZipFile(path) as z:
        names = set(z.namelist())
        shared = []
        if "xl/sharedStrings.xml" in names:
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            shared = ["".join(t.text or "" for t in si.iter("{%s}t" % MAIN_NS))
                      for si in root if _tag(si) == "si"]

        targets = {}
        if "xl/_rels/workbook.xml.rels" in names:
            for rel in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")):
                targets[rel.get("Id")] = rel.get("Target")

        out = []
        for sh in ET.fromstring(z.read("xl/workbook.xml")).iter("{%s}sheet" % MAIN_NS):
            target = targets.get(sh.get("{%s}id" % REL_NS)) or ""
            part = target.lstrip("/") if target.startswith("/") else "xl/" + target.lstrip("./")
            if part not in names:
                continue
            out.append((sh.get("name") or part, _read_sheet(ET, z.read(part), shared)))
        return out


def _read_sheet(ET, blob, shared):
    rows = []
    for row in ET.fromstring(blob).iter("{%s}row" % MAIN_NS):
        cells = []
        for c in row:
            if _tag(c) != "c":
                continue
            ref = c.get("r")
            if ref:                                   # honour gaps in sparse rows
                idx = _ref_col(ref)
                if idx > len(cells):
                    cells.extend([None] * (idx - len(cells)))
            f = c.find("{%s}f" % MAIN_NS)
            v = c.find("{%s}v" % MAIN_NS)
            ctype = c.get("t")
            if f is not None and (f.text or ""):
                val = "=" + f.text
            elif ctype == "s" and v is not None:
                try:
                    val = shared[int(v.text)]
                except (ValueError, IndexError, TypeError):
                    val = v.text
            elif ctype == "inlineStr":
                is_el = c.find("{%s}is" % MAIN_NS)
                val = "".join(is_el.itertext()) if is_el is not None else None
            elif v is None:
                val = None
            elif ctype in ("str", "e"):
                val = v.text
            elif ctype == "b":
                val = (v.text == "1")
            else:
                try:
                    val = float(v.text)
                    if val == int(val):
                        val = int(val)
                except (TypeError, ValueError):
                    val = v.text
            cells.append(val)
        while cells and cells[-1] is None:
            cells.pop()
        rows.append(cells)
    while rows and not rows[-1]:
        rows.pop()
    return rows
