# -*- coding: utf-8 -*-
"""
dialog.py  —  CSV to GeoPackage converter
"""

import os

from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox,
    QFileDialog, QTableWidget, QTableWidgetItem,
    QProgressBar, QGroupBox, QSizePolicy, QHeaderView,
    QFrame, QCheckBox, QSpacerItem, QMessageBox,
    QAbstractItemView, QRadioButton, QButtonGroup, QWidget,
)
from qgis.PyQt.QtCore import Qt, QThread, pyqtSignal, QUrl
from qgis.gui import QgsProjectionSelectionWidget
from qgis.core import (
    QgsVectorLayer,
    QgsVectorFileWriter,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransformContext,
    QgsProject,
    QgsWkbTypes,
    QgsField,
    QgsFields,
)
from qgis.PyQt.QtCore import QVariant

import csv


# ======================================================================
#  Field type definitions
# ======================================================================

FIELD_TYPES = [
    "String",
    "Int32",        # 32-bit signed integer  (≈ ±2.1 billion)
    "Int64",        # 64-bit signed integer  (≈ ±9.2 × 10¹⁸)
    "Double",       # 64-bit float (~15 significant digits)
    "Boolean",
    "Date",
    "DateTime",
]

_TYPE_TO_QVARIANT = {
    "String":   QVariant.String,
    "Int32":    QVariant.Int,
    "Int64":    QVariant.LongLong,
    "Double":   QVariant.Double,
    "Boolean":  QVariant.Bool,
    "Date":     QVariant.Date,
    "DateTime": QVariant.DateTime,
}

# (background, foreground) colours per type
_TYPE_COLORS = {
    "String":   ("#dbeafe", "#1e40af"),
    "Int32":    ("#fef3c7", "#92400e"),
    "Int64":    ("#ffedd5", "#7c2d12"),
    "Double":   ("#fce7f3", "#9d174d"),
    "Boolean":  ("#d1fae5", "#065f46"),
    "Date":     ("#ede9fe", "#4c1d95"),
    "DateTime": ("#fff1f2", "#881337"),
}


# ======================================================================
#  Helpers — CSV introspection
# ======================================================================

_FALLBACK_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
FONT_TEXT_COLOR = "color:#2c4a35;"


def _detect_encoding(path: str) -> str:
    try:
        import chardet
        with open(path, "rb") as fh:
            result = chardet.detect(fh.read(65536))
        conf = result.get("confidence") or 0
        enc  = result.get("encoding") or ""
        if conf >= 0.80 and enc and enc.lower() != "ascii":
            return enc
    except ImportError:
        pass
    with open(path, "rb") as fh:
        raw = fh.read()
    for enc in _FALLBACK_ENCODINGS:
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin-1"


def _sniff_csv(path: str, delimiter: str = ",",
               skip_lines: int = 0, has_header: bool = True) -> dict:
    import re
    _WKT_RE = re.compile(
        r"^\s*(point|multipoint|linestring|multilinestring|"
        r"polygon|multipolygon|geometrycollection)",
        re.IGNORECASE,
    )
    encoding = _detect_encoding(path)
    fields, preview, row_count = [], [], 0

    with open(path, newline="", encoding=encoding, errors="replace") as fh:
        for _ in range(skip_lines):
            fh.readline()

        if has_header:
            reader = csv.DictReader(fh, delimiter=delimiter)
            fields = list(reader.fieldnames or [])
            for i, row in enumerate(reader):
                row_count += 1
                if i < 20:
                    preview.append(dict(row))
        else:
            reader = csv.reader(fh, delimiter=delimiter)
            first_row = next(reader, None)
            if first_row is not None:
                n_cols = len(first_row)
                fields = [f"field_{i + 1}" for i in range(n_cols)]
                row_count += 1
                preview.append(dict(zip(fields, first_row)))
                for i, row in enumerate(reader):
                    row_count += 1
                    if i < 19:
                        preview.append(dict(zip(fields, row)))

    wkt_cols = []
    if preview:
        for f in fields:
            val = preview[0].get(f, "")
            if val and _WKT_RE.match(str(val)):
                wkt_cols.append(f)
    return {
        "fields":    fields,
        "preview":   preview,
        "row_count": row_count,
        "encoding":  encoding,
        "wkt_cols":  wkt_cols,
    }


def _infer_field_type(values: list, field_name: str = "") -> str:
    """Auto-detect the best FIELD_TYPES label from sample values."""
    import re
    non_empty = [str(v).strip() for v in values if str(v).strip()]
    if not non_empty:
        return "String"
    sample = non_empty[:10]

    def is_int(s):
        try: int(s); return True
        except ValueError: return False

    if all(is_int(s) for s in sample):
        vals = [int(s) for s in sample]
        mx = max(abs(v) for v in vals)
        return "Int32" if mx <= 2147483647 else "Int64"

    bool_vals = {"true", "false", "yes", "no"}
    if all(s.lower() in bool_vals for s in sample):
        return "Boolean"

    def is_float(s):
        try: float(s); return True
        except ValueError: return False

    if all(is_float(s) for s in sample):
        return "Double"

    if all(re.match(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", s) for s in sample):
        return "DateTime"
    if all(re.match(r"^\d{4}-\d{2}-\d{2}$", s) for s in sample):
        return "Date"

    return "String"


# ======================================================================
#  Worker thread
# ======================================================================

class _Worker(QThread):
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(dict)

    def __init__(self, params: dict):
        super().__init__()
        self.p = params

    def run(self):
        try:
            result = self._convert()
        except Exception as exc:
            result = {"ok": False, "error": str(exc), "written": 0}
        self.finished.emit(result)

    def _build_delimited_text_uri(self) -> str:
        from qgis.PyQt.QtCore import QUrl
        p = self.p
        file_url = QUrl.fromLocalFile(p["csv_path"]).toString()

        def encode_col(name):
            return str(QUrl.toPercentEncoding(name), "utf-8")

        delim = p["delimiter"]
        delim_param = "delimiter=tab" if delim == "\t" else f"delimiter={delim}"

        params = [
            "useHeader=yes",
            f"encoding={p['encoding']}",
            delim_param,
            f"crs={p['crs_authid']}",
        ]
        if p["mode"] == "wkt":
            params += ["geomType=wkt", f"wktField={encode_col(p['wkt_field'])}"]
        else:
            params += [
                "geomType=point",
                f"xField={encode_col(p['x_field'])}",
                f"yField={encode_col(p['y_field'])}",
            ]
        return f"{file_url}?{'&'.join(params)}"

    def _convert(self) -> dict:
        p = self.p
        self.progress.emit(10, "Opening CSV with QGIS delimited text provider…")

        uri = self._build_delimited_text_uri()
        csv_layer = QgsVectorLayer(uri, "csv_tmp", "delimitedtext")

        if not csv_layer.isValid():
            return {
                "ok": False,
                "error": (
                    "QGIS could not open the CSV as a spatial layer.\n"
                    "Check that the geometry column / CRS are correct.\n\n"
                    f"URI used:\n{uri}"
                ),
                "written": 0,
            }

        feat_count = csv_layer.featureCount()
        self.progress.emit(30, f"Layer opened — {feat_count} features detected…")

        field_types = p.get("field_types", {})
        if field_types:
            self.progress.emit(40, "Applying field type overrides…")
            csv_layer = self._apply_field_types(csv_layer, field_types)

        # Apply QML style if provided
        qml_path = p.get("qml_path", "").strip()
        if qml_path and os.path.isfile(qml_path):
            self.progress.emit(45, "Applying QML style…")
            msg, success = csv_layer.loadNamedStyle(qml_path)
            if not success:
                # Non-fatal: warn but continue
                self.progress.emit(45, f"QML style warning: {msg}")

        self.progress.emit(50, "Writing GeoPackage…")

        output_path = p["output_path"]
        layer_name  = p["layer_name"] or os.path.splitext(
            os.path.basename(p["csv_path"])
        )[0]

        save_opts                      = QgsVectorFileWriter.SaveVectorOptions()
        save_opts.driverName           = "GPKG"
        save_opts.fileEncoding         = "UTF-8"
        save_opts.layerName            = layer_name
        save_opts.actionOnExistingFile = (
            QgsVectorFileWriter.CreateOrOverwriteLayer
            if os.path.exists(output_path)
            else QgsVectorFileWriter.CreateOrOverwriteFile
        )

        # Exclude any auto-generated fid column added by the delimited text provider
        fields = csv_layer.fields()
        save_opts.attributes = [
            i for i in range(fields.count())
            if fields.at(i).name().lower() != "fid"
        ]

        error_code, error_msg, _, _ = QgsVectorFileWriter.writeAsVectorFormatV3(
            csv_layer,
            output_path,
            QgsCoordinateTransformContext(),
            save_opts,
        )

        if error_code != QgsVectorFileWriter.NoError:
            return {"ok": False, "error": f"GeoPackage write failed: {error_msg}", "written": 0}

        # Save QML style into the GeoPackage itself if requested
        if qml_path and os.path.isfile(qml_path) and p.get("embed_style", False):
            self.progress.emit(90, "Embedding style into GeoPackage…")
            gpkg_layer = QgsVectorLayer(f"{output_path}|layername={layer_name}", layer_name, "ogr")
            if gpkg_layer.isValid():
                gpkg_layer.loadNamedStyle(qml_path)
                gpkg_layer.saveStyleToDatabase(layer_name, "QML style embedded on export", True, "")

        self.progress.emit(100, "Done!")
        return {
            "ok": True,
            "written": feat_count,
            "output": output_path,
            "layer_name": layer_name,
            "qml_applied": bool(qml_path and os.path.isfile(qml_path)),
        }

    def _apply_field_types(self, layer: QgsVectorLayer, field_types: dict) -> QgsVectorLayer:
        from qgis.core import QgsMemoryProviderUtils, QgsFeature
        from qgis.PyQt.QtCore import QDate, QDateTime

        src_fields = layer.fields()
        new_fields = QgsFields()
        for f in src_fields:
            name = f.name()
            if name in field_types:
                ttype = field_types[name]
                qv    = _TYPE_TO_QVARIANT.get(ttype, QVariant.String)
                fld   = QgsField(name, qv)
                new_fields.append(fld)
            else:
                new_fields.append(QgsField(f))

        mem_layer = QgsMemoryProviderUtils.createMemoryLayer(
            "typed_tmp", new_fields, layer.wkbType(), layer.crs()
        )
        mem_layer.startEditing()

        _INT32_MIN, _INT32_MAX = -2147483648, 2147483647

        for feat in layer.getFeatures():
            new_feat = QgsFeature(new_fields)
            new_feat.setGeometry(feat.geometry())
            for i, f in enumerate(src_fields):
                name  = f.name()
                raw   = feat[name]
                ttype = field_types.get(name)
                try:
                    if raw in (None, ""):
                        val = None
                    elif ttype in ("Int32", "Int64"):
                        # raw may arrive as bool (True/False), int, float, or
                        # string — normalise before converting
                        if isinstance(raw, bool):
                            v = int(raw)          # True→1, False→0
                        elif isinstance(raw, (int, float)):
                            v = int(raw)
                        else:
                            s = str(raw).strip().lower()
                            if s in ("true", "yes"):
                                v = 1
                            elif s in ("false", "no"):
                                v = 0
                            else:
                                v = int(float(s))
                        if ttype == "Int32":
                            val = max(_INT32_MIN, min(_INT32_MAX, v))
                        else:
                            val = v
                    elif ttype == "Double":
                        if isinstance(raw, bool):
                            val = float(int(raw))
                        else:
                            val = float(str(raw).strip())
                    elif ttype == "Boolean":
                        val = str(raw).lower() in ("1", "true", "yes")
                    elif ttype == "Date":
                        val = QDate.fromString(str(raw), Qt.ISODate)
                    elif ttype == "DateTime":
                        val = QDateTime.fromString(str(raw), Qt.ISODate)
                    else:
                        val = raw
                except Exception:
                    val = None
                new_feat.setAttribute(i, val)
            mem_layer.addFeature(new_feat)
        mem_layer.commitChanges()
        return mem_layer


# ======================================================================
#  Qt UI helpers
# ======================================================================

def _section(text: str, step: int) -> QLabel:
    lbl = QLabel(f"  STEP {step}    {text}")
    lbl.setFixedHeight(30)
    lbl.setStyleSheet(
        "QLabel{"
        "  background:#2c6e49; color:#fff;"
        "  font-weight:bold; font-size:11px; letter-spacing:1px;"
        "  border-radius:4px; padding-left:4px;"
        "}"
    )
    return lbl


def _hint(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setWordWrap(True)
    lbl.setStyleSheet("color:#666; font-size:11px; padding:2px 0 0 2px;")
    return lbl


def _badge(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setStyleSheet(
        "color:#1b4332; background:#d8f3dc;"
        "border:1px solid #95d5b2; border-radius:3px;"
        "padding:2px 7px; font-size:10px; font-weight:bold;"
    )
    return lbl


# ======================================================================
#  Field-type selector — custom QHeaderView with embedded QComboBoxes
# ======================================================================

class _TypeHeaderView(QHeaderView):
    """
    Custom horizontal header with two rows:
      • Top row  : column name  (painted by Qt as normal)
      • Bottom row: QComboBox   (parented to viewport so it scrolls correctly)

    The key insight: combos live on self.viewport(), so sectionViewportPosition()
    gives the correct x directly — no offset arithmetic needed.
    """

    typeChanged = pyqtSignal(int, str)   # (logical_col, type_label)

    NAME_H  = 24   # px reserved for the column-name text row
    COMBO_H = 26   # px reserved for the type combo row
    COL_W   = 150  # fixed default column width for readability

    def __init__(self, parent=None):
        super().__init__(Qt.Horizontal, parent)
        self._combos: list = []
        self.setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.setSectionsClickable(False)   # sorting not needed
        # Reposition on any geometry change (resize OR scroll)
        self.sectionResized.connect(self._reposition_combos)
        self.geometriesChanged.connect(self._reposition_combos)

    # ------------------------------------------------------------------
    def sizeHint(self):
        s = super().sizeHint()
        return s.__class__(s.width(), self.NAME_H + self.COMBO_H)

    # Override so Qt always allocates the exact height we want
    def minimumSectionSize(self):
        return self.COL_W

    # ------------------------------------------------------------------
    def populate(self, fields: list, inferred_types: list):
        """Destroy old combos and build fresh ones for the given fields."""
        for cb in self._combos:
            cb.hide(); cb.setParent(None); cb.deleteLater()
        self._combos.clear()

        for col, (name, dtype) in enumerate(zip(fields, inferred_types)):
            combo = QComboBox(self.viewport())   # viewport = scrolling surface
            combo.addItems(FIELD_TYPES)
            combo.setCurrentText(dtype)
            self._style_combo(combo, dtype)
            combo.currentTextChanged.connect(
                lambda t, c=col, cb=combo: self._on_changed(c, t, cb)
            )
            combo.show()
            self._combos.append(combo)

        self._reposition_combos()

    # ------------------------------------------------------------------
    def _reposition_combos(self, *_):
        """Place each combo in the bottom row of its section.
        sectionViewportPosition() already accounts for scroll offset because
        the combos are children of the viewport."""
        y = self.NAME_H
        h = self.COMBO_H - 2
        for i, combo in enumerate(self._combos):
            x = self.sectionViewportPosition(i)
            w = self.sectionSize(i)
            combo.setGeometry(x + 1, y, w - 2, h)

    # ------------------------------------------------------------------
    def _on_changed(self, col: int, type_label: str, combo: QComboBox):
        self._style_combo(combo, type_label)
        self.typeChanged.emit(col, type_label)

    # ------------------------------------------------------------------
    @staticmethod
    def _style_combo(combo: QComboBox, type_label: str):
        bg, fg = _TYPE_COLORS.get(type_label, ("#a13636", "#D8D8D8"))
        combo.setStyleSheet(
            f"QComboBox {{"
            f"  background:{bg}; color:{fg};"
            f"  border:1px solid {fg}55; border-radius:3px;"
            f"  font-size:10px; font-weight:bold; padding:1px 4px;"
            f"}}"
            f"QComboBox::drop-down {{ border:none; width:16px; }}"
            f"QComboBox QAbstractItemView {{ font-size:10px; }}"
        )

    # ------------------------------------------------------------------
    def get_type_map(self, fields: list) -> dict:
        return {
            fields[i]: self._combos[i].currentText()
            for i in range(min(len(fields), len(self._combos)))
        }

    # ------------------------------------------------------------------
    def paintSection(self, painter, rect, logical_index):
        """Paint the column name in the top NAME_H pixels only."""
        from qgis.PyQt.QtCore import QRect
        top_rect = QRect(rect.x(), rect.y(), rect.width(), self.NAME_H)
        super().paintSection(painter, top_rect, logical_index)


# ======================================================================
#  Main dialog
# ======================================================================

DELIMITERS = {
    "Comma  ( , )":     ",",
    "Semicolon  ( ; )": ";",
    "Tab  ( \\t )":     "\t",
    "Pipe  ( | )":      "|",
    "Space":            " ",
}


class CsvToGpkgDialog(QDialog):

    def __init__(self, iface, parent=None):
        super().__init__(parent or iface.mainWindow())
        self.iface               = iface
        self._worker             = None
        self._fields             = []
        self._encoding           = "utf-8"
        self._preview_rows       = []

        self.setWindowTitle("Monthly GeoFASU")
        self.setMinimumWidth(1600)
        self.setMinimumHeight(900)
        self.setWindowFlags(self.windowFlags() | Qt.WindowMaximizeButtonHint)

        self._style()
        self._build()

        from qgis.PyQt.QtWidgets import QApplication
        sg = QApplication.primaryScreen().availableGeometry()
        self.move(
            (sg.width()  - self.width())  // 2 + sg.x(),
            (sg.height() - self.height()) // 2 + sg.y(),
        )

    # ------------------------------------------------------------------
    def _style(self):
        self.setStyleSheet("""
            QDialog { background:#f4f6f2; font-family:'Segoe UI',Arial,sans-serif; font-size:12px; }
            QGroupBox {
                border:1px solid #cdd8c8; border-radius:6px; background:#fff;
                margin-top:6px; padding:10px 8px 8px 8px;
            }
            QLineEdit {
                border:1px solid #bdc8b8; border-radius:4px; padding:5px 8px; background:#fff;
                color:#1a2e1a;
            }
            QLineEdit:focus { border-color:#2c6e49; }
            QLineEdit::placeholder { color:#6b7c69; }
            QComboBox {
                border:1px solid #bdc8b8; border-radius:4px; padding:4px 8px; background:#ffffff;
            }
            QComboBox:focus { border-color:#2c6e49; color:#1a2e1a; }
            QRadioButton { color:#2c4a35; spacing:6px; }
            QRadioButton::indicator {
                width:13px; height:13px; border-radius:7px;
                border:2px solid #88a888; background:#fff;
            }
            QRadioButton::indicator:checked { background:#2c6e49; border-color:#1b4332; }
            QPushButton#browse {
                background:#e6efe2; border:1px solid #adc0a8;
                color:#2c4a35; border-radius:4px; padding:5px 12px;
            }
            QPushButton#browse:hover { background:#d4e6cc; }
            QPushButton#convert {
                background:#2c6e49; color:#fff; border:none;
                border-radius:4px; font-weight:bold; font-size:13px; padding:8px 26px;
            }
            QPushButton#convert:hover    { background:#245c3d; }
            QPushButton#convert:disabled { background:#a8c4b4; }
            QPushButton#close {
                background:#f0e8e8; border:1px solid #d0b0b0;
                color:#6c3030; border-radius:4px; padding:6px 14px;
            }
            QPushButton#close:hover { background:#e8d4d4; }
            QPushButton#clear_qml {
                background:#fef3c7; border:1px solid #d9a400;
                color:#7c5a00; border-radius:4px; padding:5px 10px; font-size:11px;
            }
            QPushButton#clear_qml:hover { background:#fde68a; }
            QPushButton#tracklog_browse {
                background:#e8eaf6; border:1px solid #9fa8da;
                color:#1a237e; border-radius:4px; padding:5px 12px;
            }
            QPushButton#tracklog_browse:hover { background:#d3d8f0; }
            QTableWidget {
                border:1px solid #cdd8c8; border-radius:4px;
                gridline-color:#e8ede4; background:#fafcf8; font-size:11px;
            }
            QHeaderView::section {
                background:#2c6e49; color:#fff;
                padding:4px 7px; border:none; font-weight:bold; font-size:11px;
            }
            QProgressBar {
                border:1px solid #bdc8b8; border-radius:4px;
                text-align:center; background:#e8ede4;
                height:22px; font-weight:bold; color:#2c4a35;
            }
            QProgressBar::chunk {
                background:qlineargradient(x1:0,y1:0,x2:1,y2:0,
                    stop:0 #2c6e49,stop:1 #52b788);
                border-radius:3px;
            }
            QComboBox QAbstractItemView {
                background: #f2f3f1;
                selection-background-color: #d8f3dc;
                selection-color: #1b4332;
                border: 1px solid #bdc8b8;
                outline: none;
                color:#1a2e1a;
            }
        """)

    # ------------------------------------------------------------------
    def _build(self):
        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(14, 14, 14, 14)

        root.addWidget(self._banner())

        # Steps 1 & 2
        row12 = QHBoxLayout(); row12.setSpacing(10)

        col1 = QVBoxLayout(); col1.setSpacing(4)
        col1.addWidget(_section("Choose CSV File", 1))
        grp1 = QGroupBox(); v1 = QVBoxLayout(grp1); v1.setSpacing(6)
        h_csv = QHBoxLayout()
        self.csv_edit = QLineEdit()
        self.csv_edit.setPlaceholderText("Path to CSV file…")
        self.csv_edit.setReadOnly(True)
        btn_csv = QPushButton("Browse…"); btn_csv.setObjectName("browse")
        btn_csv.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_csv.clicked.connect(self._browse_csv)
        h_csv.addWidget(self.csv_edit); h_csv.addWidget(btn_csv)
        v1.addLayout(h_csv)
        h_delim = QHBoxLayout()
        delim_lbl = QLabel("Delimiter:")
        delim_lbl.setStyleSheet(FONT_TEXT_COLOR)
        h_delim.addWidget(delim_lbl)
        self.delim_combo = QComboBox()
        [self.delim_combo.addItem(k) for k in DELIMITERS]
        self.delim_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.delim_combo.currentIndexChanged.connect(self._on_delim_changed)
        h_delim.addWidget(self.delim_combo)
        v1.addLayout(h_delim)

        v1.addWidget(_hint("ℹ  Browse your CSV and choose the correct delimiter."))
        v1.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))
        col1.addWidget(grp1); row12.addLayout(col1, stretch=1)

        col2 = QVBoxLayout(); col2.setSpacing(4)
        col2.addWidget(_section("Geometry Source & CRS", 2))
        grp2 = QGroupBox(); v2 = QVBoxLayout(grp2); v2.setSpacing(8)
        self._mode_grp = QButtonGroup(self)
        self.radio_wkt = QRadioButton("WKT column  — geometry stored as a WKT string in one column")
        self.radio_xy  = QRadioButton("X / Y columns  — separate numeric longitude and latitude columns")
        self.radio_wkt.setChecked(True)
        self._mode_grp.addButton(self.radio_wkt, 0)
        self._mode_grp.addButton(self.radio_xy,  1)
        self._mode_grp.buttonClicked.connect(self._on_mode_changed)
        v2.addWidget(self.radio_wkt); v2.addWidget(self.radio_xy)

        self.pnl_wkt = QWidget()
        hw = QHBoxLayout(self.pnl_wkt); hw.setContentsMargins(20, 0, 0, 0)
        wkt_lbl = QLabel("WKT geometry column:")
        wkt_lbl.setStyleSheet(FONT_TEXT_COLOR)
        hw.addWidget(wkt_lbl)
        self.wkt_combo = QComboBox(); self.wkt_combo.setEnabled(False)
        self.wkt_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.wkt_combo.currentTextChanged.connect(self._refresh_wkt_badge)
        hw.addWidget(self.wkt_combo)
        self.wkt_badge = _badge(""); self.wkt_badge.hide()
        hw.addWidget(self.wkt_badge)
        v2.addWidget(self.pnl_wkt)

        self.pnl_xy = QWidget()
        gxy = QGridLayout(self.pnl_xy); gxy.setContentsMargins(20, 0, 0, 0); gxy.setSpacing(6)
        gxy.addWidget(QLabel("X / Longitude column:"), 0, 0)
        self.x_combo = QComboBox(); self.x_combo.setEnabled(False)
        self.x_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        gxy.addWidget(self.x_combo, 0, 1)
        gxy.addWidget(QLabel("Y / Latitude column:"), 1, 0)
        self.y_combo = QComboBox(); self.y_combo.setEnabled(False)
        self.y_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        gxy.addWidget(self.y_combo, 1, 1)
        self.pnl_xy.hide()
        v2.addWidget(self.pnl_xy)

        h_crs = QHBoxLayout()
        crs_lbl = QLabel("CRS:")
        crs_lbl.setStyleSheet(FONT_TEXT_COLOR)
        h_crs.addWidget(crs_lbl)
        self.crs_widget = QgsProjectionSelectionWidget()
        self.crs_widget.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
        h_crs.addWidget(self.crs_widget)
        v2.addLayout(h_crs)
        v2.addWidget(_hint("ℹ  QGIS opens the CSV via its native delimited text provider."))
        v2.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))
        col2.addWidget(grp2); row12.addLayout(col2, stretch=1)
        root.addLayout(row12)

        # Step 3: type header + preview
        root.addWidget(_section("Data Preview  (first 20 rows)  ·  Set attribute types in the header", 3))
        grp3 = QGroupBox()
        grp3.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        v3 = QVBoxLayout(grp3); v3.setSpacing(0); v3.setContentsMargins(4, 4, 4, 4)

        self._no_preview_lbl = _hint("No CSV loaded yet.")
        self._no_preview_lbl.setAlignment(Qt.AlignCenter)
        v3.addWidget(self._no_preview_lbl, alignment=Qt.AlignCenter)

        # ── Preview table with embedded type-selector header ───────────
        self.preview_tbl = QTableWidget(0, 0)
        self.preview_tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.preview_tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.preview_tbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.preview_tbl.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.preview_tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.preview_tbl.hide()

        # Install the custom header (type combos live inside it)
        self.type_header = _TypeHeaderView(self.preview_tbl)
        self.type_header.typeChanged.connect(self._on_type_changed)
        self.preview_tbl.setHorizontalHeader(self.type_header)
        # Fixed-width columns for readability; user can still drag to resize
        self.type_header.setSectionResizeMode(QHeaderView.Interactive)
        self.type_header.setDefaultSectionSize(_TypeHeaderView.COL_W)
        self.type_header.setStretchLastSection(False)
        self.type_header.setMinimumSectionSize(80)

        v3.addWidget(self.preview_tbl, stretch=1)

        self.stats_lbl = _hint("")
        self.stats_lbl.setAlignment(Qt.AlignCenter)
        self.stats_lbl.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        v3.addWidget(self.stats_lbl, alignment=Qt.AlignHCenter)
        root.addWidget(grp3, stretch=1)

        # ── Steps 4 & 5 side by side ──────────────────────────────────
        row45 = QHBoxLayout(); row45.setSpacing(10)

        # Step 4 — Output GeoPackage
        col4 = QVBoxLayout(); col4.setSpacing(4)
        col4.addWidget(_section("Output GeoPackage", 4))
        grp4 = QGroupBox(); v4 = QVBoxLayout(grp4); v4.setSpacing(6)
        h_out = QHBoxLayout()
        self.out_edit = QLineEdit(); self.out_edit.setPlaceholderText("Path to output .gpkg file…")
        btn_out = QPushButton("Browse…"); btn_out.setObjectName("browse")
        btn_out.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_out.clicked.connect(self._browse_output)
        h_out.addWidget(self.out_edit); h_out.addWidget(btn_out)
        v4.addLayout(h_out)
        h_name = QHBoxLayout()
        layer_name_lbl = QLabel("Layer name:")
        layer_name_lbl.setStyleSheet(FONT_TEXT_COLOR)
        h_name.addWidget(layer_name_lbl)
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("(defaults to CSV filename)")
        h_name.addWidget(self.name_edit); v4.addLayout(h_name)
        self.load_cb = QCheckBox("Add layer to map canvas after export")
        self.load_cb.setStyleSheet("color:#2c4a35;")
        self.load_cb.setChecked(True); v4.addWidget(self.load_cb)
        v4.addWidget(_hint(
            "ℹ  Uses QgsVectorFileWriter.writeAsVectorFormatV3() — same engine as "
            "Layer → Export → Save Features As…  Appends to existing .gpkg or creates a new one."
        ))
        v4.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))
        col4.addWidget(grp4)
        row45.addLayout(col4, stretch=1)

        # Step 5 — QML / Layer Style
        col5 = QVBoxLayout(); col5.setSpacing(4)
        col5.addWidget(_section("Layer Style  (optional)", 5))
        grp5 = QGroupBox(); v5 = QVBoxLayout(grp5); v5.setSpacing(6)

        # QML file picker row
        h_qml = QHBoxLayout()
        qml_lbl = QLabel("QML style file:")
        qml_lbl.setStyleSheet(FONT_TEXT_COLOR)
        h_qml.addWidget(qml_lbl)
        self.qml_edit = QLineEdit()
        self.qml_edit.setPlaceholderText("Path to .qml style file  (optional)…")
        self.qml_edit.setReadOnly(True)
        btn_qml = QPushButton("Browse…"); btn_qml.setObjectName("browse")
        btn_qml.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_qml.clicked.connect(self._browse_qml)
        btn_clear_qml = QPushButton("✕"); btn_clear_qml.setObjectName("clear_qml")
        btn_clear_qml.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_clear_qml.clicked.connect(self._clear_qml)
        h_qml.addWidget(self.qml_edit)
        h_qml.addWidget(btn_qml)
        h_qml.addWidget(btn_clear_qml)
        v5.addLayout(h_qml)

        # Style status indicator
        self.qml_status_lbl = QLabel("No style loaded — layer will use QGIS default rendering.")
        self.qml_status_lbl.setWordWrap(True)
        self.qml_status_lbl.setStyleSheet(
            "color:#6b7c69; font-size:11px; padding:3px 4px;"
            "border:1px dashed #bdc8b8; border-radius:4px; background:#fafcf8;"
        )
        v5.addWidget(self.qml_status_lbl)

        # Options
        self.embed_style_cb = QCheckBox("Embed style in GeoPackage (saves to layer_styles table)")
        self.embed_style_cb.setStyleSheet("color:#2c4a35;")
        self.embed_style_cb.setChecked(True)
        self.embed_style_cb.setEnabled(False)   # enabled only when a QML is loaded
        v5.addWidget(self.embed_style_cb)

        self.apply_canvas_cb = QCheckBox("Apply style to canvas layer after export")
        self.apply_canvas_cb.setStyleSheet("color:#2c4a35;")
        self.apply_canvas_cb.setChecked(True)
        self.apply_canvas_cb.setEnabled(False)  # enabled only when a QML is loaded
        v5.addWidget(self.apply_canvas_cb)

        v5.addWidget(_hint(
            "ℹ  Load a .qml file exported from QGIS (Layer → Styles → Save Style…). "
            "The style is applied before writing so symbology, labels, and field aliases "
            "are preserved inside the GeoPackage."
        ))
        v5.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))
        col5.addWidget(grp5)
        row45.addLayout(col5, stretch=1)

        # Step 6 — Tracklog GeoPackage (optional)
        col6 = QVBoxLayout(); col6.setSpacing(4)
        col6.addWidget(_section("Tracklog GeoPackage  (optional)", 6))
        grp6 = QGroupBox(); v6 = QVBoxLayout(grp6); v6.setSpacing(6)

        # GPKG file picker
        h_tlog = QHBoxLayout()
        tlog_lbl = QLabel("GeoPackage file:")
        tlog_lbl.setStyleSheet(FONT_TEXT_COLOR)
        h_tlog.addWidget(tlog_lbl)
        self.tracklog_edit = QLineEdit()
        self.tracklog_edit.setPlaceholderText("Path to tracklog .gpkg file…")
        self.tracklog_edit.setReadOnly(True)
        btn_tlog = QPushButton("Browse…"); btn_tlog.setObjectName("tracklog_browse")
        btn_tlog.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_tlog.clicked.connect(self._browse_tracklog)
        btn_clear_tlog = QPushButton("✕"); btn_clear_tlog.setObjectName("clear_qml")
        btn_clear_tlog.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_clear_tlog.clicked.connect(self._clear_tracklog)
        h_tlog.addWidget(self.tracklog_edit)
        h_tlog.addWidget(btn_tlog)
        h_tlog.addWidget(btn_clear_tlog)
        v6.addLayout(h_tlog)

        # QML style for tracklog
        h_tqml = QHBoxLayout()
        tqml_lbl = QLabel("QML style:")
        tqml_lbl.setStyleSheet(FONT_TEXT_COLOR)
        h_tqml.addWidget(tqml_lbl)
        self.tracklog_qml_edit = QLineEdit()
        self.tracklog_qml_edit.setPlaceholderText("Path to .qml style  (optional)…")
        self.tracklog_qml_edit.setReadOnly(True)
        btn_tqml = QPushButton("Browse…"); btn_tqml.setObjectName("tracklog_browse")
        btn_tqml.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        btn_tqml.clicked.connect(self._browse_tracklog_qml)
        btn_clear_tqml = QPushButton("✕"); btn_clear_tqml.setObjectName("clear_qml")
        btn_clear_tqml.setFixedWidth(28)
        btn_clear_tqml.clicked.connect(self._clear_tracklog_qml)
        h_tqml.addWidget(self.tracklog_qml_edit)
        h_tqml.addWidget(btn_tqml)
        h_tqml.addWidget(btn_clear_tqml)
        v6.addLayout(h_tqml)

        # Status badge
        self.tracklog_status_lbl = QLabel("No tracklog loaded — will be skipped on Convert.")
        self.tracklog_status_lbl.setWordWrap(True)
        self.tracklog_status_lbl.setStyleSheet(
            "color:#6b7c69; font-size:11px; padding:3px 4px;"
            "border:1px dashed #bdc8b8; border-radius:4px; background:#fafcf8;"
        )
        v6.addWidget(self.tracklog_status_lbl)

        # Options
        self.tracklog_add_canvas_cb = QCheckBox("Add tracklog to map canvas on Convert")
        self.tracklog_add_canvas_cb.setStyleSheet("color:#2c4a35;")
        self.tracklog_add_canvas_cb.setChecked(True)
        v6.addWidget(self.tracklog_add_canvas_cb)

        self.tracklog_apply_qml_cb = QCheckBox("Apply QML style to tracklog on Convert")
        self.tracklog_apply_qml_cb.setStyleSheet("color:#2c4a35;")
        self.tracklog_apply_qml_cb.setChecked(True)
        self.tracklog_apply_qml_cb.setEnabled(False)   # enabled only when a QML is loaded
        v6.addWidget(self.tracklog_apply_qml_cb)

        v6.addWidget(_hint(
            "ℹ  The tracklog will be loaded onto the canvas automatically when you click "
            "Convert to GeoPackage above. Leave blank to skip."
        ))
        v6.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))
        col6.addWidget(grp6)
        row45.addLayout(col6, stretch=1)

        root.addLayout(row45)

        self.progress = QProgressBar(); self.progress.setRange(0, 100); self.progress.hide()
        root.addWidget(self.progress)

        self.status = QLabel()
        self.status.setMinimumHeight(28)
        self._set_status("Ready — load a CSV to begin.")
        root.addWidget(self.status)

        h_btn = QHBoxLayout()
        self.close_btn = QPushButton("Close"); self.close_btn.setObjectName("close")
        self.close_btn.clicked.connect(self.reject)
        self.go_btn = QPushButton("▶   Run Process")
        self.go_btn.setObjectName("convert"); self.go_btn.setEnabled(False)
        self.go_btn.clicked.connect(self._run)
        h_btn.addWidget(self.close_btn)
        h_btn.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Expanding))
        h_btn.addWidget(self.go_btn)
        root.addLayout(h_btn)

    # ------------------------------------------------------------------
    def _banner(self) -> QFrame:
        f = QFrame(); f.setMinimumHeight(64)
        f.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        f.setStyleSheet(
            "background:qlineargradient(x1:0,y1:0,x2:1,y2:0,"
            "stop:0 #1b4332,stop:1 #2d6a4f); border-radius:6px;"
        )
        h = QHBoxLayout(f); h.setContentsMargins(14, 0, 14, 0)
        v = QVBoxLayout()
        t = QLabel("Monthly GeoFASU")
        t.setStyleSheet("color:#fff; font-size:15px; font-weight:bold; background:transparent;")
        s = QLabel("Powered by QGIS")
        s.setStyleSheet("color:#95d5b2; font-size:10px; background:transparent;")
        v.addWidget(t); v.addWidget(s); h.addLayout(v)
        h.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Expanding))
        b = QLabel("v1.0")
        b.setStyleSheet(
            "color:#52b788; font-size:10px; font-weight:bold; background:transparent;"
            "border:1px solid #52b788; border-radius:3px; padding:2px 6px;"
        )
        h.addWidget(b)
        return f

    # ------------------------------------------------------------------
    def _delim(self) -> str:
        return DELIMITERS.get(self.delim_combo.currentText(), ",")

    def _on_delim_changed(self):
        if self.csv_edit.text():
            self._load_preview(self.csv_edit.text())

    def _on_mode_changed(self):
        wkt = self.radio_wkt.isChecked()
        self.pnl_wkt.setVisible(wkt); self.pnl_xy.setVisible(not wkt)
        self._check_ready()

    def _on_type_changed(self, col: int, type_label: str):
        """Re-colour the corresponding header cell in the preview table."""
        from qgis.PyQt.QtGui import QColor, QBrush
        bg, fg = _TYPE_COLORS.get(type_label, ("#2c6e49", "#ffffff"))
        item = self.preview_tbl.horizontalHeaderItem(col)
        if item:
            item.setBackground(QBrush(QColor(bg)))
            item.setForeground(QBrush(QColor(fg)))

    # ------------------------------------------------------------------
    def _browse_csv(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open CSV", "", "CSV files (*.csv);;All files (*)"
        )
        if path:
            self.csv_edit.setText(path)
            self._load_preview(path)

    def _browse_output(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save GeoPackage", "", "GeoPackage (*.gpkg)"
        )
        if path:
            if not path.lower().endswith(".gpkg"):
                path += ".gpkg"
            self.out_edit.setText(path)
            self._check_ready()

    def _browse_qml(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load QML Style", "", "QGIS Layer Style (*.qml);;All files (*)"
        )
        if path:
            self._set_qml(path)

    def _set_qml(self, path: str):
        self.qml_edit.setText(path)
        fname = os.path.basename(path)
        self.qml_status_lbl.setText(f"✔  Style loaded:  {fname}")
        self.qml_status_lbl.setStyleSheet(
            "color:#1b4332; font-size:11px; padding:3px 4px;"
            "border:1px solid #52b788; border-radius:4px; background:#d8f3dc;"
        )
        self.embed_style_cb.setEnabled(True)
        self.apply_canvas_cb.setEnabled(True)

    def _clear_qml(self):
        self.qml_edit.clear()
        self.qml_status_lbl.setText("No style loaded — layer will use QGIS default rendering.")
        self.qml_status_lbl.setStyleSheet(
            "color:#6b7c69; font-size:11px; padding:3px 4px;"
            "border:1px dashed #bdc8b8; border-radius:4px; background:#fafcf8;"
        )
        self.embed_style_cb.setEnabled(False)
        self.apply_canvas_cb.setEnabled(False)

    # ------------------------------------------------------------------
    # Tracklog GeoPackage handlers
    # ------------------------------------------------------------------

    def _browse_tracklog(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Tracklog GeoPackage", "", "GeoPackage (*.gpkg);;All files (*)"
        )
        if path:
            self._set_tracklog(path)

    def _set_tracklog(self, path: str):
        """Validate the GPKG and update the status badge."""
        self.tracklog_edit.setText(path)
        probe = QgsVectorLayer(path, "probe", "ogr")
        if not probe.isValid():
            self.tracklog_status_lbl.setText("⚠  Could not open this GeoPackage — check the file.")
            self.tracklog_status_lbl.setStyleSheet(
                "color:#721c24; font-size:11px; padding:3px 4px;"
                "border:1px solid #f5c6cb; border-radius:4px; background:#f8d7da;"
            )
            return
        fname = os.path.basename(path)
        self.tracklog_status_lbl.setText(f"✔  {fname}  —  will load on Convert")
        self.tracklog_status_lbl.setStyleSheet(
            "color:#1a237e; font-size:11px; padding:3px 4px;"
            "border:1px solid #9fa8da; border-radius:4px; background:#e8eaf6;"
        )

    def _clear_tracklog(self):
        self.tracklog_edit.clear()
        self.tracklog_qml_edit.clear()
        self.tracklog_apply_qml_cb.setChecked(False)
        self.tracklog_apply_qml_cb.setEnabled(False)
        self.tracklog_status_lbl.setText("No tracklog loaded — will be skipped on Convert.")
        self.tracklog_status_lbl.setStyleSheet(
            "color:#6b7c69; font-size:11px; padding:3px 4px;"
            "border:1px dashed #bdc8b8; border-radius:4px; background:#fafcf8;"
        )

    def _browse_tracklog_qml(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Tracklog QML Style", "", "QGIS Layer Style (*.qml);;All files (*)"
        )
        if path:
            self.tracklog_qml_edit.setText(path)
            self.tracklog_apply_qml_cb.setEnabled(True)
            self.tracklog_apply_qml_cb.setChecked(True)

    def _clear_tracklog_qml(self):
        self.tracklog_qml_edit.clear()
        self.tracklog_apply_qml_cb.setChecked(False)
        self.tracklog_apply_qml_cb.setEnabled(False)

    def _load_tracklog_to_canvas(self) -> dict:
        """Open the tracklog GPKG and add its main layer to the canvas.
        Returns a dict with keys: ok, error (if any), layer_name."""
        gpkg_path = self.tracklog_edit.text().strip()
        qml_path  = self.tracklog_qml_edit.text().strip()

        if not gpkg_path:
            return {"ok": False, "error": "No tracklog path set."}

        # Use the bare path so OGR opens the first / main layer automatically
        layer_name = os.path.splitext(os.path.basename(gpkg_path))[0]
        vlayer = QgsVectorLayer(gpkg_path, layer_name, "ogr")

        if not vlayer.isValid():
            return {
                "ok": False,
                "error": f"Could not open tracklog:\n{gpkg_path}",
            }

        if qml_path and os.path.isfile(qml_path) and self.tracklog_apply_qml_cb.isChecked():
            msg, ok = vlayer.loadNamedStyle(qml_path)
            if not ok:
                # Non-fatal — carry on without style
                pass

        if self.tracklog_add_canvas_cb.isChecked():
            QgsProject.instance().addMapLayer(vlayer)

        return {"ok": True, "layer_name": layer_name}

    # ------------------------------------------------------------------
    def _load_preview(self, path: str):
        self._set_status("Reading CSV…", warn=True)
        try:
            meta = _sniff_csv(path, self._delim())
        except Exception as exc:
            self._set_status(f"Error reading CSV: {exc}", error=True)
            return

        self._fields       = meta["fields"]
        self._encoding     = meta["encoding"]
        self._preview_rows = meta["preview"]
        wkt_cols           = meta["wkt_cols"]
        total              = meta["row_count"]

        # Infer field types from sample values
        inferred = [
            _infer_field_type([r.get(f, "") for r in self._preview_rows], f)
            for f in self._fields
        ]

        # Preview table
        self.preview_tbl.setRowCount(len(self._preview_rows))
        self.preview_tbl.setColumnCount(len(self._fields))
        self.preview_tbl.setHorizontalHeaderLabels(self._fields)
        for r, row in enumerate(self._preview_rows):
            for c, f in enumerate(self._fields):
                item = QTableWidgetItem(str(row.get(f, "")))
                item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                self.preview_tbl.setItem(r, c, item)
        self.preview_tbl.show()
        self._no_preview_lbl.hide()

        # Colour header cells to match inferred types
        from qgis.PyQt.QtGui import QColor, QBrush
        for col, dtype in enumerate(inferred):
            bg, fg = _TYPE_COLORS.get(dtype, ("#2c6e49", "#fff"))
            item = self.preview_tbl.horizontalHeaderItem(col)
            if item:
                item.setBackground(QBrush(QColor(bg)))
                item.setForeground(QBrush(QColor(fg)))

        # Type header — populate after columns exist so sectionSize() is valid
        self.type_header.populate(self._fields, inferred)

        # WKT / XY combos
        self.wkt_combo.blockSignals(True)
        self.wkt_combo.clear()
        self.wkt_combo.addItems(self._fields)
        self.wkt_combo.setEnabled(True)
        if wkt_cols:
            self.wkt_combo.setCurrentIndex(self._fields.index(wkt_cols[0]))
            self.radio_wkt.setChecked(True)
        else:
            self.radio_xy.setChecked(True)
        self.wkt_combo.blockSignals(False)
        self._refresh_wkt_badge(self.wkt_combo.currentText())

        self.x_combo.clear(); self.y_combo.clear()
        self.x_combo.addItems(self._fields); self.y_combo.addItems(self._fields)
        self.x_combo.setEnabled(True);       self.y_combo.setEnabled(True)
        lower = [f.lower() for f in self._fields]
        for hint in ["lon", "longitude", "x", "lng", "easting"]:
            for i, lf in enumerate(lower):
                if hint in lf: self.x_combo.setCurrentIndex(i); break
        for hint in ["lat", "latitude", "y", "northing"]:
            for i, lf in enumerate(lower):
                if hint in lf: self.y_combo.setCurrentIndex(i); break

        self.name_edit.setPlaceholderText(os.path.splitext(os.path.basename(path))[0])
        self._on_mode_changed()
        self._set_status(
            f"CSV loaded — {total} rows, {len(self._fields)} columns  ·  Encoding: {self._encoding}"
            + ("  ·  WKT auto-detected ✓" if wkt_cols else "")
        )
        self._check_ready()

    def _refresh_wkt_badge(self, col_name: str):
        import re
        _WKT_RE = re.compile(
            r"^\s*(point|multipoint|linestring|multilinestring|"
            r"polygon|multipolygon|geometrycollection)",
            re.IGNORECASE,
        )
        for row in self._preview_rows:
            val = row.get(col_name, "").strip()
            if val:
                m = _WKT_RE.match(val)
                if m:
                    self.wkt_badge.setText(m.group(1).upper())
                    self.wkt_badge.show(); return
        self.wkt_badge.hide()

    # ------------------------------------------------------------------
    def _check_ready(self):
        ok = bool(self.csv_edit.text() and self.out_edit.text() and self._fields)
        self.go_btn.setEnabled(ok)

    # ------------------------------------------------------------------
    def _run(self):
        crs = self.crs_widget.crs()
        if not crs.isValid():
            self._set_status("Invalid CRS selected.", error=True); return

        params = {
            "csv_path":    self.csv_edit.text().strip(),
            "output_path": self.out_edit.text().strip(),
            "layer_name":  self.name_edit.text().strip(),
            "crs_authid":  crs.authid(),
            "delimiter":   self._delim(),
            "encoding":    self._encoding,
            "mode":        "wkt" if self.radio_wkt.isChecked() else "xy",
            "field_types": self.type_header.get_type_map(self._fields),
            "qml_path":    self.qml_edit.text().strip(),
            "embed_style": self.embed_style_cb.isChecked() and self.embed_style_cb.isEnabled(),
        }

        if params["mode"] == "wkt":
            params["wkt_field"] = self.wkt_combo.currentText()
        else:
            if self.x_combo.currentText() == self.y_combo.currentText():
                self._set_status("X and Y must be different columns.", error=True); return
            params["x_field"] = self.x_combo.currentText()
            params["y_field"] = self.y_combo.currentText()

        self.go_btn.setEnabled(False)
        self.close_btn.setText("Abort")
        self.close_btn.clicked.disconnect()
        self.close_btn.clicked.connect(self._abort)
        self.progress.setValue(0); self.progress.show()
        self._set_status("Converting…", warn=True)

        self._worker = _Worker(params)
        self._worker.progress.connect(
            lambda p, m: (self.progress.setValue(p),
                          self.progress.setFormat(f"  {p}%  —  {m}"))
        )
        self._worker.finished.connect(self._on_done)
        self._worker.start()

    def _abort(self):
        if self._worker and self._worker.isRunning():
            self._worker.terminate(); self._worker.wait()
        self._unlock()
        self._set_status("Aborted.", warn=True)

    def _on_done(self, result: dict):
        self._unlock()
        if result["ok"]:
            n, out, lyr = result["written"], result["output"], result["layer_name"]
            qml_applied = result.get("qml_applied", False)
            self.progress.setValue(100)

            # Load tracklog alongside the converted layer
            tracklog_note = ""
            if self.tracklog_edit.text().strip():
                tresult = self._load_tracklog_to_canvas()
                if tresult["ok"]:
                    qml_path = self.tracklog_qml_edit.text().strip()
                    style_tag = (
                        f"  ·  Style applied ✓"
                        if qml_path and os.path.isfile(qml_path) and self.tracklog_apply_qml_cb.isChecked()
                        else ""
                    )
                    tracklog_note = (
                        f"<br>Tracklog: <code>{tresult['layer_name']}</code> added to canvas{style_tag}"
                    )
                    self.tracklog_status_lbl.setText(
                        f"✔  '{tresult['layer_name']}' loaded to canvas{style_tag}"
                    )
                    self.tracklog_status_lbl.setStyleSheet(
                        "color:#1b4332; font-size:11px; padding:3px 4px;"
                        "border:1px solid #52b788; border-radius:4px; background:#d8f3dc;"
                    )
                else:
                    tracklog_note = f"<br><span style='color:red'>Tracklog failed: {tresult['error']}</span>"
                    self.tracklog_status_lbl.setText(f"⚠  {tresult['error']}")
                    self.tracklog_status_lbl.setStyleSheet(
                        "color:#721c24; font-size:11px; padding:3px 4px;"
                        "border:1px solid #f5c6cb; border-radius:4px; background:#f8d7da;"
                    )

            self._set_status(
                f"Done — {n} features written to {os.path.basename(out)}"
                + ("  ·  Style embedded ✓" if qml_applied and self.embed_style_cb.isChecked() else "")
            )
            if self.load_cb.isChecked():
                vlayer = QgsVectorLayer(f"{out}|layername={lyr}", lyr, "ogr")
                if vlayer.isValid():
                    qml_path = self.qml_edit.text().strip()
                    if qml_path and os.path.isfile(qml_path) and self.apply_canvas_cb.isChecked():
                        vlayer.loadNamedStyle(qml_path)
                    QgsProject.instance().addMapLayer(vlayer)
            style_note = (
                f"<br>Style: <code>{os.path.basename(self.qml_edit.text())}</code> applied ✓"
                if qml_applied else ""
            )
            QMessageBox.information(
                self, "Conversion complete",
                f"<b>{n}</b> features written.<br><br>"
                f"File: <code>{out}</code><br>Layer: <code>{lyr}</code>"
                f"{style_note}{tracklog_note}",
            )
        else:
            self._set_status(f"Failed: {result['error']}", error=True)
            QMessageBox.critical(self, "Conversion failed", result["error"])

    def _unlock(self):
        self.go_btn.setEnabled(True)
        self.close_btn.setText("Close")
        self.close_btn.clicked.disconnect()
        self.close_btn.clicked.connect(self.reject)

    # ------------------------------------------------------------------
    def _set_status(self, msg: str, warn=False, error=False):
        if error:   bg, fg = "#f8d7da", "#721c24"
        elif warn:  bg, fg = "#fff3cd", "#856404"
        else:       bg, fg = "#e8f0e4", "#2c4a35"
        self.status.setText(f"  {msg}")
        self.status.setStyleSheet(
            f"background:{bg}; border:1px solid {fg}55;"
            f"border-radius:4px; padding:5px 8px; color:{fg}; font-size:11px;"
        )
