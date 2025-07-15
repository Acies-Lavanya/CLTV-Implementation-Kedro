"""
Column‑mapping helper.
• Reads YAML files in conf/base/column_aliases/<table>.yml
• Renames raw columns → canonical snake_case names used by the pipeline
"""

from __future__ import annotations

from pathlib import Path
import re
import yaml
import pandas as pd
from typing import Dict, List

# Folder that contains the alias YAMLs
ALIAS_DIR = Path.cwd() / "conf" / "base" / "column_aliases"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _snake(s: str) -> str:
    """Convert 'User ID' → 'user_id'."""
    return re.sub(r"[\W_]+", "_", s.strip()).lower()


def _load_alias_file(table: str) -> Dict[str, List[str]]:
    """Return {canonical: [aliases]} for one table."""
    file_path = ALIAS_DIR / f"{table}.yml"
    if not file_path.exists():
        raise FileNotFoundError(f"Alias YAML not found: {file_path}")
    with file_path.open("r", encoding="utf‑8") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def standardize_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Rename *in‑place* using the alias YAML for `data_type`.
    The canonical keys in YAML are converted to snake_case.
    """
    alias_map = _load_alias_file(data_type)
    canonical_snake_map: Dict[str, str] = {
        _snake(canon): canon for canon in alias_map
    }

    # Build reverse lookup {raw_col_normalised: canonical_snake}
    reverse: Dict[str, str] = {}
    for canon, variations in alias_map.items():
        canon_snake = _snake(canon)
        reverse[_snake(canon)] = canon_snake  # include canonical itself
        for v in variations:
            reverse[_snake(v)] = canon_snake

    # Rename
    rename_dict = {}
    for col in df.columns:
        key = _snake(col)
        if key in reverse:
            rename_dict[col] = reverse[key]
    return df.rename(columns=rename_dict)
