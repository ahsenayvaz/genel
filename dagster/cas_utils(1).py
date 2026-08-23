import psycopg2  # PostgreSQL driver used to connect and run SQL statements
from dotenv import load_dotenv  # Load DB credentials from .env files into environment variables
import html  # Unescape HTML entities such as &auml; -> ä found in the source data
import re  # Regular expressions used to strip HTML tags and normalize whitespace

# ----------------------------------------------------------------------------------------------------------------------
# 🧹 HELPER FUNCTIONS --------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

def clean_html(text):
    """
    Cleans a string from HTML artifacts and formatting.

    The source CSV contains HTML entities (e.g. &auml; for "ä") and inline
    HTML tags (e.g. <sub>, <i>) used for formatting chemical formulas.
    This function normalizes such values into plain text.

    Steps performed:
    1. Handle empty input
    2. Convert HTML entities (e.g. &auml; → ä)
    3. Remove HTML tags (e.g. <sub>, <i>)
    4. Trim whitespace

    Example:
        "C<sub>8</sub>H<sub>10</sub>" → "C8H10"
    """
    # Test if the input is empty
    if text is None:
        return None

    # If a list comes in (e.g. from csv.DictReader's "restkey" collecting
    # extra/overflow columns into a list), clean every element recursively.
    if isinstance(text, list):
        return [clean_html(t) for t in text]

    # Ensure we always operate on a plain string from here on.
    text = str(text)
    # Convert HTML entities like &auml; into their actual character (ä).
    text = html.unescape(text)
    # Strip out any HTML tags such as <sub>...</sub> or <i>...</i>.
    text = re.sub(r"<.*?>", "", text)
    # Remove leading/trailing whitespace left over after tag removal.
    text = text.strip()

    # Treat empty strings and a lone "-" (used in the source as "no value")
    # as "no data" and return None instead of a meaningless placeholder.
    return text if text and text != "-" else None

def split_names(value):
    """
    Splits a pipe- or slash-separated string into a clean list of values.

    Several columns in the source file can contain multiple values packed
    into a single cell, separated either by "|" or by "/". This function
    detects the separator in use and splits accordingly.

    Used for fields like:
        "A|B|C"

    Steps:
    1. Handle empty input
    2. Detect whether "/" or "|" is used as the separator
    3. Split by that separator
    4. Strip whitespace from each element
    5. Remove empty entries (e.g. from "A||B")

    Example:
        "A | B |  | C" → ["A", "B", "C"]
    """

    # Test if the input is empty (None, empty string, etc.)
    if not value:
        return []

    # Ensure we're working with a string (defensive, in case a non-string value like a number sneaks in).
    value = str(value)

    # Some fields use "/" as the separator between multiple values.
    if "/" in value:
        return [v.strip() for v in value.split("/") if v.strip()]

    # Most fields (e.g. OCAS, SYN*) use "|" as the separator.
    if "|" in value:
        return [v.strip() for v in value.split("|") if v.strip()]

    # No separator found: treat the whole (trimmed) string as a single value.
    return [value.strip()]


def normalize_name(name):
    """
    Makes a substance name comparable/matchable across different sources
    by removing formatting differences that don't affect the actual name:
    - lowercase everything
    - remove hyphens
    - collapse repeated whitespace into single spaces

    This "name_normalized" value is what later matching/joining logic
    should use instead of the original, human-formatted name.

    Example:
        "Ibuprofen-sodium" → "ibuprofen sodium"
    """
    if not name:
        return None
    # Lowercase so "Ibuprofen" and "ibuprofen" are treated as identical.
    name = name.lower()
    # Remove hyphens so "hydrochloride-salt" and "hydrochloride salt" match.
    name = name.replace("-", " ")
    # Collapse any run of whitespace (multiple spaces, tabs, etc.) into one space.
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def extract_names(context, row):
    """
    Collects every usable name/synonym for a single substance row from the
    various name-related columns in the source CSV, de-duplicating as it goes.

    A single substance can have its name spread across several columns:
    - HBEZ1 / HBEZ2 / HBEZ3: the primary display name fields (in priority order)
    - Any column starting with "SYN" (e.g. SYN1, SYN2, ...): synonym fields
    - EXTRA: overflow column collected by csv.DictReader's restkey when a row
      has more semicolon-separated fields than there are header columns
      (this happens for rows with many pipe-separated synonyms)

    Returns a flat, de-duplicated list of clean name strings.
    """
    names = []
    seen = set()  # Tracks names already added, to avoid duplicates

    def add(val):
        # Local helper: clean a raw cell value and add each of its
        # (possibly multiple, pipe/slash-separated) parts to the result list.
        val = clean_html(val)
        if not val:
            return

        for part in split_names(val):
            if part and part not in seen:
                seen.add(part)
                names.append(part)

    # 1. Main display-name fields, in priority order.
    for f in ["HBEZ1", "HBEZ2", "HBEZ3"]:
        add(row.get(f))

    # 2. Any synonym field (column name starts with "SYN", e.g. SYN1, SYN2, ...).
    for k, v in row.items():
        if k and k.startswith("SYN"):
            add(v)

    # 3. IMPORTANT: resolve the "EXTRA" overflow column correctly.
    # csv.DictReader collects any columns beyond the defined header into a
    # list under the "restkey" (configured below as "EXTRA"). This happens
    # for rows that have more semicolon-separated synonym values than the
    # header has columns for.
    extra = row.get("EXTRA")

    if isinstance(extra, list):
        for v in extra:
            add(v)
    else:
        add(extra)

    return names
