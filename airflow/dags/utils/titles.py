"""Paper titles as stored: without the markup publishers leave in their metadata."""

import html
import re
from typing import Optional

# Inline markup found in Crossref / OpenAlex titles (JATS, HTML, MathML). Only
# these tags are removed, so a title such as "x < y and y > z" keeps its text.
_MARKUP_TAG = re.compile(
    r"</?(?:i|b|em|strong|u|sup|sub|scp|sc|span|italic|bold|small|underline"
    r"|inline-formula|tex-math|alternatives|mml:[a-z]+)\b[^<>]*>",
    re.IGNORECASE,
)


def clean_title(title: Optional[str]) -> Optional[str]:
    """
    The title without publisher markup: entities decoded ("&amp;" -> "&"),
    known inline tags dropped with their text kept ("<scp>Medication</scp>" ->
    "Medication"), whitespace collapsed. None stays None; an empty result is None.
    """
    if not isinstance(title, str):
        return title
    text = _MARKUP_TAG.sub("", html.unescape(title))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None
