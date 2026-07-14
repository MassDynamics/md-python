"""Gene-set collection catalogue for the CAMERA GSEA job (``sets`` param).

Source-of-truth: the live ``/jobs`` catalogue, slug ``camera_gsea`` ->
``EnrichmentParamsProperties.properties.sets.parameters.options``. That node is
a SPECIES-CONDITIONAL map — ``{"ref": "species", "cases": {"Human": [...],
"Mouse": [...], ...}}`` — so the legal values for ``sets`` depend on the chosen
``species``. The values below are transcribed verbatim from it.

WHY THIS MODULE EXISTS
----------------------
The backend SILENTLY DROPS a ``sets`` value it does not recognise: the job is
accepted, reports COMPLETED, and simply never runs that knowledge base. A run
submitted with ``["GO - Biological Process", "GO - Cellular Component",
"GO - Molecular Function", "Hallmark"]`` returns
``runtime_metadata.databases = "BiologicalProcess,CellularComponents,
MolecularFunction"`` — four requested, three run, no error. ``"Hallmark"`` is
not a legal value (the real string is ``"MSigDB-H (hallmark gene sets)"``).
:class:`GseaSetsValidator` turns that silent scientific error into a
fail-fast ValueError before submission.
"""

import difflib
import re
from typing import Dict, List, Mapping, Optional, Sequence

# Verbatim from EnrichmentParamsProperties.properties.sets.parameters.options.cases.
# NOTE the Mouse MSigDB prefixes are MH/M1/M2/M3/M5/M7/M8 — NOT the Human
# C-numbers. Chinese hamster has no Reactome collection.
GSEA_SETS_BY_SPECIES: Dict[str, List[str]] = {
    "Human": [
        "Reactome",
        "GO - Biological Process",
        "GO - Cellular Component",
        "GO - Molecular Function",
        "MSigDB-H (hallmark gene sets)",
        "MSigDB-C1 (positional gene sets)",
        "MSigDB-C2 (curated gene sets)",
        "MSigDB-C3 (regulatory target gene sets)",
        "MSigDB-C4 (computational gene sets)",
        "MSigDB-C5 (ontology gene sets)",
        "MSigDB-C6 (oncogenic signature gene sets)",
        "MSigDB-C7 (immunologic signature gene sets)",
        "MSigDB-C8 (cell type signature gene sets)",
        "MSigDB-C9 (computational perturbation signature gene sets)",
    ],
    "Mouse": [
        "Reactome",
        "GO - Biological Process",
        "GO - Cellular Component",
        "GO - Molecular Function",
        "MSigDB-MH (hallmark gene sets)",
        "MSigDB-M1 (positional gene sets)",
        "MSigDB-M2 (curated gene sets)",
        "MSigDB-M3 (regulatory target gene sets)",
        "MSigDB-M5 (ontology gene sets)",
        "MSigDB-M7 (immunologic signature gene sets)",
        "MSigDB-M8 (cell type signature gene sets)",
    ],
    "Yeast": [
        "Reactome",
        "GO - Biological Process",
        "GO - Cellular Component",
        "GO - Molecular Function",
    ],
    "Chinese hamster": [
        "GO - Biological Process",
        "GO - Cellular Component",
        "GO - Molecular Function",
    ],
}

# Backend default (the three GO sets) — EnrichmentParamsProperties.sets.default.
GSEA_DEFAULT_SETS: List[str] = [
    "GO - Biological Process",
    "GO - Cellular Component",
    "GO - Molecular Function",
]

# Shorthands a human or an LLM actually reaches for, mapped to the collection
# they mean. The collection key is the parenthetical descriptor of the canonical
# value, so the species-correct spelling is looked up from it (Human MSigDB-H vs
# Mouse MSigDB-MH).
_ALIAS_HINTS: Dict[str, str] = {
    "hallmark": "hallmark gene sets",
    "hallmarks": "hallmark gene sets",
    "hallmark gene sets": "hallmark gene sets",
    "msigdb hallmark": "hallmark gene sets",
    "msigdb-hallmark": "hallmark gene sets",
    "msigdb h": "hallmark gene sets",
    "msigdb mh": "hallmark gene sets",
}
# "MSigDB-H" / "MSigDB-MH" need no alias — they are the catalogue prefixes and
# are indexed automatically (see GseaSetsValidator._build_token_index).

_PAREN_RE = re.compile(r"^(?P<prefix>.+?)\s*\((?P<descriptor>.+)\)$")


class GseaSetsValidator:
    """Validate and canonicalise the CAMERA GSEA ``sets`` parameter per species.

    Matching policy (deliberate):
      * Values are matched after trimming and collapsing internal whitespace,
        CASE-INSENSITIVELY, and are normalised to the catalogue's exact
        spelling. Rationale: the canonical strings are long and punctuated
        ("MSigDB-C2 (curated gene sets)"); a case or spacing difference is an
        unambiguous typo with no other possible meaning, so silently correcting
        it is safe and strictly reduces the failure surface.
      * ANYTHING ELSE IS REJECTED — never dropped. Shorthands such as
        "Hallmark" or "MSigDB-H" are NOT accepted as aliases, because guessing
        at intent is exactly the class of error this validator exists to
        prevent. They are rejected with a hint naming the species-correct
        spelling.
      * Duplicates (including case-variant duplicates) collapse to the first
        occurrence, preserving order.
    """

    def __init__(
        self, catalogue: Mapping[str, Sequence[str]] = GSEA_SETS_BY_SPECIES
    ) -> None:
        self._catalogue = {sp: list(values) for sp, values in catalogue.items()}
        # species -> {normalised value: canonical value}
        self._by_species: Dict[str, Dict[str, str]] = {
            sp: {self._normalise(v): v for v in values}
            for sp, values in self._catalogue.items()
        }
        # species -> {collection key: canonical value}
        self._collections: Dict[str, Dict[str, str]] = {
            sp: {self._collection_key(v): v for v in values}
            for sp, values in self._catalogue.items()
        }
        self._token_index = self._build_token_index()

    def canonicalise(self, sets: Sequence[str], species: str) -> List[str]:
        """Return *sets* in catalogue spelling, or raise ValueError.

        Raises ValueError if *species* is unknown, if *sets* is empty or holds
        non-strings, or if any value is not a knowledge base of *species*.
        """
        if species not in self._by_species:
            raise ValueError(
                f"species must be one of: {sorted(self._by_species)} (got {species!r})"
            )
        if not sets:
            raise ValueError("sets cannot be empty")
        if not all(isinstance(v, str) for v in sets):
            raise ValueError("sets must be a list of strings")

        known = self._by_species[species]
        canonical: List[str] = []
        unknown: List[str] = []
        for value in sets:
            match = known.get(self._normalise(value))
            if match is None:
                unknown.append(value)
            elif match not in canonical:
                canonical.append(match)

        if unknown:
            raise ValueError(self._error_message(unknown, species))
        return canonical

    def valid_values(self, species: str) -> List[str]:
        """Return the knowledge bases available for *species*."""
        return list(self._catalogue[species])

    # ── error reporting ──────────────────────────────────────────────────────

    def _error_message(self, unknown: Sequence[str], species: str) -> str:
        offenders = ", ".join(repr(v) for v in unknown)
        hints = [h for h in (self._hint(v, species) for v in unknown) if h]
        parts = [
            f"sets contains {len(unknown)} value(s) that are not valid knowledge "
            f"bases for species '{species}': {offenders}. "
            "The backend SILENTLY DROPS unrecognised sets (the job still reports "
            "COMPLETED without running them), so this is rejected here.",
        ]
        parts.extend(hints)
        parts.append(
            f"Valid sets for '{species}': {self._catalogue[species]}. "
            "Values are matched case-insensitively after whitespace is trimmed; "
            "no other spelling is accepted."
        )
        return " ".join(parts)

    def _hint(self, value: str, species: str) -> Optional[str]:
        key = self._token_index.get(self._normalise(value))
        if key is None:
            return self._close_match_hint(value, species)
        canonical = self._collections[species].get(key)
        if canonical is not None:
            return f"Did you mean {canonical!r}? (for {value!r})"
        available = [sp for sp, cols in self._collections.items() if key in cols]
        return (
            f"{value!r} names the '{key}' collection, which is not available "
            f"for '{species}' (available for: {', '.join(sorted(available))})."
        )

    def _close_match_hint(self, value: str, species: str) -> Optional[str]:
        matches = difflib.get_close_matches(
            self._normalise(value), list(self._by_species[species]), n=1, cutoff=0.6
        )
        if not matches:
            return None
        return (
            f"Did you mean {self._by_species[species][matches[0]]!r}? (for {value!r})"
        )

    # ── indexing helpers ─────────────────────────────────────────────────────

    def _build_token_index(self) -> Dict[str, str]:
        """Map every recognisable shorthand onto a collection key.

        Indexed: the full canonical string, its prefix ("MSigDB-C2"), its
        parenthetical descriptor ("curated gene sets"), for EVERY species — so a
        Human-only value passed with species="Mouse" resolves to the collection
        and the Mouse spelling can be suggested — plus the hand-written aliases.
        """
        index: Dict[str, str] = {}
        for values in self._catalogue.values():
            for value in values:
                key = self._collection_key(value)
                index[self._normalise(value)] = key
                index[self._normalise(self._prefix(value))] = key
                index[key] = key
        index.update(_ALIAS_HINTS)
        return index

    def _collection_key(self, value: str) -> str:
        """Species-independent identity of a knowledge base.

        The parenthetical descriptor when there is one ("hallmark gene sets" for
        both Human MSigDB-H and Mouse MSigDB-MH), else the value itself.
        """
        match = _PAREN_RE.match(value.strip())
        if match is None:
            return self._normalise(value)
        return self._normalise(match.group("descriptor"))

    def _prefix(self, value: str) -> str:
        match = _PAREN_RE.match(value.strip())
        return match.group("prefix") if match else value

    @staticmethod
    def _normalise(value: str) -> str:
        return " ".join(value.split()).casefold()
