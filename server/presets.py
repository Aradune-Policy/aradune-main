"""Preset filter definitions for common query patterns."""

from server.models import PresetInfo


# ── CCBHC code lists from Milliman/AHCA Appendix I (April 2025) ──────
_CCBHC_CORE = [
    # Screening, Assessment, and Diagnosis
    "H2000", "H0031", "H0001",
    # Person-Centered and Family-Centered Treatment Planning
    "H0032", "T1007",
    # Outpatient Mental Health and Substance Use Services
    "H0015", "H0020", "H2019", "H0040", "H2033",
    # Primary Care Screening and Monitoring
    "99385", "99386", "99387", "99395", "99396", "99397", "96110",
    # Targeted Case Management
    "T1017",
    # Psychiatric Rehabilitation Services
    "H2017",
    # Peer Supports and Family/Caregiver Supports
    "H0018", "H0038",
    # Crisis Services
    "H2011", "S9484",
]

_CCBHC_EXPANDED_ONLY = [
    # Screening, Assessment, and Diagnosis (expanded)
    "H2010", "96160",
    # Outpatient MH and SU Services (expanded)
    "H0046", "H0047", "H0048", "H2012", "T1015", "T1023", "S9480",
    # Psychiatric Rehabilitation (expanded)
    "H2030",
    # Peer Supports (expanded)
    "S5102",
    # Other Medical Items or Services
    "H0035", "S9475",
]

_CCBHC_ALL = list(set(_CCBHC_CORE + _CCBHC_EXPANDED_ONLY))

PRESETS: dict[str, PresetInfo] = {
    "ccbhc_core": PresetInfo(
        id="ccbhc_core",
        name="CCBHC Core",
        description="Core CCBHC services per Milliman/AHCA Appendix I — the 9 required SAMHSA service categories",
        codes=_CCBHC_CORE,
        filter_type="hcpcs_codes",
    ),
    "ccbhc_expanded": PresetInfo(
        id="ccbhc_expanded",
        name="CCBHC Expanded",
        description="Core + Expanded CCBHC services per Milliman/AHCA Appendix I — includes additional EBPs and service types",
        codes=_CCBHC_ALL,
        filter_type="hcpcs_codes",
    ),
    "hcbs_waiver": PresetInfo(
        id="hcbs_waiver",
        name="HCBS / Waiver",
        description="Home and community-based services and waiver program codes",
        codes=[
            "T1019", "T1020", "T2025", "T2026", "T2027", "T2028",
            "S5130", "S5125", "S5150", "S5151",
            "T1005", "T2021", "S5100", "S5102",
            "T2003", "T1030", "T1031",
        ],
        filter_type="hcpcs_codes",
    ),
    "behavioral_health": PresetInfo(
        id="behavioral_health",
        name="Behavioral Health",
        description="Mental health, substance use, and ABA therapy codes",
        codes=[
            "90832", "90834", "90837", "90846", "90847", "90853",
            "90839", "90840", "90791", "90792",
            "97151", "97152", "97153", "97154", "97155", "97156", "97158",
            "H0004", "H0005", "H0015", "H0020", "H0031", "H0032",
            "H0038", "H2017", "H2018",
        ],
        filter_type="hcpcs_codes",
    ),
    "dental": PresetInfo(
        id="dental",
        name="Dental",
        description="Common dental procedure codes",
        codes=[
            "D0120", "D0150", "D0210", "D0220", "D0230", "D0272", "D0274",
            "D1110", "D1120", "D1208",
            "D2140", "D2150", "D2391", "D2392",
            "D3220", "D3310",
            "D7140", "D7210",
        ],
        filter_type="hcpcs_codes",
    ),
    "em": PresetInfo(
        id="em",
        name="E&M Services",
        description="Evaluation and management office visit codes",
        codes=[
            "99202", "99203", "99204", "99205",
            "99211", "99212", "99213", "99214", "99215",
            "99281", "99282", "99283", "99284", "99285",
            "99381", "99382", "99383", "99384", "99385",
            "99391", "99392", "99393", "99394", "99395",
        ],
        filter_type="hcpcs_codes",
    ),
    "top_spending": PresetInfo(
        id="top_spending",
        name="Top Spending",
        description="Query sorted by total spending (no code filter, just ordering)",
        codes=[],
        filter_type="ordering",
    ),
}


def get_preset(preset_id: str) -> PresetInfo | None:
    return PRESETS.get(preset_id)


def list_presets() -> list[PresetInfo]:
    return list(PRESETS.values())
