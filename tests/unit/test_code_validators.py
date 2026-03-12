"""
Unit tests for code format validators and data normalization functions.
Uses Hypothesis for property-based testing of edge cases.
"""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


# ── Code format validators (used in ingestion pipelines) ────────────


def is_valid_state_code(code: str) -> bool:
    """Two uppercase ASCII letters."""
    return bool(re.match(r"^[A-Z]{2}$", code))


def is_valid_npi(npi: str) -> bool:
    """10-digit numeric string."""
    return bool(re.match(r"^\d{10}$", npi))


def is_valid_cpt_hcpcs(code: str) -> bool:
    """5 chars: 5 digits (CPT) or alpha + 4 digits (HCPCS Level II)."""
    return bool(re.match(r"^[A-Z0-9]\d{4}$", code))


def is_valid_ndc_11(ndc: str) -> bool:
    """11-digit NDC in 5-4-2 format (no dashes) or raw 11 digits."""
    return bool(re.match(r"^\d{11}$", ndc))


def normalize_ndc_to_11(ndc: str) -> str:
    """Normalize various NDC formats to 11-digit 5-4-2."""
    clean = re.sub(r"[^0-9]", "", ndc)
    if len(clean) == 11:
        return clean
    if len(clean) == 10:
        return "0" + clean
    return clean.ljust(11, "0")[:11]


def is_valid_ccn(ccn: str) -> bool:
    """CMS Certification Number: 6 chars (2-digit state + 4-digit facility)."""
    return bool(re.match(r"^\d{6}$", ccn))


def is_valid_fips(fips: str) -> bool:
    """FIPS county code: 5 digits (2 state + 3 county)."""
    return bool(re.match(r"^\d{5}$", fips))


# ── Property-based tests ───────────────────────────────────────────


class TestStateCode:
    def test_valid_codes(self):
        for code in ["FL", "CA", "NY", "TX", "DC", "PR", "VI", "GU"]:
            assert is_valid_state_code(code)

    def test_invalid_codes(self):
        for code in ["fl", "F", "FLA", "12", "", "F1", " FL"]:
            assert not is_valid_state_code(code)

    @given(st.text(min_size=2, max_size=2, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    def test_two_uppercase_ascii_always_valid(self, code):
        assert is_valid_state_code(code)

    @given(st.text(min_size=0, max_size=1))
    def test_short_strings_invalid(self, code):
        assert not is_valid_state_code(code)


class TestNPI:
    def test_valid_npis(self):
        assert is_valid_npi("1234567890")
        assert is_valid_npi("0000000001")

    def test_invalid_npis(self):
        assert not is_valid_npi("123456789")  # 9 digits
        assert not is_valid_npi("12345678901")  # 11 digits
        assert not is_valid_npi("123456789A")
        assert not is_valid_npi("")

    @given(st.from_regex(r"^\d{10}$", fullmatch=True))
    def test_10_digit_always_valid(self, npi):
        assert is_valid_npi(npi)


class TestCPTHCPCS:
    def test_valid_codes(self):
        for code in ["99213", "99214", "99215", "G0101", "J0129", "A0425"]:
            assert is_valid_cpt_hcpcs(code)

    def test_invalid_codes(self):
        for code in ["9921", "992130", "abcde", "", "99 13"]:
            assert not is_valid_cpt_hcpcs(code)

    @given(st.from_regex(r"^[A-Z0-9]\d{4}$", fullmatch=True))
    def test_pattern_always_valid(self, code):
        assert is_valid_cpt_hcpcs(code)


class TestNDC:
    def test_valid_ndcs(self):
        assert is_valid_ndc_11("00002032201")
        assert is_valid_ndc_11("12345678901")

    def test_normalization(self):
        assert normalize_ndc_to_11("00002-0322-01") == "00002032201"
        assert normalize_ndc_to_11("0002032201") == "00002032201"  # 10-digit padded
        assert normalize_ndc_to_11("00002032201") == "00002032201"

    @given(st.from_regex(r"^[0-9]{11}$", fullmatch=True))
    def test_11_digit_roundtrip(self, ndc):
        assert normalize_ndc_to_11(ndc) == ndc


class TestCCN:
    def test_valid_ccns(self):
        assert is_valid_ccn("100001")
        assert is_valid_ccn("010234")

    def test_invalid_ccns(self):
        assert not is_valid_ccn("10000")
        assert not is_valid_ccn("1000001")
        assert not is_valid_ccn("10000A")


class TestFIPS:
    def test_valid_fips(self):
        assert is_valid_fips("12086")  # Miami-Dade
        assert is_valid_fips("06037")  # LA County

    def test_invalid_fips(self):
        assert not is_valid_fips("1208")
        assert not is_valid_fips("120860")
        assert not is_valid_fips("1208A")
