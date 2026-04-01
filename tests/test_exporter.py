"""Comprehensive tests for the Arris Touchstone cable modem Prometheus exporter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests
from prometheus_client import CollectorRegistry, generate_latest

import arris_exporter


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

STATUS_CGI_HTML = """\
<html>
<head><title>Arris Status</title></head>
<body>

<!-- Downstream table -->
<table border="2">
<tr><th>Channel</th><th>DCID</th><th>Freq</th><th>Power</th><th>SNR</th><th>Modulation</th><th>Octets</th><th>Correcteds</th><th>Uncorrectables</th></tr>
<tr><td>Downstream 1</td><td>14</td><td>722.00 MHz</td><td>5.00 dBmV</td><td>38.98 dB</td><td>256QAM</td><td>3932404</td><td>8</td><td>0</td></tr>
<tr><td>Downstream 2</td><td>15</td><td>730.00 MHz</td><td>4.50 dBmV</td><td>37.50 dB</td><td>256QAM</td><td>5000000</td><td>120</td><td>3</td></tr>
</table>

<!-- Upstream table -->
<table border="2">
<tr><th>Channel</th><th>UCID</th><th>Freq</th><th>Power</th><th>Channel Type</th><th>Symbol Rate</th><th>Modulation</th></tr>
<tr><td>Upstream 1</td><td>1</td><td>81.80 MHz</td><td>47.25 dBmV</td><td>DOCSIS2.0 (ATDMA)</td><td>5120 kSym/s</td><td>64QAM</td></tr>
<tr><td>Upstream 2</td><td>3</td><td>36.50 MHz</td><td>44.00 dBmV</td><td>DOCSIS3.0 (ATDMA)</td><td>2560 kSym/s</td><td>32QAM</td></tr>
</table>

<!-- Interface table -->
<table border="2">
<tr><th>Interface Name</th><th>Provisioned</th><th>State</th><th>Speed</th><th>MAC Address</th></tr>
<tr><td>LAN</td><td>Enabled</td><td>Up</td><td>1000(Full)</td><td>78:23:AE:E3:47:AD</td></tr>
<tr><td>WAN</td><td>Enabled</td><td>Down</td><td>100(Half)</td><td>78:23:AE:E3:47:AE</td></tr>
</table>

<!-- Status tables -->
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>2 d:  5 h: 07  m</td></tr>
<tr><td width="160">Computers Detected:</td><td>staticCPE(0), dynamicCPE(1)</td></tr>
<tr><td width="160">CM Status:</td><td>OPERATIONAL</td></tr>
</table>

</body>
</html>
"""

CM_STATE_CGI_HTML = """\
<html>
<head><title>CM State</title></head>
<body>

<!-- DOCSIS registration steps -->
<table border="1">
<tr><td>Docsis-Downstream Scanning</td><td>Completed</td></tr>
<tr><td>Docsis-Ranging</td><td>Completed</td></tr>
<tr><td>Docsis-DHCP</td><td>Completed</td></tr>
<tr><td>Docsis-Registration</td><td>In Progress</td></tr>
</table>

<!-- TOD -->
<table border="1">
<tr><td>Time of Day</td><td>Retrieved</td></tr>
</table>

<!-- BPI -->
<table border="1">
<tr><td>BPI Status</td><td>Enabled, Authorized</td></tr>
</table>

<!-- DHCP Attempts -->
<table border="1">
<tr><td>IPv4 Attempt(s)</td><td>1</td></tr>
<tr><td>IPv6 Attempt(s)</td><td>3</td></tr>
</table>

</body>
</html>
"""

# Status page with CM Status offline
STATUS_CGI_OFFLINE_HTML = """\
<html><body>
<table border="2"></table>
<table border="2"></table>
<table border="2"></table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>0 d:  0 h: 02  m</td></tr>
</table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">CM Status:</td><td>OFFLINE</td></tr>
</table>
</body></html>
"""

# Status page with unknown CM Status
STATUS_CGI_OTHER_HTML = """\
<html><body>
<table border="2"></table>
<table border="2"></table>
<table border="2"></table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>0 d:  0 h: 00  m</td></tr>
</table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">CM Status:</td><td>INITIALIZING</td></tr>
</table>
</body></html>
"""

# CM State page where TOD is not retrieved, BPI not authorized
CM_STATE_CGI_INCOMPLETE_HTML = """\
<html><body>
<table border="1">
<tr><td>Docsis-Downstream Scanning</td><td>Completed</td></tr>
<tr><td>Docsis-Ranging</td><td>In Progress</td></tr>
</table>
<table border="1">
<tr><td>Time of Day</td><td>Not Retrieved</td></tr>
</table>
<table border="1">
<tr><td>BPI Status</td><td>Disabled</td></tr>
</table>
<table border="1">
<tr><td>IPv4 Attempt(s)</td><td>5</td></tr>
<tr><td>IPv6 Attempt(s)</td><td>0</td></tr>
</table>
</body></html>
"""

EVENT_CGI_HTML = """\
<html>
<head><title>Touchstone Event Log</title></head>
<body>
<table border=1 cellpadding="1" cols="4" width="770">
<tbody>
<tr>
  <td width="10%" valign=top align=center><b>Date Time</b></td>
  <td width="10%" valign=top align=center><b>Event ID</b></td>
  <td width="5%"  valign=top align=center><b>Event Level</b></td>
  <td width="45%" valign=top align=center><b>Description</b></td>
</tr>
<tr>
    <td align=center>4/1/2026 13:15</td>
    <td align=center>84020200</td>
    <td align=center>5</td>
    <td align=left>Lost MDD Timeout;CM-MAC=78:23:ae:e3:47:ae;</td>
</tr><tr>
    <td align=center>4/1/2026 13:15</td>
    <td align=center>84000700</td>
    <td align=center>5</td>
    <td align=left>RCS Partial Service;CM-MAC=78:23:ae:e3:47:ae;</td>
</tr><tr>
    <td align=center>4/1/2026 13:16</td>
    <td align=center>84020200</td>
    <td align=center>3</td>
    <td align=left>Lost MDD Timeout;CM-MAC=78:23:ae:e3:47:ae;</td>
</tr><tr>
    <td align=center>4/1/2026 13:16</td>
    <td align=center>82000200</td>
    <td align=center>6</td>
    <td align=left>SYNC Timing Synchronization failure;</td>
</tr>
</tbody>
</table>
</body>
</html>
"""

EVENT_CGI_EMPTY_HTML = """\
<html>
<head><title>Touchstone Event Log</title></head>
<body>
<table border=1 cellpadding="1" cols="4" width="770">
<tbody>
<tr>
  <td width="10%" valign=top align=center><b>Date Time</b></td>
  <td width="10%" valign=top align=center><b>Event ID</b></td>
  <td width="5%"  valign=top align=center><b>Event Level</b></td>
  <td width="45%" valign=top align=center><b>Description</b></td>
</tr>
</tbody>
</table>
</body>
</html>
"""

VERS_CGI_HTML = """\
<html>
<head><title>Touchstone HW/FW Versions</title></head>
<body>
<table cellpadding="0" cellspacing="0">
<tbody>
<tr valign="top">
             <td width="170">System: </td>
             <td width="600">ARRIS EuroDOCSIS 3.0 Touchstone WideBand Cable Modem<br>
             HW_REV: 1<br>
             VENDOR: ARRIS Group, Inc.<br>
             BOOTR: 2.2.0.45<br>
             SW_REV: 9.1.103DE3<br>
             MODEL: CM3200B-85</td>
</tr>
<tr>
             <td>Serial Number:</td>
             <td>7682D4111125294</td>
</tr>
</tbody>
</table>
<table cellpadding="0" cellspacing="0">
<tbody><tr><td width="160">Firmware Name:</td><td>TS0901103DE3_061119_1602.TM</td></tr>
<tr><td>Firmware Build Time:   </td><td>Tue Jun 11 20:52:12 EDT 2019</td></tr>
</tbody></table>
</body>
</html>
"""

# Status with Computers Detected showing higher counts
STATUS_CGI_WITH_COMPUTERS_HTML = """\
<html><body>
<table border="2"></table>
<table border="2"></table>
<table border="2"></table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>1 d:  0 h: 00  m</td></tr>
<tr><td width="160">Computers Detected:</td><td>staticCPE(2), dynamicCPE(5)</td></tr>
<tr><td width="160">CM Status:</td><td>OPERATIONAL</td></tr>
</table>
</body></html>
"""

# Status with CABLE interface that has unparseable speed ("-----")
STATUS_CGI_CABLE_IFACE_HTML = """\
<html><body>
<table border="2"></table>
<table border="2"></table>
<table border="2">
<tr><td>Interface Name</td><td>Provisioned</td><td>State</td><td>Speed (Mbps)</td><td>MAC address</td></tr>
<tr><td>LAN</td><td>Enabled</td><td>Up</td><td>1000(Full)</td><td>78:23:AE:E3:47:AD</td></tr>
<tr><td>CABLE</td><td>Enabled</td><td>Up</td><td>-----</td><td>78:23:AE:E3:47:AE</td></tr>
</table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>0 d:  1 h: 00  m</td></tr>
<tr><td width="160">CM Status:</td><td>OPERATIONAL</td></tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Helpers to build mock responses
# ---------------------------------------------------------------------------

def _mock_response(html: str, status_code: int = 200) -> MagicMock:
    """Create a mock requests.Response with the given HTML body."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = html
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=resp,
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def _get_sample_value(registry: CollectorRegistry, metric_name: str, labels: dict | None = None) -> float | None:
    """Extract a single sample value from the registry.

    Searches through all metrics/samples in *registry* for one whose name
    matches *metric_name* and whose labels match *labels* (if provided).
    Returns the value, or None if not found.
    """
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name == metric_name:
                if labels is None or sample.labels == labels:
                    return sample.value
    return None


def _get_all_samples(registry: CollectorRegistry, metric_name: str) -> list[tuple[dict, float]]:
    """Return all (labels, value) pairs for the given metric name."""
    results = []
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name == metric_name:
                results.append((sample.labels, sample.value))
    return results


def _full_side_effect(
    status_html: str = STATUS_CGI_HTML,
    cm_state_html: str = CM_STATE_CGI_HTML,
    event_html: str = EVENT_CGI_HTML,
    vers_html: str = VERS_CGI_HTML,
):
    """Return a side_effect function that routes modem URLs to the right fixture."""
    def side_effect(url, **kwargs):
        if "status_cgi" in url:
            return _mock_response(status_html)
        elif "cm_state_cgi" in url:
            return _mock_response(cm_state_html)
        elif "event_cgi" in url:
            return _mock_response(event_html)
        elif "vers_cgi" in url:
            return _mock_response(vers_html)
        return _mock_response("", status_code=404)
    return side_effect


# ---------------------------------------------------------------------------
# Fixture: fresh registry for every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _fresh_registry():
    """Replace the module-level registry before each test and restore it after.

    This prevents state from leaking between tests.  We swap in a brand-new
    CollectorRegistry and re-create every Gauge/Enum so each test starts
    clean.
    """
    from prometheus_client import Enum, Gauge

    old_registry = arris_exporter.registry
    new_registry = CollectorRegistry()
    arris_exporter.registry = new_registry

    # Re-create all metric objects on the new registry
    arris_exporter.ds_power = Gauge(
        "arris_downstream_power_dbmv",
        "Downstream channel power level in dBmV",
        ["channel", "dcid", "frequency_mhz", "modulation"],
        registry=new_registry,
    )
    arris_exporter.ds_snr = Gauge(
        "arris_downstream_snr_db",
        "Downstream channel SNR in dB",
        ["channel", "dcid", "frequency_mhz", "modulation"],
        registry=new_registry,
    )
    arris_exporter.ds_octets = Gauge(
        "arris_downstream_octets_total",
        "Downstream octets received (resets on modem reboot)",
        ["channel", "dcid", "frequency_mhz"],
        registry=new_registry,
    )
    arris_exporter.ds_correcteds = Gauge(
        "arris_downstream_correcteds_total",
        "Downstream FEC corrected codewords",
        ["channel", "dcid", "frequency_mhz"],
        registry=new_registry,
    )
    arris_exporter.ds_uncorrectables = Gauge(
        "arris_downstream_uncorrectables_total",
        "Downstream FEC uncorrectable codewords",
        ["channel", "dcid", "frequency_mhz"],
        registry=new_registry,
    )
    arris_exporter.ds_frequency = Gauge(
        "arris_downstream_frequency_hz",
        "Downstream channel frequency in Hz",
        ["channel", "dcid"],
        registry=new_registry,
    )

    arris_exporter.us_power = Gauge(
        "arris_upstream_power_dbmv",
        "Upstream channel power level in dBmV",
        ["channel", "ucid", "frequency_mhz", "modulation", "channel_type"],
        registry=new_registry,
    )
    arris_exporter.us_frequency = Gauge(
        "arris_upstream_frequency_hz",
        "Upstream channel frequency in Hz",
        ["channel", "ucid"],
        registry=new_registry,
    )
    arris_exporter.us_symbol_rate = Gauge(
        "arris_upstream_symbol_rate_ksps",
        "Upstream symbol rate in kSym/s",
        ["channel", "ucid"],
        registry=new_registry,
    )

    arris_exporter.uptime_seconds = Gauge(
        "arris_uptime_seconds",
        "Modem uptime in seconds",
        registry=new_registry,
    )
    arris_exporter.cm_status = Enum(
        "arris_cm_status",
        "Cable modem operational status",
        states=["operational", "offline", "other"],
        registry=new_registry,
    )

    arris_exporter.iface_up = Gauge(
        "arris_interface_up",
        "Interface state (1 = Up, 0 = Down)",
        ["interface", "mac_address"],
        registry=new_registry,
    )
    arris_exporter.iface_speed = Gauge(
        "arris_interface_speed_mbps",
        "Interface speed in Mbps",
        ["interface", "mac_address"],
        registry=new_registry,
    )

    arris_exporter.docsis_step_completed = Gauge(
        "arris_docsis_step_completed",
        "DOCSIS registration step status (1 = Completed, 0 = not)",
        ["step"],
        registry=new_registry,
    )
    arris_exporter.tod_retrieved = Gauge(
        "arris_tod_retrieved",
        "Time of Day retrieved (1 = yes, 0 = no)",
        registry=new_registry,
    )
    arris_exporter.bpi_authorized = Gauge(
        "arris_bpi_authorized",
        "BPI authorized (1 = yes, 0 = no)",
        registry=new_registry,
    )
    arris_exporter.dhcp_attempts_ipv4 = Gauge(
        "arris_dhcp_attempts_ipv4",
        "DHCP IPv4 attempts to obtain CM IP address",
        registry=new_registry,
    )
    arris_exporter.dhcp_attempts_ipv6 = Gauge(
        "arris_dhcp_attempts_ipv6",
        "DHCP IPv6 attempts to obtain CM IP address",
        registry=new_registry,
    )

    arris_exporter.scrape_success = Gauge(
        "arris_scrape_success",
        "Whether the last scrape was successful (1 = yes, 0 = no)",
        registry=new_registry,
    )
    arris_exporter.scrape_duration = Gauge(
        "arris_scrape_duration_seconds",
        "Duration of the last scrape in seconds",
        registry=new_registry,
    )

    arris_exporter.event_log_total = Gauge(
        "arris_event_log_total",
        "Total number of events in the event log",
        registry=new_registry,
    )
    arris_exporter.event_log_by_level = Gauge(
        "arris_event_log_by_level_total",
        "Number of events in the event log by severity level",
        ["level"],
        registry=new_registry,
    )
    arris_exporter.modem_info = Gauge(
        "arris_modem_info",
        "Modem hardware and firmware information (always 1)",
        ["model", "serial_number", "hw_rev", "sw_rev", "firmware_name", "firmware_build_time"],
        registry=new_registry,
    )
    arris_exporter.computers_detected = Gauge(
        "arris_computers_detected",
        "Number of computers detected by the modem",
        ["type"],
        registry=new_registry,
    )

    yield new_registry

    # Restore original registry (not strictly necessary since every test gets
    # a fresh one, but keeps module state tidy).
    arris_exporter.registry = old_registry


# ===========================================================================
# Unit tests: helper functions
# ===========================================================================

class TestParseFloat:
    def test_dbmv(self):
        assert arris_exporter.parse_float("5.00 dBmV") == 5.0

    def test_mhz(self):
        assert arris_exporter.parse_float("722.00 MHz") == 722.0

    def test_negative(self):
        assert arris_exporter.parse_float("-3.50 dBmV") == -3.5

    def test_positive_sign(self):
        assert arris_exporter.parse_float("+12.3 dB") == 12.3

    def test_integer(self):
        assert arris_exporter.parse_float("5120 kSym/s") == 5120.0

    def test_no_number_raises(self):
        with pytest.raises(ValueError, match="Cannot parse number"):
            arris_exporter.parse_float("no digits here")

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Cannot parse number"):
            arris_exporter.parse_float("")

    def test_speed_with_parentheses(self):
        assert arris_exporter.parse_float("1000(Full)") == 1000.0

    def test_speed_100(self):
        assert arris_exporter.parse_float("100(Half)") == 100.0


class TestParseInt:
    def test_plain(self):
        assert arris_exporter.parse_int("3932404") == 3932404

    def test_with_comma(self):
        assert arris_exporter.parse_int("1,234,567") == 1234567

    def test_no_number_raises(self):
        with pytest.raises(ValueError, match="Cannot parse int"):
            arris_exporter.parse_int("abc")

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Cannot parse int"):
            arris_exporter.parse_int("")


class TestParseUptime:
    def test_normal(self):
        # 2 days, 5 hours, 7 minutes
        result = arris_exporter.parse_uptime("2 d:  5 h: 07  m")
        assert result == 2 * 86400 + 5 * 3600 + 7 * 60

    def test_zero(self):
        result = arris_exporter.parse_uptime("0 d:  0 h: 00  m")
        assert result == 0

    def test_large(self):
        result = arris_exporter.parse_uptime("100 d: 23 h: 59  m")
        assert result == 100 * 86400 + 23 * 3600 + 59 * 60

    def test_no_match(self):
        assert arris_exporter.parse_uptime("no match") == 0

    def test_empty(self):
        assert arris_exporter.parse_uptime("") == 0


# ===========================================================================
# Unit tests: fetch_page
# ===========================================================================

class TestFetchPage:
    @patch("arris_exporter.requests.get")
    def test_success(self, mock_get):
        mock_get.return_value = _mock_response("<html><body>ok</body></html>")
        soup = arris_exporter.fetch_page("http://example.com/page")
        assert soup is not None
        assert soup.find("body").get_text() == "ok"
        mock_get.assert_called_once_with("http://example.com/page", timeout=10)

    @patch("arris_exporter.requests.get")
    def test_connection_error_returns_none(self, mock_get):
        mock_get.side_effect = requests.ConnectionError("Connection refused")
        result = arris_exporter.fetch_page("http://192.168.100.1/cgi-bin/status_cgi")
        assert result is None

    @patch("arris_exporter.requests.get")
    def test_timeout_returns_none(self, mock_get):
        mock_get.side_effect = requests.Timeout("Timed out")
        result = arris_exporter.fetch_page("http://192.168.100.1/cgi-bin/status_cgi")
        assert result is None

    @patch("arris_exporter.requests.get")
    def test_http_error_returns_none(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        result = arris_exporter.fetch_page("http://192.168.100.1/cgi-bin/status_cgi")
        assert result is None


# ===========================================================================
# Integration tests: scrape() -- full status page
# ===========================================================================

class TestScrapeDownstream:
    """Verify downstream channel metrics are correctly parsed from status_cgi."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_ds_power_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_power_dbmv",
            {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0", "modulation": "256QAM"},
        )
        assert val == 5.0

    def test_ds_power_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_power_dbmv",
            {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0", "modulation": "256QAM"},
        )
        assert val == 4.5

    def test_ds_snr_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_snr_db",
            {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0", "modulation": "256QAM"},
        )
        assert val == 38.98

    def test_ds_snr_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_snr_db",
            {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0", "modulation": "256QAM"},
        )
        assert val == 37.5

    def test_ds_octets_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_octets_total",
            {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0"},
        )
        assert val == 3932404.0

    def test_ds_octets_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_octets_total",
            {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0"},
        )
        assert val == 5000000.0

    def test_ds_correcteds(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_correcteds_total",
            {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0"},
        )
        assert val == 8.0

    def test_ds_correcteds_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_correcteds_total",
            {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0"},
        )
        assert val == 120.0

    def test_ds_uncorrectables(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_uncorrectables_total",
            {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0"},
        )
        assert val == 0.0

    def test_ds_uncorrectables_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_uncorrectables_total",
            {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0"},
        )
        assert val == 3.0

    def test_ds_frequency_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_frequency_hz",
            {"channel": "Downstream 1", "dcid": "14"},
        )
        assert val == 722_000_000.0

    def test_ds_frequency_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_downstream_frequency_hz",
            {"channel": "Downstream 2", "dcid": "15"},
        )
        assert val == 730_000_000.0

    def test_two_downstream_channels_present(self):
        samples = _get_all_samples(self.registry, "arris_downstream_power_dbmv")
        assert len(samples) == 2


class TestScrapeUpstream:
    """Verify upstream channel metrics are correctly parsed from status_cgi."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_us_power_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_upstream_power_dbmv",
            {
                "channel": "Upstream 1",
                "ucid": "1",
                "frequency_mhz": "81.8",
                "modulation": "64QAM",
                "channel_type": "DOCSIS2.0 (ATDMA)",
            },
        )
        assert val == 47.25

    def test_us_power_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_upstream_power_dbmv",
            {
                "channel": "Upstream 2",
                "ucid": "3",
                "frequency_mhz": "36.5",
                "modulation": "32QAM",
                "channel_type": "DOCSIS3.0 (ATDMA)",
            },
        )
        assert val == 44.0

    def test_us_frequency_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_upstream_frequency_hz",
            {"channel": "Upstream 1", "ucid": "1"},
        )
        assert val == 81_800_000.0

    def test_us_frequency_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_upstream_frequency_hz",
            {"channel": "Upstream 2", "ucid": "3"},
        )
        assert val == 36_500_000.0

    def test_us_symbol_rate_channel_1(self):
        val = _get_sample_value(
            self.registry,
            "arris_upstream_symbol_rate_ksps",
            {"channel": "Upstream 1", "ucid": "1"},
        )
        assert val == 5120.0

    def test_us_symbol_rate_channel_2(self):
        val = _get_sample_value(
            self.registry,
            "arris_upstream_symbol_rate_ksps",
            {"channel": "Upstream 2", "ucid": "3"},
        )
        assert val == 2560.0

    def test_two_upstream_channels_present(self):
        samples = _get_all_samples(self.registry, "arris_upstream_power_dbmv")
        assert len(samples) == 2


class TestScrapeModemStatus:
    """Verify modem uptime and CM status parsing."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_uptime(self):
        val = _get_sample_value(self.registry, "arris_uptime_seconds")
        expected = 2 * 86400 + 5 * 3600 + 7 * 60  # 190020
        assert val == expected

    def test_cm_status_operational(self):
        # Enum metric exposes one sample per state; the active state has value 1.0
        val = _get_sample_value(
            self.registry,
            "arris_cm_status",
            {"arris_cm_status": "operational"},
        )
        assert val == 1.0

    def test_cm_status_offline_is_zero(self):
        val = _get_sample_value(
            self.registry,
            "arris_cm_status",
            {"arris_cm_status": "offline"},
        )
        assert val == 0.0

    def test_scrape_success_is_one(self):
        val = _get_sample_value(self.registry, "arris_scrape_success")
        assert val == 1.0

    def test_scrape_duration_is_positive(self):
        val = _get_sample_value(self.registry, "arris_scrape_duration_seconds")
        assert val is not None
        assert val >= 0.0


class TestScrapeModemStatusOffline:
    """Test CM Status parsing when modem reports OFFLINE."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect(status_html=STATUS_CGI_OFFLINE_HTML)
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_cm_status_offline(self):
        val = _get_sample_value(
            self.registry,
            "arris_cm_status",
            {"arris_cm_status": "offline"},
        )
        assert val == 1.0

    def test_cm_status_operational_is_zero(self):
        val = _get_sample_value(
            self.registry,
            "arris_cm_status",
            {"arris_cm_status": "operational"},
        )
        assert val == 0.0

    def test_uptime_is_120_seconds(self):
        val = _get_sample_value(self.registry, "arris_uptime_seconds")
        assert val == 2 * 60


class TestScrapeModemStatusOther:
    """Test CM Status parsing when modem reports an unknown status."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect(status_html=STATUS_CGI_OTHER_HTML)
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_cm_status_other(self):
        val = _get_sample_value(
            self.registry,
            "arris_cm_status",
            {"arris_cm_status": "other"},
        )
        assert val == 1.0


class TestScrapeInterfaces:
    """Verify interface metrics (state, speed, MAC) are parsed correctly."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_lan_interface_up(self):
        val = _get_sample_value(
            self.registry,
            "arris_interface_up",
            {"interface": "LAN", "mac_address": "78:23:AE:E3:47:AD"},
        )
        assert val == 1.0

    def test_wan_interface_down(self):
        val = _get_sample_value(
            self.registry,
            "arris_interface_up",
            {"interface": "WAN", "mac_address": "78:23:AE:E3:47:AE"},
        )
        assert val == 0.0

    def test_lan_speed(self):
        val = _get_sample_value(
            self.registry,
            "arris_interface_speed_mbps",
            {"interface": "LAN", "mac_address": "78:23:AE:E3:47:AD"},
        )
        assert val == 1000.0

    def test_wan_speed(self):
        val = _get_sample_value(
            self.registry,
            "arris_interface_speed_mbps",
            {"interface": "WAN", "mac_address": "78:23:AE:E3:47:AE"},
        )
        assert val == 100.0

    def test_two_interfaces_present(self):
        samples = _get_all_samples(self.registry, "arris_interface_up")
        assert len(samples) == 2


# ===========================================================================
# Integration tests: scrape_cm_state()
# ===========================================================================

class TestScrapeCmState:
    """Verify CM State (DOCSIS registration) metrics are parsed correctly."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response(CM_STATE_CGI_HTML)
            result = arris_exporter.scrape_cm_state("http://modem/cgi-bin")
            self.scrape_result = result

    def test_returns_true(self):
        assert self.scrape_result is True

    def test_downstream_scanning_completed(self):
        val = _get_sample_value(
            self.registry,
            "arris_docsis_step_completed",
            {"step": "Docsis-Downstream Scanning"},
        )
        assert val == 1.0

    def test_ranging_completed(self):
        val = _get_sample_value(
            self.registry,
            "arris_docsis_step_completed",
            {"step": "Docsis-Ranging"},
        )
        assert val == 1.0

    def test_dhcp_completed(self):
        val = _get_sample_value(
            self.registry,
            "arris_docsis_step_completed",
            {"step": "Docsis-DHCP"},
        )
        assert val == 1.0

    def test_registration_in_progress(self):
        val = _get_sample_value(
            self.registry,
            "arris_docsis_step_completed",
            {"step": "Docsis-Registration"},
        )
        assert val == 0.0

    def test_tod_retrieved(self):
        val = _get_sample_value(self.registry, "arris_tod_retrieved")
        assert val == 1.0

    def test_bpi_authorized(self):
        val = _get_sample_value(self.registry, "arris_bpi_authorized")
        assert val == 1.0

    def test_dhcp_ipv4_attempts(self):
        val = _get_sample_value(self.registry, "arris_dhcp_attempts_ipv4")
        assert val == 1.0

    def test_dhcp_ipv6_attempts(self):
        val = _get_sample_value(self.registry, "arris_dhcp_attempts_ipv6")
        assert val == 3.0

    def test_four_docsis_steps(self):
        samples = _get_all_samples(self.registry, "arris_docsis_step_completed")
        assert len(samples) == 4


class TestScrapeCmStateIncomplete:
    """Verify CM State parsing when TOD is not retrieved and BPI is not authorized."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response(CM_STATE_CGI_INCOMPLETE_HTML)
            arris_exporter.scrape_cm_state("http://modem/cgi-bin")

    def test_downstream_scanning_completed(self):
        val = _get_sample_value(
            self.registry,
            "arris_docsis_step_completed",
            {"step": "Docsis-Downstream Scanning"},
        )
        assert val == 1.0

    def test_ranging_not_completed(self):
        val = _get_sample_value(
            self.registry,
            "arris_docsis_step_completed",
            {"step": "Docsis-Ranging"},
        )
        assert val == 0.0

    def test_tod_not_retrieved(self):
        val = _get_sample_value(self.registry, "arris_tod_retrieved")
        assert val == 0.0

    def test_bpi_not_authorized(self):
        val = _get_sample_value(self.registry, "arris_bpi_authorized")
        assert val == 0.0

    def test_dhcp_ipv4_attempts(self):
        val = _get_sample_value(self.registry, "arris_dhcp_attempts_ipv4")
        assert val == 5.0

    def test_dhcp_ipv6_attempts(self):
        val = _get_sample_value(self.registry, "arris_dhcp_attempts_ipv6")
        assert val == 0.0


class TestScrapeCmStateFailure:
    """Verify scrape_cm_state returns False when the page cannot be fetched."""

    def test_connection_failure(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection refused")
            result = arris_exporter.scrape_cm_state("http://modem/cgi-bin")
            assert result is False


# ===========================================================================
# Error handling: scrape() with failures
# ===========================================================================

class TestScrapeFailure:
    """Verify scrape() handles errors gracefully."""

    def test_connection_failure_sets_scrape_success_zero(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection refused")
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_scrape_success")
        assert val == 0.0

    def test_connection_failure_still_records_duration(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection refused")
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_scrape_duration_seconds")
        assert val is not None
        assert val >= 0.0

    def test_http_500_sets_scrape_success_zero(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response("", status_code=500)
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_scrape_success")
        assert val == 0.0

    def test_timeout_sets_scrape_success_zero(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = requests.Timeout("Timed out")
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_scrape_success")
        assert val == 0.0

    def test_cm_state_failure_still_succeeds_status(self, _fresh_registry):
        """If status_cgi succeeds but cm_state_cgi fails, scrape_success is still 1."""
        with patch("arris_exporter.requests.get") as mock_get:
            def side_effect(url, **kwargs):
                if "status_cgi" in url:
                    return _mock_response(STATUS_CGI_HTML)
                elif "cm_state_cgi" in url:
                    raise requests.ConnectionError("CM state unavailable")
                elif "event_cgi" in url:
                    return _mock_response(EVENT_CGI_HTML)
                elif "vers_cgi" in url:
                    return _mock_response(VERS_CGI_HTML)
                return _mock_response("", status_code=404)
            mock_get.side_effect = side_effect
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_scrape_success")
        assert val == 1.0


# ===========================================================================
# Edge cases
# ===========================================================================

class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_html_does_not_crash(self, _fresh_registry):
        """Scraping an empty HTML page should not raise."""
        with patch("arris_exporter.requests.get") as mock_get:
            def side_effect(url, **kwargs):
                return _mock_response("<html><body></body></html>")
            mock_get.side_effect = side_effect
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_scrape_success")
        assert val == 1.0

    def test_no_downstream_channels(self, _fresh_registry):
        """If no downstream rows match, no downstream metrics are set."""
        html = """\
        <html><body>
        <table border="2"><tr><th>Header</th></tr></table>
        <table border="2"><tr><th>Header</th></tr></table>
        <table border="2"><tr><th>Header</th></tr></table>
        <table cellpadding="0" cellspacing="0">
        <tr><td width="160">System Uptime: </td><td>0 d:  0 h: 01  m</td></tr>
        </table>
        <table cellpadding="0" cellspacing="0">
        <tr><td width="160">CM Status:</td><td>OPERATIONAL</td></tr>
        </table>
        </body></html>
        """
        with patch("arris_exporter.requests.get") as mock_get:
            def side_effect(url, **kwargs):
                return _mock_response(html)
            mock_get.side_effect = side_effect
            arris_exporter.scrape("http://modem/cgi-bin")

        samples = _get_all_samples(_fresh_registry, "arris_downstream_power_dbmv")
        assert len(samples) == 0

    def test_generate_latest_produces_valid_output(self, _fresh_registry):
        """The Prometheus text exposition format should be valid after scrape."""
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

        output = generate_latest(_fresh_registry).decode("utf-8")
        # Should contain HELP and TYPE lines for key metrics
        assert "# HELP arris_downstream_power_dbmv" in output
        assert "# HELP arris_upstream_power_dbmv" in output
        assert "# HELP arris_uptime_seconds" in output
        assert "# HELP arris_scrape_success" in output
        assert "# HELP arris_event_log_total" in output
        assert "# HELP arris_modem_info" in output
        # Should contain actual values
        assert "arris_downstream_power_dbmv{" in output
        assert "arris_upstream_power_dbmv{" in output
        assert "arris_event_log_total " in output
        assert "arris_modem_info{" in output

    def test_interface_header_row_skipped(self, _fresh_registry):
        """The 'Interface Name' header row in the interface table should be skipped."""
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

        # Only LAN and WAN should be present, not "Interface Name"
        samples = _get_all_samples(_fresh_registry, "arris_interface_up")
        interfaces = [labels["interface"] for labels, _ in samples]
        assert "Interface Name" not in interfaces
        assert "LAN" in interfaces
        assert "WAN" in interfaces

    def test_cable_interface_unparseable_speed_falls_back_to_zero(self, _fresh_registry):
        """CABLE interface with '-----' speed should get speed 0."""
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect(status_html=STATUS_CGI_CABLE_IFACE_HTML)
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(
            _fresh_registry,
            "arris_interface_speed_mbps",
            {"interface": "CABLE", "mac_address": "78:23:AE:E3:47:AE"},
        )
        assert val == 0.0

        val_up = _get_sample_value(
            _fresh_registry,
            "arris_interface_up",
            {"interface": "CABLE", "mac_address": "78:23:AE:E3:47:AE"},
        )
        assert val_up == 1.0


# ===========================================================================
# Integration tests: scrape_event_log()
# ===========================================================================

class TestScrapeEventLog:
    """Verify event log metrics are correctly parsed from event_cgi."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response(EVENT_CGI_HTML)
            self.result = arris_exporter.scrape_event_log("http://modem/cgi-bin")

    def test_returns_true(self):
        assert self.result is True

    def test_total_events(self):
        val = _get_sample_value(self.registry, "arris_event_log_total")
        assert val == 4.0

    def test_level_5_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_event_log_by_level_total",
            {"level": "5"},
        )
        assert val == 2.0

    def test_level_3_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_event_log_by_level_total",
            {"level": "3"},
        )
        assert val == 1.0

    def test_level_6_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_event_log_by_level_total",
            {"level": "6"},
        )
        assert val == 1.0

    def test_three_distinct_levels(self):
        samples = _get_all_samples(self.registry, "arris_event_log_by_level_total")
        assert len(samples) == 3


class TestScrapeEventLogEmpty:
    """Verify event log parsing when there are no events (only header row)."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response(EVENT_CGI_EMPTY_HTML)
            self.result = arris_exporter.scrape_event_log("http://modem/cgi-bin")

    def test_returns_true(self):
        assert self.result is True

    def test_total_events_is_zero(self):
        val = _get_sample_value(self.registry, "arris_event_log_total")
        assert val == 0.0

    def test_no_level_samples(self):
        samples = _get_all_samples(self.registry, "arris_event_log_by_level_total")
        assert len(samples) == 0


class TestScrapeEventLogFailure:
    """Verify scrape_event_log returns False on connection failure."""

    def test_connection_failure(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection refused")
            result = arris_exporter.scrape_event_log("http://modem/cgi-bin")
            assert result is False


class TestScrapeEventLogNoTable:
    """Verify scrape_event_log handles a page with no event table."""

    def test_no_table_returns_true(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response("<html><body></body></html>")
            result = arris_exporter.scrape_event_log("http://modem/cgi-bin")
            assert result is True


# ===========================================================================
# Integration tests: scrape_vers()
# ===========================================================================

class TestScrapeVers:
    """Verify HW/FW version info metrics are correctly parsed from vers_cgi."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response(VERS_CGI_HTML)
            self.result = arris_exporter.scrape_vers("http://modem/cgi-bin")

    def test_returns_true(self):
        assert self.result is True

    def test_modem_info_value_is_one(self):
        val = _get_sample_value(
            self.registry,
            "arris_modem_info",
            {
                "model": "CM3200B-85",
                "serial_number": "7682D4111125294",
                "hw_rev": "1",
                "sw_rev": "9.1.103DE3",
                "firmware_name": "TS0901103DE3_061119_1602.TM",
                "firmware_build_time": "Tue Jun 11 20:52:12 EDT 2019",
            },
        )
        assert val == 1.0

    def test_exactly_one_info_sample(self):
        samples = _get_all_samples(self.registry, "arris_modem_info")
        assert len(samples) == 1

    def test_model_label(self):
        samples = _get_all_samples(self.registry, "arris_modem_info")
        assert samples[0][0]["model"] == "CM3200B-85"

    def test_serial_number_label(self):
        samples = _get_all_samples(self.registry, "arris_modem_info")
        assert samples[0][0]["serial_number"] == "7682D4111125294"

    def test_sw_rev_label(self):
        samples = _get_all_samples(self.registry, "arris_modem_info")
        assert samples[0][0]["sw_rev"] == "9.1.103DE3"

    def test_firmware_name_label(self):
        samples = _get_all_samples(self.registry, "arris_modem_info")
        assert samples[0][0]["firmware_name"] == "TS0901103DE3_061119_1602.TM"


class TestScrapeVersFailure:
    """Verify scrape_vers returns False on connection failure."""

    def test_connection_failure(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection refused")
            result = arris_exporter.scrape_vers("http://modem/cgi-bin")
            assert result is False


class TestScrapeVersMinimalPage:
    """Verify scrape_vers handles a page with no parseable info gracefully."""

    def test_empty_page(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.return_value = _mock_response("<html><body></body></html>")
            result = arris_exporter.scrape_vers("http://modem/cgi-bin")
            assert result is True

        # Info metric should still be set (with empty labels)
        samples = _get_all_samples(_fresh_registry, "arris_modem_info")
        assert len(samples) == 1
        assert samples[0][1] == 1.0
        assert samples[0][0]["model"] == ""


# ===========================================================================
# Integration tests: Computers Detected
# ===========================================================================

class TestScrapeComputersDetected:
    """Verify computers detected metrics from the status page."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_static_cpe_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_computers_detected",
            {"type": "static"},
        )
        assert val == 0.0

    def test_dynamic_cpe_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_computers_detected",
            {"type": "dynamic"},
        )
        assert val == 1.0

    def test_two_types_present(self):
        samples = _get_all_samples(self.registry, "arris_computers_detected")
        assert len(samples) == 2


class TestScrapeComputersDetectedHigherCounts:
    """Verify computers detected with higher counts."""

    @pytest.fixture(autouse=True)
    def _run_scrape(self, _fresh_registry):
        self.registry = _fresh_registry
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect(
                status_html=STATUS_CGI_WITH_COMPUTERS_HTML,
            )
            arris_exporter.scrape("http://modem/cgi-bin")

    def test_static_cpe_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_computers_detected",
            {"type": "static"},
        )
        assert val == 2.0

    def test_dynamic_cpe_count(self):
        val = _get_sample_value(
            self.registry,
            "arris_computers_detected",
            {"type": "dynamic"},
        )
        assert val == 5.0


# ===========================================================================
# Integration test: full scrape calls all sub-scrapers
# ===========================================================================

class TestScrapeCallsAllSubscrapers:
    """Verify that scrape() calls event_cgi and vers_cgi in addition to the others."""

    def test_all_urls_fetched(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

        called_urls = [call.args[0] for call in mock_get.call_args_list]
        assert any("status_cgi" in u for u in called_urls)
        assert any("cm_state_cgi" in u for u in called_urls)
        assert any("event_cgi" in u for u in called_urls)
        assert any("vers_cgi" in u for u in called_urls)

    def test_event_metrics_populated_via_full_scrape(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

        val = _get_sample_value(_fresh_registry, "arris_event_log_total")
        assert val == 4.0

    def test_vers_metrics_populated_via_full_scrape(self, _fresh_registry):
        with patch("arris_exporter.requests.get") as mock_get:
            mock_get.side_effect = _full_side_effect()
            arris_exporter.scrape("http://modem/cgi-bin")

        samples = _get_all_samples(_fresh_registry, "arris_modem_info")
        assert len(samples) == 1
        assert samples[0][0]["model"] == "CM3200B-85"
