"""Tests for the Arris Touchstone cable modem Prometheus exporter."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from arris_exporter import ArrisCollector, __version__, parse_float, parse_int, parse_uptime


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

STATUS_CGI_HTML = """\
<html><body>
<table border="2">
<tr><th>Channel</th><th>DCID</th><th>Freq</th><th>Power</th><th>SNR</th><th>Modulation</th><th>Octets</th><th>Correcteds</th><th>Uncorrectables</th></tr>
<tr><td>Downstream 1</td><td>14</td><td>722.00 MHz</td><td>5.00 dBmV</td><td>38.98 dB</td><td>256QAM</td><td>3932404</td><td>8</td><td>0</td></tr>
<tr><td>Downstream 2</td><td>15</td><td>730.00 MHz</td><td>4.50 dBmV</td><td>37.50 dB</td><td>256QAM</td><td>5000000</td><td>120</td><td>3</td></tr>
</table>
<table border="2">
<tr><th>Channel</th><th>UCID</th><th>Freq</th><th>Power</th><th>Channel Type</th><th>Symbol Rate</th><th>Modulation</th></tr>
<tr><td>Upstream 1</td><td>1</td><td>81.80 MHz</td><td>47.25 dBmV</td><td>DOCSIS2.0 (ATDMA)</td><td>5120 kSym/s</td><td>64QAM</td></tr>
<tr><td>Upstream 2</td><td>3</td><td>36.50 MHz</td><td>44.00 dBmV</td><td>DOCSIS3.0 (ATDMA)</td><td>2560 kSym/s</td><td>32QAM</td></tr>
</table>
<table border="2">
<tr><th>Interface Name</th><th>Provisioned</th><th>State</th><th>Speed</th><th>MAC Address</th></tr>
<tr><td>LAN</td><td>Enabled</td><td>Up</td><td>1000(Full)</td><td>78:23:AE:E3:47:AD</td></tr>
<tr><td>WAN</td><td>Enabled</td><td>Down</td><td>100(Half)</td><td>78:23:AE:E3:47:AE</td></tr>
</table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>2 d:  5 h: 07  m</td></tr>
<tr><td width="160">Computers Detected:</td><td>staticCPE(0), dynamicCPE(1)</td></tr>
<tr><td width="160">CM Status:</td><td>OPERATIONAL</td></tr>
</table>
</body></html>
"""

STATUS_CGI_WITH_INACTIVE_HTML = """\
<html><body>
<table border="2">
<tr><td>Downstream 1</td><td>14</td><td>722.00 MHz</td><td>5.00 dBmV</td><td>38.98 dB</td><td>256QAM</td><td>3932404</td><td>8</td><td>0</td></tr>
<tr><td>Downstream 2</td><td>25</td><td>810.00 MHz</td><td>----</td><td>----</td><td>----</td><td>----</td><td>----</td><td>----</td></tr>
</table>
<table border="2">
<tr><td>Upstream 1</td><td>1</td><td>81.80 MHz</td><td>47.25 dBmV</td><td>DOCSIS2.0 (ATDMA)</td><td>5120 kSym/s</td><td>64QAM</td></tr>
</table>
<table border="2"></table>
<table cellpadding="0" cellspacing="0">
<tr><td width="160">System Uptime: </td><td>0 d:  1 h: 00  m</td></tr>
<tr><td width="160">CM Status:</td><td>OPERATIONAL</td></tr>
</table>
</body></html>
"""

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

CM_STATE_CGI_HTML = """\
<html><body>
<table border="1">
<tr><td>Docsis-Downstream Scanning</td><td>Completed</td></tr>
<tr><td>Docsis-Ranging</td><td>Completed</td></tr>
<tr><td>Docsis-DHCP</td><td>Completed</td></tr>
<tr><td>Docsis-Registration</td><td>In Progress</td></tr>
</table>
<table border="1">
<tr><td>Time of Day</td><td>Retrieved</td></tr>
</table>
<table border="1">
<tr><td>BPI Status</td><td>Enabled, Authorized</td></tr>
</table>
<table border="1">
<tr><td>IPv4 Attempt(s)</td><td>1</td></tr>
<tr><td>IPv6 Attempt(s)</td><td>3</td></tr>
</table>
</body></html>
"""

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
<html><body>
<table border=1 cellpadding="1" cols="4" width="770">
<tbody>
<tr>
  <td><b>Date Time</b></td>
  <td><b>Event ID</b></td>
  <td><b>Event Level</b></td>
  <td><b>Description</b></td>
</tr>
<tr><td>4/1/2026 13:15</td><td>84020200</td><td>5</td><td>Lost MDD Timeout</td></tr>
<tr><td>4/1/2026 13:15</td><td>84000700</td><td>5</td><td>RCS Partial Service</td></tr>
<tr><td>4/1/2026 13:16</td><td>84020200</td><td>3</td><td>Lost MDD Timeout</td></tr>
<tr><td>4/1/2026 13:16</td><td>82000200</td><td>6</td><td>SYNC Timing failure</td></tr>
</tbody>
</table>
</body></html>
"""

EVENT_CGI_EMPTY_HTML = """\
<html><body>
<table border=1>
<tbody>
<tr><td><b>Date Time</b></td><td><b>Event ID</b></td><td><b>Event Level</b></td><td><b>Description</b></td></tr>
</tbody>
</table>
</body></html>
"""

VERS_CGI_HTML = """\
<html><body>
<table cellpadding="0" cellspacing="0">
<tbody>
<tr><td>System: </td><td>ARRIS EuroDOCSIS 3.0<br>
HW_REV: 1<br>
SW_REV: 9.1.103DE3<br>
MODEL: CM3200B-85</td></tr>
<tr><td>Serial Number:</td><td>7682D4111125294</td></tr>
</tbody>
</table>
<table cellpadding="0" cellspacing="0">
<tbody>
<tr><td>Firmware Name:</td><td>TS0901103DE3_061119_1602.TM</td></tr>
<tr><td>Firmware Build Time:   </td><td>Tue Jun 11 20:52:12 EDT 2019</td></tr>
</tbody>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(html: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = html
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


def _route(status_html=STATUS_CGI_HTML, cm_state_html=CM_STATE_CGI_HTML,
           event_html=EVENT_CGI_HTML, vers_html=VERS_CGI_HTML):
    """Return a side_effect that routes modem URLs to fixtures."""
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


def _collect(side_effect=None, **route_kwargs):
    """Run the collector and return {metric_name: [(labels, value), ...]}."""
    if side_effect is None:
        side_effect = _route(**route_kwargs)

    mock_session = MagicMock()
    mock_session.get.side_effect = side_effect
    collector = ArrisCollector("http://test/cgi-bin", session=mock_session)

    result: dict[str, list[tuple[dict, float]]] = {}
    for metric_family in collector.collect():
        for sample in metric_family.samples:
            result.setdefault(sample.name, []).append((sample.labels, sample.value))
    return result


def _val(metrics, name, labels=None):
    """Get a single value from collected metrics."""
    for lbl, val in metrics.get(name, []):
        if labels is None or lbl == labels:
            return val
    return None


# ===========================================================================
# Unit tests: helper functions
# ===========================================================================

class TestParseFloat:
    def test_dbmv(self):
        assert parse_float("5.00 dBmV") == 5.0

    def test_mhz(self):
        assert parse_float("722.00 MHz") == 722.0

    def test_negative(self):
        assert parse_float("-1.50 dBmV") == -1.5

    def test_plain_number(self):
        assert parse_float("3932404") == 3932404.0

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_float("----")

    def test_ksym(self):
        assert parse_float("5120 kSym/s") == 5120.0


class TestParseInt:
    def test_simple(self):
        assert parse_int("3932404") == 3932404

    def test_with_commas(self):
        assert parse_int("3,932,404") == 3932404

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_int("----")


class TestParseUptime:
    def test_normal(self):
        assert parse_uptime("2 d:  5 h: 07  m") == 2 * 86400 + 5 * 3600 + 7 * 60

    def test_zero(self):
        assert parse_uptime("0 d:  0 h: 00  m") == 0

    def test_days_only(self):
        assert parse_uptime("10 d:  0 h: 00  m") == 10 * 86400

    def test_unparseable(self):
        assert parse_uptime("unknown") == 0


# ===========================================================================
# Downstream
# ===========================================================================

class TestDownstream:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_power_ds1(self):
        assert _val(self.m, "arris_downstream_power_dbmv",
                     {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0", "modulation": "256QAM"}) == 5.0

    def test_power_ds2(self):
        assert _val(self.m, "arris_downstream_power_dbmv",
                     {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0", "modulation": "256QAM"}) == 4.5

    def test_snr(self):
        assert _val(self.m, "arris_downstream_snr_db",
                     {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0", "modulation": "256QAM"}) == 38.98

    def test_octets(self):
        assert _val(self.m, "arris_downstream_octets_total",
                     {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0"}) == 3932404

    def test_correcteds(self):
        assert _val(self.m, "arris_downstream_correcteds_total",
                     {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0"}) == 120

    def test_uncorrectables(self):
        assert _val(self.m, "arris_downstream_uncorrectables_total",
                     {"channel": "Downstream 2", "dcid": "15", "frequency_mhz": "730.0"}) == 3

    def test_frequency_hz(self):
        assert _val(self.m, "arris_downstream_frequency_hz",
                     {"channel": "Downstream 1", "dcid": "14"}) == 722e6

    def test_active(self):
        assert _val(self.m, "arris_downstream_channel_active",
                     {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0"}) == 1

    def test_channel_count(self):
        assert len(self.m.get("arris_downstream_power_dbmv", [])) == 2


# ===========================================================================
# Downstream with inactive channels
# ===========================================================================

class TestDownstreamInactive:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect(status_html=STATUS_CGI_WITH_INACTIVE_HTML)

    def test_active_channel_is_1(self):
        assert _val(self.m, "arris_downstream_channel_active",
                     {"channel": "Downstream 1", "dcid": "14", "frequency_mhz": "722.0"}) == 1

    def test_inactive_channel_is_0(self):
        assert _val(self.m, "arris_downstream_channel_active",
                     {"channel": "Downstream 2", "dcid": "25", "frequency_mhz": "810.0"}) == 0

    def test_inactive_has_parsed_frequency(self):
        """frequency_mhz label should be '810.0', not '810.00 MHz'."""
        for labels, _ in self.m.get("arris_downstream_channel_active", []):
            if labels["channel"] == "Downstream 2":
                assert labels["frequency_mhz"] == "810.0"

    def test_inactive_channel_has_no_power(self):
        """Inactive channels should not emit power/snr metrics."""
        for labels, _ in self.m.get("arris_downstream_power_dbmv", []):
            assert labels["channel"] != "Downstream 2"

    def test_no_stale_metrics(self):
        """Only active channels appear in power metrics."""
        assert len(self.m.get("arris_downstream_power_dbmv", [])) == 1


# ===========================================================================
# Upstream
# ===========================================================================

class TestUpstream:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_power(self):
        assert _val(self.m, "arris_upstream_power_dbmv",
                     {"channel": "Upstream 1", "ucid": "1", "frequency_mhz": "81.8",
                      "modulation": "64QAM", "channel_type": "DOCSIS2.0 (ATDMA)"}) == 47.25

    def test_frequency_hz(self):
        assert _val(self.m, "arris_upstream_frequency_hz",
                     {"channel": "Upstream 1", "ucid": "1"}) == 81.8e6

    def test_symbol_rate(self):
        assert _val(self.m, "arris_upstream_symbol_rate_ksps",
                     {"channel": "Upstream 2", "ucid": "3"}) == 2560

    def test_active(self):
        assert _val(self.m, "arris_upstream_channel_active",
                     {"channel": "Upstream 1", "ucid": "1", "frequency_mhz": "81.8"}) == 1

    def test_channel_count(self):
        assert len(self.m.get("arris_upstream_power_dbmv", [])) == 2


# ===========================================================================
# Status
# ===========================================================================

class TestStatus:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_uptime(self):
        assert _val(self.m, "arris_uptime_seconds") == 2 * 86400 + 5 * 3600 + 7 * 60

    def test_cm_status_operational(self):
        assert _val(self.m, "arris_cm_status", {"arris_cm_status": "operational"}) == 1

    def test_cm_status_offline_is_zero(self):
        assert _val(self.m, "arris_cm_status", {"arris_cm_status": "offline"}) == 0


class TestStatusOffline:
    def test_offline(self):
        m = _collect(status_html=STATUS_CGI_OFFLINE_HTML)
        assert _val(m, "arris_cm_status", {"arris_cm_status": "offline"}) == 1
        assert _val(m, "arris_cm_status", {"arris_cm_status": "operational"}) == 0

    def test_uptime(self):
        m = _collect(status_html=STATUS_CGI_OFFLINE_HTML)
        assert _val(m, "arris_uptime_seconds") == 120


class TestStatusOther:
    def test_other(self):
        m = _collect(status_html=STATUS_CGI_OTHER_HTML)
        assert _val(m, "arris_cm_status", {"arris_cm_status": "other"}) == 1
        assert _val(m, "arris_cm_status", {"arris_cm_status": "operational"}) == 0


# ===========================================================================
# Interfaces
# ===========================================================================

class TestInterfaces:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_lan_up(self):
        assert _val(self.m, "arris_interface_up",
                     {"interface": "LAN", "mac_address": "78:23:AE:E3:47:AD"}) == 1

    def test_wan_down(self):
        assert _val(self.m, "arris_interface_up",
                     {"interface": "WAN", "mac_address": "78:23:AE:E3:47:AE"}) == 0

    def test_lan_speed(self):
        assert _val(self.m, "arris_interface_speed_mbps",
                     {"interface": "LAN", "mac_address": "78:23:AE:E3:47:AD"}) == 1000

    def test_cable_unparseable_speed(self):
        m = _collect(status_html=STATUS_CGI_CABLE_IFACE_HTML)
        assert _val(m, "arris_interface_speed_mbps",
                     {"interface": "CABLE", "mac_address": "78:23:AE:E3:47:AE"}) == 0


# ===========================================================================
# CM State
# ===========================================================================

class TestCmState:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_scanning_completed(self):
        assert _val(self.m, "arris_docsis_step_completed",
                     {"step": "Docsis-Downstream Scanning"}) == 1

    def test_registration_not_completed(self):
        assert _val(self.m, "arris_docsis_step_completed",
                     {"step": "Docsis-Registration"}) == 0

    def test_tod_retrieved(self):
        assert _val(self.m, "arris_tod_retrieved") == 1

    def test_bpi_authorized(self):
        assert _val(self.m, "arris_bpi_authorized") == 1

    def test_dhcp_ipv4(self):
        assert _val(self.m, "arris_dhcp_attempts_ipv4") == 1

    def test_dhcp_ipv6(self):
        assert _val(self.m, "arris_dhcp_attempts_ipv6") == 3


class TestCmStateIncomplete:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect(cm_state_html=CM_STATE_CGI_INCOMPLETE_HTML)

    def test_scanning_completed(self):
        assert _val(self.m, "arris_docsis_step_completed",
                     {"step": "Docsis-Downstream Scanning"}) == 1

    def test_ranging_not_completed(self):
        assert _val(self.m, "arris_docsis_step_completed",
                     {"step": "Docsis-Ranging"}) == 0

    def test_tod_not_retrieved(self):
        assert _val(self.m, "arris_tod_retrieved") == 0

    def test_bpi_not_authorized(self):
        assert _val(self.m, "arris_bpi_authorized") == 0

    def test_dhcp_ipv4(self):
        assert _val(self.m, "arris_dhcp_attempts_ipv4") == 5


# ===========================================================================
# Event log
# ===========================================================================

class TestEventLog:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_total(self):
        assert _val(self.m, "arris_event_log_total") == 4

    def test_level_5(self):
        assert _val(self.m, "arris_event_log_by_level_total", {"level": "5"}) == 2

    def test_level_3(self):
        assert _val(self.m, "arris_event_log_by_level_total", {"level": "3"}) == 1

    def test_level_6(self):
        assert _val(self.m, "arris_event_log_by_level_total", {"level": "6"}) == 1


class TestEventLogEmpty:
    def test_total_zero(self):
        m = _collect(event_html=EVENT_CGI_EMPTY_HTML)
        assert _val(m, "arris_event_log_total") == 0


# ===========================================================================
# HW/FW Versions
# ===========================================================================

class TestVers:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_info_value(self):
        assert _val(self.m, "arris_modem_info") == 1

    def test_model(self):
        for labels, _ in self.m["arris_modem_info"]:
            assert labels["model"] == "CM3200B-85"

    def test_serial(self):
        for labels, _ in self.m["arris_modem_info"]:
            assert labels["serial_number"] == "7682D4111125294"

    def test_sw_rev(self):
        for labels, _ in self.m["arris_modem_info"]:
            assert labels["sw_rev"] == "9.1.103DE3"

    def test_firmware_name(self):
        for labels, _ in self.m["arris_modem_info"]:
            assert labels["firmware_name"] == "TS0901103DE3_061119_1602.TM"


# ===========================================================================
# Computers detected
# ===========================================================================

class TestComputers:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.m = _collect()

    def test_static(self):
        assert _val(self.m, "arris_computers_detected", {"type": "static"}) == 0

    def test_dynamic(self):
        assert _val(self.m, "arris_computers_detected", {"type": "dynamic"}) == 1


# ===========================================================================
# Build info
# ===========================================================================

class TestBuildInfo:
    def test_version(self):
        m = _collect()
        assert _val(m, "arris_exporter_build_info", {"version": __version__}) == 1


# ===========================================================================
# Scrape health
# ===========================================================================

class TestScrapeHealth:
    def test_success(self):
        m = _collect()
        assert _val(m, "arris_scrape_success") == 1

    def test_duration_positive(self):
        m = _collect()
        assert _val(m, "arris_scrape_duration_seconds") > 0

    def test_failure(self):
        def fail(url, **kwargs):
            raise requests.ConnectionError("refused")
        m = _collect(side_effect=fail)
        assert _val(m, "arris_scrape_success") == 0


# ===========================================================================
# All URLs fetched
# ===========================================================================

class TestAllUrlsFetched:
    def test_fetches_four_urls(self):
        mock_session = MagicMock()
        mock_session.get.side_effect = _route()
        collector = ArrisCollector("http://test/cgi-bin", session=mock_session)
        list(collector.collect())
        urls = [call.args[0] for call in mock_session.get.call_args_list]
        assert "http://test/cgi-bin/status_cgi" in urls
        assert "http://test/cgi-bin/cm_state_cgi" in urls
        assert "http://test/cgi-bin/event_cgi" in urls
        assert "http://test/cgi-bin/vers_cgi" in urls


# ===========================================================================
# No stale metrics between collects
# ===========================================================================

class TestNoStaleMetrics:
    def test_channel_disappears_between_scrapes(self):
        """When a channel goes from active to inactive, old active=1 entry must not persist."""
        mock_session = MagicMock()
        collector = ArrisCollector("http://test/cgi-bin", session=mock_session)

        # First scrape: 2 active channels
        mock_session.get.side_effect = _route(status_html=STATUS_CGI_HTML)
        m1: dict[str, list] = {}
        for mf in collector.collect():
            for s in mf.samples:
                m1.setdefault(s.name, []).append((s.labels, s.value))
        active1 = [v for _, v in m1.get("arris_downstream_channel_active", [])]
        assert active1.count(1) == 2

        # Second scrape: 1 active, 1 inactive
        mock_session.get.side_effect = _route(status_html=STATUS_CGI_WITH_INACTIVE_HTML)
        m2: dict[str, list] = {}
        for mf in collector.collect():
            for s in mf.samples:
                m2.setdefault(s.name, []).append((s.labels, s.value))
        active2 = [(l, v) for l, v in m2.get("arris_downstream_channel_active", [])]
        assert len(active2) == 2
        values = [v for _, v in active2]
        assert values.count(1) == 1
        assert values.count(0) == 1
