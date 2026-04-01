#!/usr/bin/env python3
"""Prometheus exporter for Arris Touchstone cable modem (DOCSIS).

Uses a Custom Collector so metrics are generated fresh on each
Prometheus scrape — no stale label problems.
"""

from __future__ import annotations

import argparse
import logging
import re
import signal
import sys
import threading
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from prometheus_client import (
    CollectorRegistry,
    start_http_server,
)
from prometheus_client.core import GaugeMetricFamily

__version__ = "0.1.0"

MODEM_BASE = "http://192.168.100.1/cgi-bin"
MAX_RESPONSE_BYTES = 1_000_000  # 1 MB

log = logging.getLogger("arris_exporter")


def parse_float(text: str) -> float:
    """Extract the numeric part from strings like '5.00 dBmV', '722.00 MHz'."""
    m = re.search(r"[-+]?\d+\.?\d*", text)
    if m:
        return float(m.group())
    raise ValueError(f"Cannot parse number from: {text!r}")


def parse_int(text: str) -> int:
    m = re.search(r"\d+", text.replace(",", ""))
    if m:
        return int(m.group())
    raise ValueError(f"Cannot parse int from: {text!r}")


def parse_uptime(text: str) -> int:
    """Parse '0 d:  0 h: 07  m' into seconds."""
    m = re.search(r"(\d+)\s*d.*?(\d+)\s*h.*?(\d+)\s*m", text)
    if m:
        days, hours, minutes = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return days * 86400 + hours * 3600 + minutes * 60
    return 0


class ArrisCollector:
    """Custom Prometheus collector for Arris Touchstone cable modems."""

    def __init__(self, base_url: str, timeout: int = 10,
                 session: requests.Session | None = None):
        self.base_url = base_url
        self.timeout = timeout
        self.session = session or requests.Session()

    def _fetch_page(self, url: str) -> BeautifulSoup | None:
        """Fetch a modem page and return parsed soup, or None on failure."""
        log.debug("Fetching %s", url)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            if len(resp.text) > MAX_RESPONSE_BYTES:
                log.error("Response from %s exceeds %d bytes, skipping",
                          url, MAX_RESPONSE_BYTES)
                return None
            log.debug("Fetched %s (%d bytes)", url, len(resp.text))
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            log.error("Failed to fetch %s: %s", url, e)
            return None

    def _parse_downstream(self, tables):
        """Parse downstream channels. Returns list of metric families."""
        ds_power = GaugeMetricFamily(
            "arris_downstream_power_dbmv",
            "Downstream channel power level in dBmV",
            labels=["channel", "dcid", "frequency_mhz", "modulation"],
        )
        ds_snr = GaugeMetricFamily(
            "arris_downstream_snr_db",
            "Downstream channel SNR in dB",
            labels=["channel", "dcid", "frequency_mhz", "modulation"],
        )
        ds_octets = GaugeMetricFamily(
            "arris_downstream_octets_total",
            "Downstream octets received (resets on modem reboot)",
            labels=["channel", "dcid", "frequency_mhz"],
        )
        ds_correcteds = GaugeMetricFamily(
            "arris_downstream_correcteds_total",
            "Downstream FEC corrected codewords",
            labels=["channel", "dcid", "frequency_mhz"],
        )
        ds_uncorrectables = GaugeMetricFamily(
            "arris_downstream_uncorrectables_total",
            "Downstream FEC uncorrectable codewords",
            labels=["channel", "dcid", "frequency_mhz"],
        )
        ds_frequency = GaugeMetricFamily(
            "arris_downstream_frequency_hz",
            "Downstream channel frequency in Hz",
            labels=["channel", "dcid"],
        )
        ds_active = GaugeMetricFamily(
            "arris_downstream_channel_active",
            "Downstream channel active (1 = locked, 0 = no signal)",
            labels=["channel", "dcid", "frequency_mhz"],
        )

        if len(tables) >= 1:
            for row in tables[0].find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 9:
                    continue
                text = [c.get_text(strip=True) for c in cells]
                if text[0].startswith("Downstream"):
                    channel = text[0]
                    dcid = text[1]
                    freq_mhz = str(parse_float(text[2]))
                    try:
                        freq_hz = parse_float(text[2]) * 1e6
                        power = parse_float(text[3])
                        snr = parse_float(text[4])
                        modulation = text[5]
                        octets = parse_int(text[6])
                        correcteds = parse_int(text[7])
                        uncorr = parse_int(text[8])
                    except ValueError:
                        log.debug("%s (dcid=%s, %s MHz) inactive — no signal data",
                                  channel, dcid, freq_mhz)
                        ds_active.add_metric([channel, dcid, freq_mhz], 0)
                        continue

                    ds_active.add_metric([channel, dcid, freq_mhz], 1)
                    ds_power.add_metric([channel, dcid, freq_mhz, modulation], power)
                    ds_snr.add_metric([channel, dcid, freq_mhz, modulation], snr)
                    ds_octets.add_metric([channel, dcid, freq_mhz], octets)
                    ds_correcteds.add_metric([channel, dcid, freq_mhz], correcteds)
                    ds_uncorrectables.add_metric([channel, dcid, freq_mhz], uncorr)
                    ds_frequency.add_metric([channel, dcid], freq_hz)

        return [ds_power, ds_snr, ds_octets, ds_correcteds,
                ds_uncorrectables, ds_frequency, ds_active]

    def _parse_upstream(self, tables):
        """Parse upstream channels. Returns list of metric families."""
        us_power = GaugeMetricFamily(
            "arris_upstream_power_dbmv",
            "Upstream channel power level in dBmV",
            labels=["channel", "ucid", "frequency_mhz", "modulation", "channel_type"],
        )
        us_frequency = GaugeMetricFamily(
            "arris_upstream_frequency_hz",
            "Upstream channel frequency in Hz",
            labels=["channel", "ucid"],
        )
        us_symbol_rate = GaugeMetricFamily(
            "arris_upstream_symbol_rate_ksps",
            "Upstream symbol rate in kSym/s",
            labels=["channel", "ucid"],
        )
        us_active = GaugeMetricFamily(
            "arris_upstream_channel_active",
            "Upstream channel active (1 = locked, 0 = no signal)",
            labels=["channel", "ucid", "frequency_mhz"],
        )

        if len(tables) >= 2:
            for row in tables[1].find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 7:
                    continue
                text = [c.get_text(strip=True) for c in cells]
                if text[0].startswith("Upstream"):
                    channel = text[0]
                    ucid = text[1]
                    freq_mhz = str(parse_float(text[2]))
                    try:
                        freq_hz = parse_float(text[2]) * 1e6
                        power = parse_float(text[3])
                        channel_type = text[4]
                        sym_rate = parse_float(text[5])
                        modulation = text[6]
                    except ValueError:
                        log.debug("%s (ucid=%s, %s MHz) inactive — no signal data",
                                  channel, ucid, freq_mhz)
                        us_active.add_metric([channel, ucid, freq_mhz], 0)
                        continue

                    us_active.add_metric([channel, ucid, freq_mhz], 1)
                    us_power.add_metric([channel, ucid, freq_mhz, modulation, channel_type], power)
                    us_frequency.add_metric([channel, ucid], freq_hz)
                    us_symbol_rate.add_metric([channel, ucid], sym_rate)

        return [us_power, us_frequency, us_symbol_rate, us_active]

    def _parse_status(self, soup, tables):
        """Parse modem status, interfaces, and computers. Returns list of metric families."""
        uptime_metric = GaugeMetricFamily(
            "arris_uptime_seconds", "Modem uptime in seconds",
        )
        cm_status = GaugeMetricFamily(
            "arris_cm_status", "Cable modem operational status",
            labels=["arris_cm_status"],
        )
        iface_up = GaugeMetricFamily(
            "arris_interface_up", "Interface state (1 = Up, 0 = Down)",
            labels=["interface", "mac_address"],
        )
        iface_speed = GaugeMetricFamily(
            "arris_interface_speed_mbps", "Interface speed in Mbps",
            labels=["interface", "mac_address"],
        )
        computers = GaugeMetricFamily(
            "arris_computers_detected",
            "Number of computers detected by the modem",
            labels=["type"],
        )

        status_tables = soup.find_all("table", attrs={"cellpadding": "0", "cellspacing": "0"})
        cm_state_val = {"operational": 0, "offline": 0, "other": 0}
        for tbl in status_tables:
            for row in tbl.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                if "uptime" in label:
                    uptime_metric.add_metric([], parse_uptime(value))
                elif "computers detected" in label:
                    for cpe_match in re.finditer(r"(static|dynamic)CPE\((\d+)\)", value):
                        computers.add_metric([cpe_match.group(1)], int(cpe_match.group(2)))
                elif "cm status" in label:
                    status = value.lower()
                    if status == "operational":
                        cm_state_val["operational"] = 1
                    elif status in ("offline", "not operational"):
                        cm_state_val["offline"] = 1
                    else:
                        cm_state_val["other"] = 1

        for state, val in cm_state_val.items():
            cm_status.add_metric([state], val)

        if len(tables) >= 3:
            for row in tables[2].find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                text = [c.get_text(strip=True) for c in cells]
                name = text[0]
                if name in ("Interface Name", ""):
                    continue
                state = text[2]
                speed_str = text[3]
                mac = text[4]

                iface_up.add_metric([name, mac], 1 if state.lower() == "up" else 0)
                try:
                    iface_speed.add_metric([name, mac], parse_float(speed_str))
                except ValueError:
                    iface_speed.add_metric([name, mac], 0)

        return [uptime_metric, cm_status, iface_up, iface_speed, computers]

    def _parse_cm_state(self):
        """Parse cm_state_cgi. Returns list of metric families."""
        docsis_step = GaugeMetricFamily(
            "arris_docsis_step_completed",
            "DOCSIS registration step status (1 = Completed, 0 = not)",
            labels=["step"],
        )
        tod = GaugeMetricFamily(
            "arris_tod_retrieved",
            "Time of Day retrieved (1 = yes, 0 = no)",
        )
        bpi = GaugeMetricFamily(
            "arris_bpi_authorized",
            "BPI authorized (1 = yes, 0 = no)",
        )
        dhcp_v4 = GaugeMetricFamily(
            "arris_dhcp_attempts_ipv4",
            "DHCP IPv4 attempts to obtain CM IP address",
        )
        dhcp_v6 = GaugeMetricFamily(
            "arris_dhcp_attempts_ipv6",
            "DHCP IPv6 attempts to obtain CM IP address",
        )

        cm_soup = self._fetch_page(f"{self.base_url}/cm_state_cgi")
        if cm_soup is not None:
            cm_tables = cm_soup.find_all("table", attrs={"border": "1"})

            if len(cm_tables) >= 1:
                for row in cm_tables[0].find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        step_name = cells[0].get_text(strip=True)
                        step_status = cells[1].get_text(strip=True).lower()
                        docsis_step.add_metric([step_name], 1 if step_status == "completed" else 0)

            if len(cm_tables) >= 2:
                for row in cm_tables[1].find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        tod.add_metric([], 1 if cells[1].get_text(strip=True).lower() == "retrieved" else 0)

            if len(cm_tables) >= 3:
                for row in cm_tables[2].find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        bpi.add_metric([], 1 if "authorized" in cells[1].get_text(strip=True).lower() else 0)

            if len(cm_tables) >= 4:
                for row in cm_tables[3].find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    lbl = cells[0].get_text(strip=True).lower()
                    try:
                        val = parse_int(cells[1].get_text(strip=True))
                    except ValueError:
                        continue
                    if "ipv4" in lbl:
                        dhcp_v4.add_metric([], val)
                    elif "ipv6" in lbl:
                        dhcp_v6.add_metric([], val)

        return [docsis_step, tod, bpi, dhcp_v4, dhcp_v6]

    def _parse_events(self):
        """Parse event_cgi. Returns list of metric families."""
        evt_total = GaugeMetricFamily(
            "arris_event_log_total",
            "Total number of events in the event log",
        )
        evt_by_level = GaugeMetricFamily(
            "arris_event_log_by_level_total",
            "Number of events in the event log by severity level",
            labels=["level"],
        )

        evt_soup = self._fetch_page(f"{self.base_url}/event_cgi")
        if evt_soup is not None:
            table = evt_soup.find("table", attrs={"border": "1"})
            if table is not None:
                total = 0
                level_counts: dict[str, int] = {}
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 4:
                        continue
                    if cells[0].find("b"):
                        continue
                    total += 1
                    level = cells[2].get_text(strip=True)
                    level_counts[level] = level_counts.get(level, 0) + 1

                evt_total.add_metric([], total)
                for level, count in level_counts.items():
                    evt_by_level.add_metric([level], count)

        return [evt_total, evt_by_level]

    def _parse_versions(self):
        """Parse vers_cgi. Returns list of metric families."""
        modem_info = GaugeMetricFamily(
            "arris_modem_info",
            "Modem hardware and firmware information (always 1)",
            labels=["model", "serial_number", "hw_rev", "sw_rev",
                    "firmware_name", "firmware_build_time"],
        )

        vers_soup = self._fetch_page(f"{self.base_url}/vers_cgi")
        if vers_soup is not None:
            text = vers_soup.get_text()
            model = hw_rev = sw_rev = serial = fw_name = fw_build = ""

            m = re.search(r"MODEL:\s*(.+)", text)
            if m:
                model = m.group(1).strip()
            m = re.search(r"HW_REV:\s*(.+)", text)
            if m:
                hw_rev = m.group(1).strip()
            m = re.search(r"SW_REV:\s*(.+)", text)
            if m:
                sw_rev = m.group(1).strip()
            m = re.search(r"Serial\s*Number:\s*(.+)", text)
            if m:
                serial = m.group(1).strip()

            for tbl in vers_soup.find_all("table", attrs={"cellpadding": "0", "cellspacing": "0"}):
                for row in tbl.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    lbl = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    if "firmware name" in lbl:
                        fw_name = val
                    elif "firmware build time" in lbl:
                        fw_build = val

            modem_info.add_metric([model, serial, hw_rev, sw_rev, fw_name, fw_build], 1)

        return [modem_info]

    def collect(self):
        log.info("Scrape started")
        start = time.monotonic()
        success = 1
        ds_total = ds_locked = us_total = us_locked = 0

        soup = self._fetch_page(f"{self.base_url}/status_cgi")
        if soup is None:
            log.warning("status_cgi unavailable, skipping RF metrics")
            success = 0
        else:
            tables = soup.find_all("table", attrs={"border": "2"})

            ds_families = self._parse_downstream(tables)
            for f in ds_families:
                if f.name == "arris_downstream_channel_active":
                    ds_total = len(f.samples)
                    ds_locked = sum(1 for s in f.samples if s.value == 1)

            us_families = self._parse_upstream(tables)
            for f in us_families:
                if f.name == "arris_upstream_channel_active":
                    us_total = len(f.samples)
                    us_locked = sum(1 for s in f.samples if s.value == 1)

            status_families = self._parse_status(soup, tables)

            yield from ds_families
            yield from us_families
            yield from status_families

        yield from self._parse_cm_state()
        yield from self._parse_events()
        yield from self._parse_versions()

        # Build info
        build_info = GaugeMetricFamily(
            "arris_exporter_build_info",
            "Exporter version information (always 1)",
            labels=["version"],
        )
        build_info.add_metric([__version__], 1)
        yield build_info

        # Scrape health
        elapsed = time.monotonic() - start
        scrape_ok = GaugeMetricFamily(
            "arris_scrape_success",
            "Whether the last scrape was successful (1 = yes, 0 = no)",
        )
        scrape_dur = GaugeMetricFamily(
            "arris_scrape_duration_seconds",
            "Duration of the last scrape in seconds",
        )
        scrape_ok.add_metric([], success)
        scrape_dur.add_metric([], elapsed)

        if success:
            log.info("Scrape completed in %.2fs — DS: %d/%d locked, US: %d/%d locked",
                     elapsed, ds_locked, ds_total, us_locked, us_total)
        else:
            log.warning("Scrape failed after %.2fs", elapsed)

        yield scrape_ok
        yield scrape_dur


def main():
    parser = argparse.ArgumentParser(description="Arris Touchstone modem Prometheus exporter")
    parser.add_argument("--port", type=int, default=9120, help="Exporter listen port (default: 9120)")
    parser.add_argument("--base-url", default=MODEM_BASE, help="Modem CGI base URL")
    parser.add_argument("--log-level", default="info",
                        choices=["debug", "info", "warning", "error"],
                        help="Log level (default: info)")
    args = parser.parse_args()

    parsed_url = urlparse(args.base_url)
    if parsed_url.scheme not in ("http", "https"):
        parser.error("--base-url must use http:// or https://")

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, args.log_level.upper()),
    )

    registry = CollectorRegistry()
    registry.register(ArrisCollector(args.base_url))

    log.info("Starting Arris exporter v%s on :%d, target %s", __version__, args.port, args.base_url)
    start_http_server(args.port, registry=registry)
    log.info("Listening on :%d/metrics", args.port)

    shutdown = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())
    shutdown.wait()
    log.info("Shutting down")


if __name__ == "__main__":
    main()
