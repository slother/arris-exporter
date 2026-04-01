#!/usr/bin/env python3
"""Prometheus exporter for Arris Touchstone cable modem (DOCSIS)."""

from __future__ import annotations

import argparse
import re
import sys
import time

import requests
from bs4 import BeautifulSoup
from prometheus_client import (
    CollectorRegistry,
    Enum,
    Gauge,
    start_http_server,
)

MODEM_BASE = "http://192.168.100.1/cgi-bin"
MODEM_STATUS_URL = f"{MODEM_BASE}/status_cgi"
MODEM_CM_STATE_URL = f"{MODEM_BASE}/cm_state_cgi"
MODEM_EVENT_URL = f"{MODEM_BASE}/event_cgi"
MODEM_VERS_URL = f"{MODEM_BASE}/vers_cgi"
SCRAPE_INTERVAL = 30

registry = CollectorRegistry()

# -- Downstream --
ds_power = Gauge(
    "arris_downstream_power_dbmv",
    "Downstream channel power level in dBmV",
    ["channel", "dcid", "frequency_mhz", "modulation"],
    registry=registry,
)
ds_snr = Gauge(
    "arris_downstream_snr_db",
    "Downstream channel SNR in dB",
    ["channel", "dcid", "frequency_mhz", "modulation"],
    registry=registry,
)
ds_octets = Gauge(
    "arris_downstream_octets_total",
    "Downstream octets received (resets on modem reboot)",
    ["channel", "dcid", "frequency_mhz"],
    registry=registry,
)
ds_correcteds = Gauge(
    "arris_downstream_correcteds_total",
    "Downstream FEC corrected codewords",
    ["channel", "dcid", "frequency_mhz"],
    registry=registry,
)
ds_uncorrectables = Gauge(
    "arris_downstream_uncorrectables_total",
    "Downstream FEC uncorrectable codewords",
    ["channel", "dcid", "frequency_mhz"],
    registry=registry,
)
ds_frequency = Gauge(
    "arris_downstream_frequency_hz",
    "Downstream channel frequency in Hz",
    ["channel", "dcid"],
    registry=registry,
)
ds_active = Gauge(
    "arris_downstream_channel_active",
    "Downstream channel active (1 = locked, 0 = no signal)",
    ["channel", "dcid", "frequency_mhz"],
    registry=registry,
)

# -- Upstream --
us_power = Gauge(
    "arris_upstream_power_dbmv",
    "Upstream channel power level in dBmV",
    ["channel", "ucid", "frequency_mhz", "modulation", "channel_type"],
    registry=registry,
)
us_frequency = Gauge(
    "arris_upstream_frequency_hz",
    "Upstream channel frequency in Hz",
    ["channel", "ucid"],
    registry=registry,
)
us_symbol_rate = Gauge(
    "arris_upstream_symbol_rate_ksps",
    "Upstream symbol rate in kSym/s",
    ["channel", "ucid"],
    registry=registry,
)
us_active = Gauge(
    "arris_upstream_channel_active",
    "Upstream channel active (1 = locked, 0 = no signal)",
    ["channel", "ucid", "frequency_mhz"],
    registry=registry,
)

# -- Status --
uptime_seconds = Gauge(
    "arris_uptime_seconds",
    "Modem uptime in seconds",
    registry=registry,
)
cm_status = Enum(
    "arris_cm_status",
    "Cable modem operational status",
    states=["operational", "offline", "other"],
    registry=registry,
)

# -- Interfaces --
iface_up = Gauge(
    "arris_interface_up",
    "Interface state (1 = Up, 0 = Down)",
    ["interface", "mac_address"],
    registry=registry,
)
iface_speed = Gauge(
    "arris_interface_speed_mbps",
    "Interface speed in Mbps",
    ["interface", "mac_address"],
    registry=registry,
)

# -- CM State (registration) --
docsis_step_completed = Gauge(
    "arris_docsis_step_completed",
    "DOCSIS registration step status (1 = Completed, 0 = not)",
    ["step"],
    registry=registry,
)
tod_retrieved = Gauge(
    "arris_tod_retrieved",
    "Time of Day retrieved (1 = yes, 0 = no)",
    registry=registry,
)
bpi_authorized = Gauge(
    "arris_bpi_authorized",
    "BPI authorized (1 = yes, 0 = no)",
    registry=registry,
)
dhcp_attempts_ipv4 = Gauge(
    "arris_dhcp_attempts_ipv4",
    "DHCP IPv4 attempts to obtain CM IP address",
    registry=registry,
)
dhcp_attempts_ipv6 = Gauge(
    "arris_dhcp_attempts_ipv6",
    "DHCP IPv6 attempts to obtain CM IP address",
    registry=registry,
)

# -- Event log --
event_log_total = Gauge(
    "arris_event_log_total",
    "Total number of events in the event log",
    registry=registry,
)
event_log_by_level = Gauge(
    "arris_event_log_by_level_total",
    "Number of events in the event log by severity level",
    ["level"],
    registry=registry,
)

# -- HW/FW version info --
modem_info = Gauge(
    "arris_modem_info",
    "Modem hardware and firmware information (always 1)",
    ["model", "serial_number", "hw_rev", "sw_rev", "firmware_name", "firmware_build_time"],
    registry=registry,
)

# -- Computers detected --
computers_detected = Gauge(
    "arris_computers_detected",
    "Number of computers detected by the modem",
    ["type"],
    registry=registry,
)

# -- Scrape health --
scrape_success = Gauge(
    "arris_scrape_success",
    "Whether the last scrape was successful (1 = yes, 0 = no)",
    registry=registry,
)
scrape_duration = Gauge(
    "arris_scrape_duration_seconds",
    "Duration of the last scrape in seconds",
    registry=registry,
)


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


def fetch_page(url: str) -> BeautifulSoup | None:
    """Fetch a modem page and return parsed soup, or None on failure."""
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch {url}: {e}", file=sys.stderr)
        return None


def scrape_event_log(base_url: str) -> bool:
    """Scrape the event log page."""
    url = f"{base_url}/event_cgi"
    soup = fetch_page(url)
    if soup is None:
        return False

    table = soup.find("table", attrs={"border": "1"})
    if table is None:
        return True

    total = 0
    level_counts: dict[str, int] = {}
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        # Skip header row (contains <b> tags)
        if cells[0].find("b"):
            continue
        total += 1
        level = cells[2].get_text(strip=True)
        level_counts[level] = level_counts.get(level, 0) + 1

    event_log_total.set(total)
    for level, count in level_counts.items():
        event_log_by_level.labels(level).set(count)

    return True


def scrape_vers(base_url: str) -> bool:
    """Scrape the HW/FW versions page."""
    url = f"{base_url}/vers_cgi"
    soup = fetch_page(url)
    if soup is None:
        return False

    text = soup.get_text()

    model = ""
    serial_number = ""
    hw_rev = ""
    sw_rev = ""
    firmware_name = ""
    firmware_build_time = ""

    # Parse the system info block
    m = re.search(r"MODEL:\s*(.+)", text)
    if m:
        model = m.group(1).strip()

    m = re.search(r"HW_REV:\s*(.+)", text)
    if m:
        hw_rev = m.group(1).strip()

    m = re.search(r"SW_REV:\s*(.+)", text)
    if m:
        sw_rev = m.group(1).strip()

    # Parse serial number from table
    m = re.search(r"Serial\s*Number:\s*(.+)", text)
    if m:
        serial_number = m.group(1).strip()

    # Parse firmware info from table
    tables = soup.find_all("table", attrs={"cellpadding": "0", "cellspacing": "0"})
    for tbl in tables:
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            if "firmware name" in label:
                firmware_name = value
            elif "firmware build time" in label:
                firmware_build_time = value

    modem_info.labels(
        model=model,
        serial_number=serial_number,
        hw_rev=hw_rev,
        sw_rev=sw_rev,
        firmware_name=firmware_name,
        firmware_build_time=firmware_build_time,
    ).set(1)

    return True


def scrape_cm_state(base_url: str) -> bool:
    """Scrape the CM State / registration page."""
    url = f"{base_url}/cm_state_cgi"
    soup = fetch_page(url)
    if soup is None:
        return False

    tables = soup.find_all("table", attrs={"border": "1"})

    # DOCSIS registration steps (first table)
    if len(tables) >= 1:
        for row in tables[0].find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            step = cells[0].get_text(strip=True)
            status = cells[1].get_text(strip=True).lower()
            docsis_step_completed.labels(step).set(1 if status == "completed" else 0)

    # TOD State (second table)
    if len(tables) >= 2:
        for row in tables[1].find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                status = cells[1].get_text(strip=True).lower()
                tod_retrieved.set(1 if status == "retrieved" else 0)

    # BPI State (third table)
    if len(tables) >= 3:
        for row in tables[2].find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                status = cells[1].get_text(strip=True).lower()
                bpi_authorized.set(1 if "authorized" in status else 0)

    # DHCP Attempts (fourth table)
    if len(tables) >= 4:
        for row in tables[3].find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            try:
                value = parse_int(cells[1].get_text(strip=True))
            except ValueError:
                continue
            if "ipv4" in label:
                dhcp_attempts_ipv4.set(value)
            elif "ipv6" in label:
                dhcp_attempts_ipv6.set(value)

    return True


def scrape(base_url: str) -> None:
    start = time.monotonic()

    status_url = f"{base_url}/status_cgi"
    soup = fetch_page(status_url)
    if soup is None:
        scrape_success.set(0)
        scrape_duration.set(time.monotonic() - start)
        return

    # --- Downstream ---
    tables = soup.find_all("table", attrs={"border": "2"})
    if len(tables) >= 1:
        ds_table = tables[0]
        for row in ds_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 9:
                continue
            text = [c.get_text(strip=True) for c in cells]
            if text[0].startswith("Downstream"):
                channel = text[0]
                dcid = text[1]
                try:
                    freq_mhz = str(parse_float(text[2]))
                    freq_hz = parse_float(text[2]) * 1e6
                    power = parse_float(text[3])
                    snr = parse_float(text[4])
                    modulation = text[5]
                    octets = parse_int(text[6])
                    correcteds = parse_int(text[7])
                    uncorrectables = parse_int(text[8])
                except ValueError:
                    ds_active.labels(channel, dcid, text[2]).set(0)
                    continue

                ds_active.labels(channel, dcid, freq_mhz).set(1)
                labels = [channel, dcid, freq_mhz, modulation]
                ds_power.labels(*labels).set(power)
                ds_snr.labels(*labels).set(snr)
                ds_octets.labels(channel, dcid, freq_mhz).set(octets)
                ds_correcteds.labels(channel, dcid, freq_mhz).set(correcteds)
                ds_uncorrectables.labels(channel, dcid, freq_mhz).set(uncorrectables)
                ds_frequency.labels(channel, dcid).set(freq_hz)

    # --- Upstream ---
    if len(tables) >= 2:
        us_table = tables[1]
        for row in us_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 7:
                continue
            text = [c.get_text(strip=True) for c in cells]
            if text[0].startswith("Upstream"):
                channel = text[0]
                ucid = text[1]
                try:
                    freq_mhz = str(parse_float(text[2]))
                    freq_hz = parse_float(text[2]) * 1e6
                    power = parse_float(text[3])
                    channel_type = text[4]
                    sym_rate = parse_float(text[5])
                    modulation = text[6]
                except ValueError:
                    us_active.labels(channel, ucid, text[2]).set(0)
                    continue

                us_active.labels(channel, ucid, freq_mhz).set(1)
                us_power.labels(channel, ucid, freq_mhz, modulation, channel_type).set(power)
                us_frequency.labels(channel, ucid).set(freq_hz)
                us_symbol_rate.labels(channel, ucid).set(sym_rate)

    # --- Status ---
    status_tables = soup.find_all("table", attrs={"cellpadding": "0", "cellspacing": "0"})
    for tbl in status_tables:
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)

            if "uptime" in label:
                uptime_seconds.set(parse_uptime(value))
            elif "computers detected" in label:
                # Format: "staticCPE(0), dynamicCPE(1)"
                for cpe_match in re.finditer(r"(static|dynamic)CPE\((\d+)\)", value):
                    cpe_type = cpe_match.group(1)
                    cpe_count = int(cpe_match.group(2))
                    computers_detected.labels(cpe_type).set(cpe_count)
            elif "cm status" in label:
                status = value.lower()
                if status == "operational":
                    cm_status.state("operational")
                elif status in ("offline", "not operational"):
                    cm_status.state("offline")
                else:
                    cm_status.state("other")

    # --- Interfaces ---
    if len(tables) >= 3:
        iface_table = tables[2]
        for row in iface_table.find_all("tr"):
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

            iface_up.labels(name, mac).set(1 if state.lower() == "up" else 0)
            try:
                speed = parse_float(speed_str)
                iface_speed.labels(name, mac).set(speed)
            except ValueError:
                iface_speed.labels(name, mac).set(0)

    # --- CM State ---
    scrape_cm_state(base_url)

    # --- Event log ---
    scrape_event_log(base_url)

    # --- HW/FW Versions ---
    scrape_vers(base_url)

    elapsed = time.monotonic() - start
    scrape_success.set(1)
    scrape_duration.set(elapsed)
    print(f"[OK] Scraped modem in {elapsed:.2f}s")


def main():
    parser = argparse.ArgumentParser(description="Arris Touchstone modem Prometheus exporter")
    parser.add_argument("--port", type=int, default=9120, help="Exporter listen port (default: 9120)")
    parser.add_argument("--interval", type=int, default=SCRAPE_INTERVAL, help="Scrape interval in seconds (default: 30)")
    parser.add_argument("--base-url", default=MODEM_BASE, help="Modem CGI base URL")
    args = parser.parse_args()

    print(f"Starting Arris exporter on :{args.port}, scraping {args.base_url} every {args.interval}s")
    start_http_server(args.port, registry=registry)

    while True:
        scrape(args.base_url)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
