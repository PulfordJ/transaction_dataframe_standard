"""
Vanguard Fund Name to Ticker Mapping

Maps Vanguard fund names (as they appear in transaction details) to standard ticker symbols.
"""

import re

# Fund name patterns -> ticker mappings
# Using regex patterns to handle variations in fund names
VANGUARD_FUND_MAPPINGS = [
    # US Equity
    (r'U\.?S\.?\s+Equity\s+Index\s+Fund', 'VUSA'),  # Vanguard S&P 500 UCITS ETF
    (r'US\s+Equity\s+Index', 'VUSA'),

    # UK Equity
    (r'FTSE\s+100\s+Index', 'VUKE'),  # Vanguard FTSE 100 UCITS ETF
    (r'FTSE\s+UK\s+Equity\s+Income', 'VDID'),  # Vanguard FTSE UK Equity Income Index

    # Europe
    (r'FTSE\s+Developed\s+Europe\s+.*ex.*UK', 'VERX'),  # Vanguard FTSE Developed Europe ex UK
    (r'Developed\s+Europe\s+ex[- ]UK', 'VERX'),

    # Asia Pacific
    (r'Pacific\s+ex[- ]Japan\s+Stock\s+Index', 'VFEG'),  # Vanguard FTSE Developed Asia Pacific ex Japan
    (r'FTSE\s+Developed\s+Asia\s+Pacific\s+ex[- ]Japan', 'VAPX'),
    (r'Developed\s+Asia\s+Pacific\s+ex[- ]Japan', 'VAPX'),

    # Global/All World
    (r'FTSE\s+All[- ]World', 'VWRL'),  # Vanguard FTSE All-World UCITS ETF
    (r'All[- ]World\s+Stock', 'VWRL'),

    # Add more as discovered
]


def fund_name_to_ticker(fund_name: str) -> str:
    """
    Convert Vanguard fund name to ticker symbol.

    Args:
        fund_name: Fund name as it appears in transaction details

    Returns:
        Ticker symbol, or fund_name prefixed with 'FUND:' if not found
    """
    if not fund_name:
        return None

    fund_name_clean = fund_name.strip()

    # Try each pattern
    for pattern, ticker in VANGUARD_FUND_MAPPINGS:
        if re.search(pattern, fund_name_clean, re.IGNORECASE):
            return ticker

    # Not found - return with prefix to indicate it's unmapped
    return f"FUND:{fund_name_clean[:20]}"  # Truncate long names


def is_fund_mapped(fund_name: str) -> bool:
    """Check if a fund name has a known ticker mapping."""
    if not fund_name:
        return False

    for pattern, _ in VANGUARD_FUND_MAPPINGS:
        if re.search(pattern, fund_name, re.IGNORECASE):
            return True

    return False


def extract_fund_name_from_details(details: str) -> tuple[str, float]:
    """
    Extract fund name and units from transaction details.

    Vanguard transaction details format examples:
    - "Bought 48.2405 U.S. Equity Index Fund - Accumulation"
    - "Sold 116.4353 U.S. Equity Index Fund - Accumulation"
    - "DIV: 2.0000 FTSE Developed Asia Pacific ex-Japan UCITS ETF"

    Returns:
        (fund_name, units) tuple
    """
    if not details:
        return (None, None)

    # Pattern for Buy/Sell: "Bought/Sold X.XXX Fund Name"
    buy_sell_pattern = r'(?:Bought|Sold)\s+([\d,.]+)\s+(.+?)(?:\s*-\s*Accumulation|\s*-\s*Income|\s*\(.*?\))?$'
    match = re.search(buy_sell_pattern, details, re.IGNORECASE)

    if match:
        units_str = match.group(1).replace(',', '')
        fund_name = match.group(2).strip()
        try:
            units = float(units_str)
        except:
            units = None
        return (fund_name, units)

    # Pattern for Dividend: "DIV: X.XXXX Fund Name"
    div_pattern = r'DIV:\s+([\d,.]+)\s+(.+?)(?:\s*-\s*Accumulation|\s*-\s*Income)?$'
    match = re.search(div_pattern, details, re.IGNORECASE)

    if match:
        units_str = match.group(1).replace(',', '')
        fund_name = match.group(2).strip()
        try:
            units = float(units_str)
        except:
            units = None
        return (fund_name, units)

    return (None, None)
