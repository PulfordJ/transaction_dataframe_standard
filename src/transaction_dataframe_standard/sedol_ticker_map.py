"""
SEDOL to Ticker Mapping

SEDOL (Stock Exchange Daily Official List) codes are 7-character identifiers
used in the UK and Ireland. This module maps SEDOL codes to standard ticker symbols.
"""

# Known SEDOL -> Ticker mappings from HL reports
SEDOL_TO_TICKER = {
    # US Tech Stocks (traded on London exchanges)
    '2857817': 'NFLX',      # Netflix Inc
    'B7TL820': 'META',      # Meta Platforms Inc
    '2073390': 'BRK.B',     # Berkshire Hathaway Inc Class B

    # Add more mappings as discovered from HL reports
    # Format: 'SEDOL': 'TICKER'
}

# Reverse mapping for convenience
TICKER_TO_SEDOL = {v: k for k, v in SEDOL_TO_TICKER.items()}


def sedol_to_ticker(sedol: str) -> str:
    """
    Convert SEDOL code to ticker symbol.

    Args:
        sedol: 7-character SEDOL code

    Returns:
        Ticker symbol, or SEDOL if not found (with 'SEDOL:' prefix)
    """
    sedol = sedol.strip().upper()

    if sedol in SEDOL_TO_TICKER:
        return SEDOL_TO_TICKER[sedol]
    else:
        # Return SEDOL prefixed to indicate it's unmapped
        return f"SEDOL:{sedol}"


def ticker_to_sedol(ticker: str) -> str:
    """
    Convert ticker symbol to SEDOL code.

    Args:
        ticker: Stock ticker symbol

    Returns:
        SEDOL code, or None if not found
    """
    ticker = ticker.strip().upper()
    return TICKER_TO_SEDOL.get(ticker)


def is_sedol_mapped(sedol: str) -> bool:
    """Check if a SEDOL code has a known ticker mapping."""
    return sedol.strip().upper() in SEDOL_TO_TICKER


def get_all_mappings() -> dict:
    """Get all SEDOL->Ticker mappings."""
    return SEDOL_TO_TICKER.copy()
