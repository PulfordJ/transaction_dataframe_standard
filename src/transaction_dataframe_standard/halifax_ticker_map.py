"""
Halifax Fund Ticker Mapping

Maps Halifax fund tickers to standard identifiers.
Halifax uses internal fund codes that need to be mapped to recognizable tickers.
"""

# Halifax fund ticker mappings
HALIFAX_TICKER_MAP = {
    'MDAABG': 'MDAABG',  # Halifax unit trust - Yahoo Finance ticker: 0P00013P6I.L (£2.55-3.76)
    'VRXXC': 'VUKE-OEIC',  # Vanguard UK Equity Index Fund - OEIC (£150-210 per unit)
    'VAPX': 'VAPX',      # Vanguard Asia Pacific ex Japan ETF
    'VVUSEI': 'VUSA-OEIC',  # Vanguard US Equity Index Fund - OEIC (£900+ per unit)

    # Add more mappings as discovered
}

def halifax_ticker_to_standard(halifax_ticker: str) -> str:
    """
    Convert Halifax fund ticker to standard ticker symbol.

    Args:
        halifax_ticker: Halifax internal fund code

    Returns:
        Standard ticker symbol, or Halifax ticker prefixed with 'HFX:' if not found
    """
    if not halifax_ticker:
        return None

    ticker_upper = halifax_ticker.strip().upper()

    if ticker_upper in HALIFAX_TICKER_MAP:
        return HALIFAX_TICKER_MAP[ticker_upper]
    else:
        # Return with prefix to indicate unmapped Halifax ticker
        return f"HFX:{ticker_upper}"


def is_halifax_ticker_mapped(halifax_ticker: str) -> bool:
    """Check if a Halifax ticker has a known mapping."""
    if not halifax_ticker:
        return False
    return halifax_ticker.strip().upper() in HALIFAX_TICKER_MAP


# Note: These mappings may need verification
# To verify, check the Halifax account statements for full fund names
# or cross-reference with holdings that transferred from HL
