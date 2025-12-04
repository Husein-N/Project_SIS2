import pandas as pd
import re

# -----------------------------
# Helper functions
# -----------------------------

def clean_market_cap(value):
    """
    Convert '$4.36T' → 4.36e12
    Convert '$708.39B' → 7.0839e11
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):  # already numeric
        return float(value)

    value = value.replace("$", "").strip()

    multipliers = {
        "T": 1e12,
        "B": 1e9,
        "M": 1e6,
        "K": 1e3
    }

    match = re.match(r"([0-9\.]+)([TBMK])", value)
    if not match:
        return None

    num, unit = match.groups()
    return float(num) * multipliers[unit]


def clean_ratio(value):
    """
    Clean P/E and PEG values.
    '44.6x' → 44.6
    Handles floats, strings, NaN.
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    value = value.replace("x", "").strip()
    return float(value)


def clean_price_inplace(value):
    """
    Convert '$553.73' → 553.73
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    return float(value.replace("$", "").replace(",", "").strip())


# -----------------------------
# Main cleaning function
# -----------------------------

def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:

    # 1) Remove Sector (all are Technology)
    if "Sector" in df.columns:
        df = df.drop(columns=["Sector"])

    # 2) Create numeric MarketCap next to Market Cap
    df.insert(
        df.columns.get_loc("Market Cap") + 1,
        "MarketCap_Digit",
        df["Market Cap"].apply(clean_market_cap)
    )

    # 3) Fix P/E and PEG ratios
    df["P/E Ratio"] = df["P/E Ratio"].apply(clean_ratio)
    df["PEG Ratio"] = df["PEG Ratio"].apply(clean_ratio)

    # 4) Clean Last Trade Price IN-PLACE
    df["Last Trade Price"] = df["Last Trade Price"].apply(clean_price_inplace)

    return df


# -----------------------------
# Airflow wrapper (REQUIRED)
# -----------------------------
def run_cleaning():
    df = pd.read_csv("raw_data.csv")
    cleaned = clean_dataset(df)
    cleaned.to_csv("cleaned_data.csv", index=False)
    print("[CLEANER] Saved cleaned_data.csv with", len(cleaned), "rows")


# -----------------------------
# Manual execution
# -----------------------------
if __name__ == "__main__":
    df = pd.read_csv("raw_data.csv")
    cleaned = clean_dataset(df)
    cleaned.to_csv("cleaned_data.csv", index=False)
    print("Cleaning complete → cleaned_data.csv")
