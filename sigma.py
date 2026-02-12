import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path


BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"

# Get all London monitoring stations
def get_london_sites():

    url = f"{BASE_URL}/Information/MonitoringSites/GroupName=London/Json"

    r = requests.get(url)
    r.raise_for_status()

    data = r.json()

    sites = data["Sites"]["Site"]

    site_codes = [site["@SiteCode"] for site in sites]
    print(f"Found {len(site_codes)} London monitoring sites")

    return site_codes


# Get raw data for one site

def fetch_site_data(site_code, start, end):

    url = (
        f"{BASE_URL}/Data/Site/"
        f"SiteCode={site_code}/"
        f"StartDate={start}/"
        f"EndDate={end}/Json"
    )

    r = requests.get(url)
    r.raise_for_status()

    data = r.json()

    if "Site" not in data:
        return None

    records = data["Site"].get("Data", [])

    if not records:
        return None

    df = pd.DataFrame(records)
    df["site"] = site_code

    return df


# ----------------------------------
# Get yesterday's date
# ----------------------------------
def get_yesterday():

    y = date.today() - timedelta(days=1)

    return y.strftime("%Y-%m-%d")


# ----------------------------------
# Main pipeline
# ----------------------------------
def main():

    print("Starting London Air Quality Pipeline")

    # Create folders
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)

    # Date
    target_date = get_yesterday()

    print("Target date:", target_date)

    # Get sites
    sites = get_london_sites()

    print(f"Found {len(sites)} monitoring sites")

    all_data = []

    # Download each site
    for site in sites:

        print(f"Fetching {site}...")

        try:
            df = fetch_site_data(site, target_date, target_date)

            if df is not None:
                all_data.append(df)

        except Exception as e:
            print(f"Failed {site}: {e}")

    if not all_data:
        raise RuntimeError("No data collected")

    # Combine
    combined = pd.concat(all_data, ignore_index=True)

    # Save raw
    raw_file = f"data/raw/london_raw_{target_date}.csv"
    combined.to_csv(raw_file, index=False)

    print("Saved raw:", raw_file)

    # Process into daily averages

    combined["DateTime"] = pd.to_datetime(combined["DateTime"])
    combined["date"] = combined["DateTime"].dt.date

    # Keep only numeric columns
    numeric_cols = combined.select_dtypes("number").columns

    daily = (
        combined
        .groupby("date")[numeric_cols]
        .mean()
        .reset_index()
    )

    processed_file = f"data/processed/london_daily_{target_date}.csv"

    daily.to_csv(processed_file, index=False)

    print("Saved processed:", processed_file)

    print("Pipeline finished successfully")

if __name__ == "__main__":
    main()