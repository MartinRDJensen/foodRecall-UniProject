import xml.etree.ElementTree as et
import pandas as pd
import datetime
import os.path

# -----------------------------------------------------------------------------
# Time
# -----------------------------------------------------------------------------
# The interval is including START_YEAR:START_MONTH and excluding END_YEAR:END_MONTH
START_YEAR = 1979
START_MONTH = 9  # september
END_YEAR = 2020
END_MONTH = 1  # januar


# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------
RAW_DATA_FILE = "data/data.xml"


def create_dataframes():
    """
    Creates a new Pandas dataframe containing only alert notifications.

    The created dataframes does not contain all information from each data entry.
    """

    # NotificationFrom   -> string Country
    # DateOfCase         -> datetime Date
    # RiskDecision       -> string Risk
    # ActionTaken        -> string Action
    # DistributionStatus -> string DistributionStatus
    # Product            -> string Product
    # ProductCategory    -> string Category
    # Flagged origins    -> list(stirng) Origins

    alert_cols = [
        "Country",
        "Date",
        "Subject",
        "Risk",
        "Action",
        "DistributionStatus",
        "ProductCategory",
        "Product",
    ]
    alert_index = []
    alert_rows = []
    hazard_cols = ["Reference", "Substance", "Category"]
    hazard_rows = []

    origin_cols = ["Reference", "Country"]
    origin_rows = []

    init_raw_data()

    xtree = et.parse(RAW_DATA_FILE)
    xroot = xtree.getroot()
    for entry in xroot:
        notification = entry.find("Notification")
        details = notification.find("Details")

        # Filter only alerts
        if "-  alert  -" not in details.find("NotificationType").text:
            continue

        # Filter date
        date = parse_date(details.find("DateOfCase").text)
        if date.year > END_YEAR or (date.year == END_YEAR and date.month >= END_MONTH):
            continue

        subject = details.find("Subject").text
        reference = details.find("Reference").text
        action = details.find("ActionTaken").text
        country = sanitize_country(details.find("NotificationFrom").text)
        distribution_status = details.find("DistributionStatus").text
        product = details.find("Product").text
        product_category = details.find("ProductCategory").text
        risk = details.find("RiskDecision").text

        alert_index.append(reference)
        alert_rows.append(
            (
                country,
                date,
                subject,
                risk,
                action,
                distribution_status,
                product_category,
                product,
            )
        )

        for row in notification.find("Flagged"):
            if row.find("Orig").text == "1":
                country = sanitize_country(row.find("Country").text)
                origin_rows.append((reference, country))

        for row in notification.find("Hazards"):
            substance = row.find("Substance").text
            category = row.find("Category").text
            hazard_rows.append((reference, substance, category))

    alerts_df = pd.DataFrame(alert_rows, columns=alert_cols, index=alert_index)
    hazards_df = pd.DataFrame(hazard_rows, columns=hazard_cols)
    origins_df = pd.DataFrame(origin_rows, columns=origin_cols)
    return alerts_df, hazards_df, origins_df


def parse_date(date_str):
    """
    Parses a date string into a datetime object
    """
    return datetime.datetime.strptime(date_str, r"%d/%m/%Y")


def sanitize_country(country: str):
    """
    Removes an eventual country code from the country name
    """
    if country.endswith(")"):
        country = country[0 : country.index("(")]
    return country.strip()


def init_raw_data(raw_dir="data/raw", out=RAW_DATA_FILE):
    """
    Creates a combined xml file containing the scraped data.
    """
    if os.path.exists(RAW_DATA_FILE):
        return
    data = "<Data>\n"
    for filename in os.listdir(raw_dir):
        if filename.endswith(".xml"):
            with open(os.path.join(raw_dir, filename), "r") as f:
                content = f.read().replace('<?xml version="1.0" encoding="UTF-8"?>', "")
                data += content

    data += "\n</Data>"
    with open(out, "w") as f:
        f.write(data)


# Create global variables
alerts_df, hazards_df, origins_df = create_dataframes()

# -----------------------------------------------------------------------------
# Data retrieval
# -----------------------------------------------------------------------------


def select_alerts(countries=None, interval=None, category=None, product=None):
    """
    Selects all alerts by specified country in given time interval.
    If country is None, the alerts are not filtered based on country.
    If interval is None, the alerts are not filtered on date.
    """
    alerts = alerts_df
    if interval is not None:
        alerts = alerts[
            (alerts["Date"] >= interval[0]) & (alerts["Date"] <= interval[1])
        ]
    if countries is not None:
        if type(countries) is list:
            alerts = alerts[alerts["Country"].isin(countries)]
        else:
            alerts = alerts[alerts["Country"] == countries]
    if category is not None:
        alerts = alerts[alerts["ProductCategory"] == category]
        if product is not None:
            alerts = alerts[alerts["Product"] == product]
    return alerts


def select_origins(refs=None):
    """
    Return origins grouped by country for the specified references.
    """
    if refs is None:
        return origins_df
    return origins_df[origins_df["Reference"].isin(refs)]


def group_by_country(data):
    """
    Return a series with data grouped by countries and counted.
    """
    return data.groupby("Country")["Country"].count()


def get_pies(country=None, interval=None):
    """
    Returns df with columns=['ProductCategory', 'Count']
            dff with columns=['Category', 'Count']
    """

    df = select_alerts(country, interval)
    dff = hazards_df[hazards_df["Reference"].isin(df.index)]
    df = df.groupby("ProductCategory")["ProductCategory"].count()
    df = df.to_frame()
    df = df.rename(columns={"ProductCategory": "Count"})
    df = df.reset_index()

    dff = dff.groupby("Category")["Category"].count()
    dff = dff.to_frame()
    dff = dff.rename(columns={"Category": "Count"})
    dff = dff.reset_index()
    return df, dff


def get_product_categories():
    """
    Returns a dict of product category mapping to list of products in that category.
    """

    def key(x):
        t1 = 2 if x.endswith("(obsolete)") else (1 if x.endswith("(other)") else 0)
        return (t1, x)

    alerts = select_alerts()[["ProductCategory", "Product"]]

    cat_list = sorted(alerts.ProductCategory.unique().tolist(), key=key)
    categories = [{"label": c, "value": c} for c in cat_list]

    products = {}
    for c in cat_list:
        prod_list = sorted(
            alerts[alerts.ProductCategory == c].Product.unique().tolist(), key=key
        )
        prods = [{"label": p, "value": p} for p in prod_list]
        products[c] = prods

    return categories, products


# -----------------------------------------------------------------------------
# Test
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Run some tests
    if sanitize_country("Denmark (DA)") != "Denmark":
        print("Failed to sanitize Denmark, got", sanitize_country("Denmark (DA)"))
    if sanitize_country("United Kingdom (GB)") != "United Kingdom":
        print(
            "Failed to sanitize United Kingdom, got",
            sanitize_country("United Kingdom (GB)"),
        )

    # alerts = select_alerts(countries=['Germany', 'Italy'], interval=[datetime.datetime(2019, 10, 1), datetime.datetime(2019,10,31)])
    # origins = select_origins(alerts.index)
    # print('Alerts by country',group_by_country(alerts))
    # print('Origins by country',group_by_country(origins))

    # dates = select_alerts()['Date']
    # print('min date', min(dates))
    # print('max date', max(dates))

    get_product_categories()
