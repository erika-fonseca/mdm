import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal


st.write("""<figure><embed type="image/svg+xml" src="https://www.oldendorff.com/img/oldendorff-logo.svg" /></figure>""", unsafe_allow_html=True)
st.header(body="Demo - Master Data Management", divider=True)
st.write(
    "Mainting master data using INSERT OVERWRITE using SQL warehouse."
)

server_hostname = os.getenv("DATABRICKS_HOST")
client_id = os.getenv("DATABRICKS_CLIENT_ID")
client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
whs_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
http_path = f"/sql/1.0/warehouses/{whs_id}"

def credential_provider():
    config = Config(
        host=f"https://{server_hostname}",
        client_id=client_id,
        client_secret=client_secret,
    )

    return oauth_service_principal(config)


def read_table(table_name) -> pd.DataFrame:
    info = st.empty()
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=credential_provider,
    ) as conn:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM {table_name}"
            with info:
                st.info("Calling Databricks SQL...")
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall_arrow().to_pandas())
            info.empty()

        return df


def insert_overwrite_table(table_name, edited_df):
    progress = st.empty()
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=credential_provider,
    ) as conn:
        with conn.cursor() as cursor:
            rows = list(edited_df.itertuples(index=False))
            values = ",".join([f"({','.join(map(repr, row))})" for row in rows])
            with progress:
                st.info("Calling Databricks SQL...")
            cursor.execute(
                f"INSERT OVERWRITE {table_name} VALUES {values}",
            )
            progress.empty()
            st.success(f"Changes saved")



original_df = pd.DataFrame(
        {
            "customer_id": [f"cust_{i}" for i in range(1, 6)],
            "state": ["CA", "NY", "TX", "FL", "IL"],
            "review": [
                "Great product!",
                "Very satisfied",
                "Could be better",
                "Not what I expected",
                "Excellent service",
            ],
            "review_score": [5, 4, 3, 2, 5],
        }
    )

table_name = st.text_input(
        "Specify a Catalog table name:",
        placeholder="dbdemos.data_talks.MasterData",
        help="Copy the three-level table name from the [Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#granting-and-revoking-access-to-database-objects-and-other-securable-objects-in-unity-catalog).",
    )
if table_name:
    original_df = read_table(table_name)
else:
    st.warning("Using mock data")

edited_df = st.data_editor(original_df, num_rows="dynamic", hide_index=True)

df_diff = pd.concat([original_df, edited_df]).drop_duplicates(keep=False)
if not df_diff.empty:
    if st.button("Save"):
        insert_overwrite_table(table_name, edited_df)

