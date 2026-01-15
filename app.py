import streamlit as st
from databricks import sql
import pandas as pd
import os
import pydeck as pdk

# Page config
st.set_page_config(page_title="US Correctional Facilities", layout="wide")

# Database connection
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

# Query data
@st.cache_data(ttl=3600)
def load_facilities_data():
    conn = get_connection()
    query = """
    WITH correctional_facilities AS (
      SELECT 
        facility_name,
        notes,
        statename,
        countyname,
        instpop,
        latitude,
        longitude,
        full_address,
        so_operated_flag
      FROM raw.google_sheets_lyons_schema.local_state_correctional
    ),
    
    salesforce_sheriffs AS (
      SELECT 
        id AS account_id,
        name AS account_name,
        billing_state,
        billing_city,
        type
      FROM raw.salesforce.account
      WHERE name LIKE '%Sheriff%'
        AND is_deleted = false
    ),
    
    matched_facilities AS (
      SELECT 
        cf.facility_name,
        cf.notes AS original_notes,
        cf.countyname,
        cf.statename,
        cf.instpop AS inmate_population,
        cf.full_address,
        cf.so_operated_flag,
        cf.latitude,
        cf.longitude,
        ss.account_name AS salesforce_account_name,
        CASE 
          WHEN ss.account_id IS NOT NULL 
          THEN CONCAT('https://skydio.lightning.force.com/', ss.account_id)
          ELSE NULL
        END AS salesforce_link,
        ss.billing_state AS sf_state,
        ss.billing_city AS sf_city,
        ss.type AS sf_account_type,
        CASE
          WHEN ss.account_name IS NULL THEN 999
          WHEN LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%sheriff%')
               AND (LOWER(ss.billing_state) = LOWER(cf.statename) 
                    OR LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.statename), '%'))
          THEN 1
          WHEN LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%')
               AND LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.statename), '%')
          THEN 2
          WHEN LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%')
          THEN 3
          ELSE 4
        END AS match_score,
        CASE
          WHEN ss.account_name IS NULL THEN 'No Match Found'
          WHEN LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%sheriff%')
               AND (LOWER(ss.billing_state) = LOWER(cf.statename) 
                    OR LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.statename), '%'))
          THEN 'High Confidence'
          WHEN LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%')
               AND LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.statename), '%')
          THEN 'Medium Confidence'
          WHEN LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%')
          THEN 'Low Confidence'
          ELSE 'Very Low'
        END AS match_confidence
      FROM correctional_facilities cf
      LEFT JOIN salesforce_sheriffs ss
        ON (LOWER(ss.account_name) LIKE CONCAT('%', LOWER(cf.countyname), '%')
            OR (LOWER(ss.billing_state) = LOWER(cf.statename) 
                AND LOWER(ss.account_name) LIKE '%sheriff%'))
    )
    
    SELECT *
    FROM matched_facilities
    QUALIFY ROW_NUMBER() OVER (PARTITION BY facility_name ORDER BY match_score, salesforce_account_name) = 1
    ORDER BY match_confidence, statename, countyname
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetchall_arrow().to_pandas()
    cursor.close()
    return df

# Main app
st.title("🏛️ US Correctional Facilities")

# Load data
with st.spinner("Loading facilities data..."):
    df = load_facilities_data()

# Sidebar filters
st.sidebar.header("Filters")

# State filter
states = ['All'] + sorted(df['statename'].dropna().unique().tolist())
selected_state = st.sidebar.selectbox("State", states)

# County filter
if selected_state != 'All':
    counties = ['All'] + sorted(df[df['statename'] == selected_state]['countyname'].dropna().unique().tolist())
else:
    counties = ['All'] + sorted(df['countyname'].dropna().unique().tolist())
selected_county = st.sidebar.selectbox("County", counties)

# Search box
search_term = st.sidebar.text_input("🔍 Search Facility Name", "")

# Apply filters
filtered_df = df.copy()

if selected_state != 'All':
    filtered_df = filtered_df[filtered_df['statename'] == selected_state]

if selected_county != 'All':
    filtered_df = filtered_df[filtered_df['countyname'] == selected_county]

if search_term:
    filtered_df = filtered_df[
        filtered_df['facility_name'].str.contains(search_term, case=False, na=False)
    ]

# Display summary metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Facilities", len(filtered_df))
with col2:
    matched = len(filtered_df[filtered_df['match_confidence'] != 'No Match Found'])
    st.metric("Matched", matched)
with col3:
    high_conf = len(filtered_df[filtered_df['match_confidence'] == 'High Confidence'])
    st.metric("High Confidence", high_conf)
with col4:
    no_match = len(filtered_df[filtered_df['match_confidence'] == 'No Match Found'])
    st.metric("No Match", no_match)

# Interactive Map Section
st.subheader("🗺️ Facility Locations Map")

# Prepare map data
map_df = filtered_df[filtered_df['latitude'].notna() & filtered_df['longitude'].notna()].copy()

# Create tooltip text
map_df['tooltip_text'] = map_df.apply(
    lambda row: f"{row['facility_name']}\n"
                f"County: {row['countyname']}, {row['statename']}\n"
                f"Population: {row['inmate_population']}\n"
                f"Match: {row['match_confidence']}\n"
                f"SF Account: {row['salesforce_account_name'] if pd.notna(row['salesforce_account_name']) else 'None'}",
    axis=1
)

# Create the map layer with a single, neutral color for all points
layer = pdk.Layer(
    'ScatterplotLayer',
    data=map_df,
    get_position='[longitude, latitude]',
    get_fill_color=[40, 80, 120, 200],  # Muted blue-gray
    get_radius=5000,  # Radius in meters
    pickable=True,
    auto_highlight=True
)

# Set the viewport location
view_state = pdk.ViewState(
    latitude=map_df['latitude'].mean(),
    longitude=map_df['longitude'].mean(),
    zoom=4,
    pitch=0
)

# Render the map with a clean, minimalist basemap (no API key required)
r = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={
        'text': '{tooltip_text}'
    },
    map_provider='carto',
    map_style='road'
)

st.pydeck_chart(r)
st.markdown("---")

# Display interactive table
st.subheader(f"Facilities ({len(filtered_df)} results)")

# Format the dataframe for display
display_df = filtered_df[[
    'facility_name', 
    'countyname', 
    'statename',
    'inmate_population',
    'salesforce_account_name',
    'salesforce_link',
    'full_address'
]].copy()

# Rename columns for better display
display_df.columns = [
    'Facility Name',
    'County',
    'State',
    'Inmate Pop.',
    'Salesforce Account',
    'Salesforce Link',
    'Address'
]

# Display with clickable links
st.dataframe(
    display_df,
    use_container_width=True,
    height=600,
    column_config={
        "Salesforce Link": st.column_config.LinkColumn(
            "Salesforce Link",
            display_text="Open in SF"
        ),
        "Inmate Pop.": st.column_config.NumberColumn(
            "Inmate Pop.",
            format="%d"
        )
    }
)

# Download button
csv = filtered_df.to_csv(index=False)
st.download_button(
    label="📥 Download Results as CSV",
    data=csv,
    file_name="correctional_facilities_matches.csv",
    mime="text/csv"
)