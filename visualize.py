import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import streamlit as st
from PIL import Image


# Authentication and BigQuery client setup
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Cache data loading function
@st.cache_data(ttl=600)
def load_data():
    query = f"""
        SELECT day, steps
        FROM `{client.project}.fitbit_transform.fct_steps`
        ORDER BY day
    """
    data = client.query(query).to_dataframe()
    data['day'] = pd.to_datetime(data['day'])
    return data

# Load data
data = load_data()

# Sort data by day in descending order
data = data.sort_values(by='day', ascending=False)

# Format the 'day' column to show as 'YYYY-MM-DD'
data['day'] = data['day'].dt.strftime('%Y-%m-%d')

# Title of the app
st.title('Steps Data Visualization')

# Create tabs
tab1, tab2, tab3 = st.tabs(["Overview", "Insights", "Pipeline Details"])

# Overview tab
with tab1:
    st.header("Overview")
    st.write("This app visualizes step count data over time, collected from a Fitbit device.")

    st.write("### Key Metrics")
    st.write(f"**Total Days of Data:** {len(data)}")
    st.write(f"**Average Steps per Day:** {data['steps'].mean():,.0f}")
    st.write(f"**Total Steps:** {data['steps'].sum():,}")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.write("### Line Chart")
        st.line_chart(data.set_index('day')['steps'], width=700, height=400)

    with col2:
        st.write("### Data Table")
        st.dataframe(data)

# Insights tab
with tab2:
    st.header("Insights")
    st.write("In this section, we provide insights derived from the step data.")

    # Calculate moving average
    data['7_day_avg'] = data['steps'].rolling(window=7).mean()

    st.write("### 7-Day Moving Average of Steps")
    st.line_chart(data.set_index('day')[['steps', '7_day_avg']])

    # Highlight trends
    max_steps_day = data[data['steps'] == data['steps'].max()]['day'].iloc[0]
    min_steps_day = data[data['steps'] == data['steps'].min()]['day'].iloc[0]
    st.write(f"**Day with Maximum Steps:** {max_steps_day} ({data['steps'].max():,} steps)")
    st.write(f"**Day with Minimum Steps:** {min_steps_day} ({data['steps'].min():,} steps)")

    st.write("### Weekly Trends")
    data['week'] = pd.to_datetime(data['day']).dt.isocalendar().week
    weekly_data = data.groupby('week').agg({'steps': 'sum'})
    st.bar_chart(weekly_data)

# Pipeline Details tab
with tab3:
    st.header("Pipeline Details")

    st.markdown(
        """
        <div style="background-color: #e7f3fe; padding: 10px; border-left: 6px solid #2196F3; color: black; margin-bottom: 20px;">
            💻 Here's the <a href="https://github.com/fnery/fitbit-steps" target="_blank" style="color: #2196F3;">GitHub repo</a> including source code for this data app and the underlying pipeline. 
            <br><br>
            📝 Also, I wrote a blog post describing this project in detail. Check it out <a href="https://fnery.io/posts/end-to-end-de-fitbit-data/" target="_blank" style="color: #2196F3;">here</a>!
        </div>
        """,
        unsafe_allow_html=True
    )

    st.write("This section provides details about the data pipeline used to extract, transform, and visualize the step data.")

    image = Image.open('diagram.png')
    st.image(image, caption="Data Pipeline")

    st.write("""
        ### Pipeline Description
        The data pipeline consists of the following components:

        - **[Fitbit API](https://dev.fitbit.com/build/reference/web-api/)**: Used to extract step count data.
        - **[data load tool (dlt)](https://dlthub.com/)**: Used to load data into BigQuery.
        - **[Google BigQuery](https://cloud.google.com/bigquery)**: A fully-managed data warehouse used to store and query the step data.
        - **[dbt](https://www.getdbt.com/)**: A data transformation tool used to transform and model the data in BigQuery.
        - **[GitHub Actions](https://github.com/features/actions)**: Used to automate the deployment of the data pipeline.
        - **[Streamlit](https://streamlit.io/)**: Used to visualize the data in an interactive web app.

        The process starts with extracting data from the Fitbit API, loading it into BigQuery using dlt. The data is then transformed using dbt and visualized using Streamlit. GitHub Actions are used to automate the deployment of the pipeline.
    """)
