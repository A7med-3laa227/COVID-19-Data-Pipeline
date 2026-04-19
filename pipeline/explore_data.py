import pandas as pd
import os

from sqlalchemy import create_engine, text



# 1. Provide the raw GitHub URL for a specific day's dataset (Jan 1, 2021)

csv_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/01-01-2021.csv"


def extraction():

    print(f"Downloading COVID-19 data from {csv_url} ...\n")
    

    # 2. Read the CSV directly from the URL using pandas

    try:

        df = pd.read_csv(csv_url)

    except Exception as e:

        print(f"Error downloading data: {e}")

        print("Please ensure you have an internet connection and 'pandas' is installed.")

        return None
    

    print(f"Extraction complete: {len(df)} rows, {len(df.columns)} columns.\n")
    return df



def transformation(df):
    if df is None:

        print("No data to transform.")

        return None, None, None


    print("Transforming data...")


    # --- Common Transformations ---

    # Convert Last_Update column to datetime

    df['Last_Update'] = pd.to_datetime(df['Last_Update'], errors='coerce')


    # Standardize location names

    location_mapping = {

        'US': 'United States',

        'UK': 'United Kingdom',

        'Korea, South': 'South Korea',

        'Taiwan*': 'Taiwan',

        'Korea, North': 'North Korea'

    }

    df['Country_Region'] = df['Country_Region'].replace(location_mapping)


    # Drop unuseful columns for the fact table

    df.drop(['FIPS', 'Admin2', 'Province_State'], axis=1, inplace=True)


    # Convert negative coordinate values to absolute values

    df['Lat'] = df['Lat'].abs()

    df['Long_'] = df['Long_'].abs()


    # Fill NaN values in numeric columns with the column mean

    numerical_cols = df.select_dtypes(include='number').columns

    for col in numerical_cols:

        df[col] = df[col].fillna(df[col].mean())


    # --- Create dim_date ---

    print("Creating dim_date...")

    # Ensure unique dates (ignore time) to satisfy the UNIQUE constraint on full_date
    dim_date = pd.DataFrame({'date': df['Last_Update'].dt.normalize()}).drop_duplicates().dropna().reset_index(drop=True)

    dim_date['date_id'] = dim_date.index + 1

    dim_date['full_date']  = dim_date['date'].dt.strftime('%Y-%m-%d')

    dim_date['year']  = dim_date['date'].dt.year

    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day']   = dim_date['date'].dt.day
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['month_name'] = dim_date['date'].dt.month_name()
    dim_date['day_of_week'] = dim_date['date'].dt.day_name()

    dim_date = dim_date[['date_id', 'full_date', 'year', 'month', 'day', 'quarter', 'month_name', 'day_of_week']]

    print(f"dim_date created: {len(dim_date)} rows, {len(dim_date.columns)} columns.")


    # --- Create dim_location ---

    print("Creating dim_location...")

    dim_location = (

        df[['Country_Region', 'Combined_Key', 'Lat', 'Long_']]

        .drop_duplicates('Combined_Key')

        .reset_index(drop=True)

        .copy()
    )

    dim_location.rename(columns={
        'Country_Region': 'country_region', 
        'Combined_Key': 'combined_key', 
        'Lat': 'lat', 
        'Long_': 'long_'
    }, inplace=True)

    dim_location['location_id'] = dim_location.index + 1

    dim_location = dim_location[['location_id', 'country_region', 'combined_key', 'lat', 'long_']]

    print(f"dim_location created: {len(dim_location)} rows, {len(dim_location.columns)} columns.")


    # --- Create fact_covid_cases ---

    print("Creating fact_covid_cases...")

    fact_covid_cases = df.copy()


    # Add a string date column to join cleanly with dim_date

    fact_covid_cases['_date_str'] = fact_covid_cases['Last_Update'].dt.strftime('%Y-%m-%d')


    # Merge with dim_date to get date_id

    fact_covid_cases = pd.merge(

        fact_covid_cases,

        dim_date[['date_id', 'full_date']],

        left_on='_date_str',

        right_on='full_date',

        how='left'
    )

    fact_covid_cases.drop(columns=['_date_str', 'full_date', 'Last_Update'], inplace=True)


    # Merge with dim_location to get location_id

    fact_covid_cases = pd.merge(

        fact_covid_cases,

        dim_location[['location_id', 'combined_key']],

        left_on='Combined_Key',

        right_on='combined_key',

        how='left'
    )

    fact_covid_cases.drop(columns=['Country_Region', 'Lat', 'Long_', 'Combined_Key', 'combined_key'], inplace=True)


    # Select and rename columns for fact table

    fact_covid_cases = fact_covid_cases[[

        'date_id', 'location_id', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Incident_Rate', 'Case_Fatality_Ratio'

    ]]

    fact_covid_cases.rename(columns={

        'Confirmed': 'confirmed',

        'Deaths': 'deaths',

        'Recovered': 'recovered',

        'Active': 'active',

        'Incident_Rate': 'incident_rate',

        'Case_Fatality_Ratio': 'case_fatality'

    }, inplace=True)

    fact_covid_cases['fact_id'] = fact_covid_cases.index + 1

    fact_covid_cases = fact_covid_cases[['fact_id', 'date_id', 'location_id', 'confirmed', 'deaths', 'recovered', 'active', 'incident_rate', 'case_fatality']]

    print(f"fact_covid_cases created: {len(fact_covid_cases)} rows, {len(fact_covid_cases.columns)} columns.\n")


    print("Transformation complete.")

    return dim_date, dim_location, fact_covid_cases


def load(dim_date, dim_location, fact_covid_cases):

    if dim_date is None or dim_location is None or fact_covid_cases is None:

        print("No data to load to PostgreSQL.")
        return


    # PostgreSQL connection details

    # IMPORTANT: Replace with your actual PostgreSQL credentials and host

    DB_HOST = os.getenv("DB_HOST", "localhost")

    DB_PORT = os.getenv("DB_PORT", "5433")

    DB_NAME = os.getenv("DB_NAME", "covid_pipeline")

    DB_USER = os.getenv("DB_USER", "postgres")

    DB_PASSWORD = os.getenv("DB_PASSWORD", "6112001") # Use a strong password or environment variable


    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


    print(f"Attempting to connect to PostgreSQL database: {DB_NAME} on {DB_HOST}:{DB_PORT}...")

    try:

        engine = create_engine(DATABASE_URL)

        with engine.connect() as connection:

            # Test connection

            connection.execute(text("SELECT 1"))

        print("Successfully connected to PostgreSQL.")

    except Exception as e:

        print(f"Error connecting to PostgreSQL: {e}")

        print("Please ensure PostgreSQL is running and connection details are correct.")
        return


    # Define table names

    table_names = {

        'dim_date': dim_date,

        'dim_location': dim_location,

        'fact_covid_cases': fact_covid_cases

    }


    for table_name, df_to_load in table_names.items():

        print(f"Loading data to '{table_name}' table...")

        try:

            # Use 'append' to add to existing, preventing the drop of the schema.sql tables!

            # For initial load or daily updates, 'replace' would destroy constraints.

            # We must use 'append' for all tables to honor constraints.

            df_to_load.to_sql(table_name, engine, if_exists='append', index=False)

            print(f"Successfully loaded {len(df_to_load)} rows to '{table_name}'.")

        except Exception as e:

            print(f"Error loading data to '{table_name}': {e}")

            # Optionally, roll back or handle specific errors


    print("Load to PostgreSQL complete!")


if __name__ == "__main__":

    # Run the full ETL pipeline

    raw_df = extraction()

    dim_date_df, dim_location_df, fact_covid_cases_df = transformation(raw_df)

    load(dim_date_df, dim_location_df, fact_covid_cases_df)

