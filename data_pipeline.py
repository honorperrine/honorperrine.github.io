import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
import os

# --- 1. CONFIGURATION ---

# Define the name of your local SQLite database file
DB_NAME = 'reit_research.db'

# Use the current directory to place the database file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(BASE_DIR, DB_NAME)

# --- 2. SQLALCHEMY ENGINE SETUP ---

# The connection string for a SQLite file
ENGINE_STRING = f'sqlite:///{DATABASE_PATH}'
engine = create_engine(ENGINE_STRING)

print(f"SQLAlchemy Engine created for: {DATABASE_PATH}")

# Optional: Test the connection by creating a dummy connection
try:
    with engine.connect() as connection:
        print("Successfully connected to/created the database file.")
except Exception as e:
    print(f"Error establishing connection: {e}")

# --- 3. REIT SECTOR DEFINITION ---

# Choose a single, specific REIT sector for focus.
SECTOR_NAME = "U.S. Multifamily REITs"

# List the tickers for the major players in your chosen sector.
REIT_TICKERS = [
    'EQR',  # Equity Residential
    'AVB',  # AvalonBay Communities
    'ESS',  # Essex Property Trust
    'MAA',  # Mid-America Apartment Communities
    'AIV',  # Apartment Investment and Management Company
    'UDR',  # UDR, Inc.
    'CIM',  # Chimera Investment Corporation (Mortgage REIT)
]

print(f"\nTargeting Sector: {SECTOR_NAME} with {len(REIT_TICKERS)} tickers.")

# --- 4. DATA FETCHING FUNCTION ---

def fetch_historical_prices(tickers):
    """
    Fetches historical closing prices for a list of stock tickers
    over the last 5 years.
    """
    print(f"\nFetching historical price data for {len(tickers)} REITs...")

    try:
        # Use yfinance download to get data for all tickers at once
        price_data = yf.download(
            tickers,
            period="5y",
            interval="1wk",
            group_by='ticker',
            progress=False
        )

        # Prepare a list to hold the cleaned dataframes
        cleaned_dfs = []

        # Loop through each ticker to clean the data
        for ticker in tickers:
            # Select the ticker's data and only keep the 'Close' price
            ticker_df = price_data[ticker]['Close'].reset_index()
            ticker_df.columns = ['Date', 'Close_Price']

            # Add the Ticker column
            ticker_df['Ticker'] = ticker

            # Drop any rows where the closing price is missing (NaN)
            ticker_df.dropna(subset=['Close_Price'], inplace=True)

            # Convert Date column to consistent string format for SQL
            ticker_df['Date'] = ticker_df['Date'].dt.strftime('%Y-%m-%d')

            cleaned_dfs.append(ticker_df)

        # Concatenate all individual ticker dataframes into one final DataFrame
        final_prices_df = pd.concat(cleaned_dfs, ignore_index=True)

        print("Price data fetching complete.")
        return final_prices_df

    except Exception as e:
        print(f"An error occurred during price data fetching: {e}")
        return pd.DataFrame()

# --- 5. FUNDAMENTALS FETCHING FUNCTION ---

def fetch_fundamentals(tickers):
    """
    Fetches key quarterly financial data (Net Income, Shares Outstanding)
    for TTM P/FFO calculation proxy.
    """
    print(f"\nFetching quarterly financial fundamentals...")

    fundamentals_data = []

    # Loop through each ticker individually to fetch financial statements
    for ticker_symbol in tickers:
        print(f"  -> Processing fundamentals for {ticker_symbol}...")
        try:
            ticker = yf.Ticker(ticker_symbol)

            # Get Quarterly Financials
            quarterly_fin = ticker.quarterly_financials.T
            if quarterly_fin.empty:
                print(f"    Warning: No quarterly financials found for {ticker_symbol}.")
                continue

            # Try multiple methods to get shares outstanding
            latest_shares = None
            
            # Method 1: Try from info dictionary
            try:
                info = ticker.info
                latest_shares = info.get('sharesOutstanding', None)
            except Exception as e:
                print(f"    Info method failed: {e}")
            
            # Method 2: Try from get_shares_full() method if it exists
            if latest_shares is None:
                try:
                    shares_data = ticker.get_shares_full()
                    if shares_data is not None and not shares_data.empty:
                        latest_shares = shares_data.iloc[-1]
                except AttributeError:
                    print(f"    get_shares_full() not available")
                except Exception as e:
                    print(f"    get_shares_full() failed: {e}")
            
            # Method 3: Try from fast_info
            if latest_shares is None:
                try:
                    latest_shares = ticker.fast_info.get('shares', None)
                except Exception as e:
                    print(f"    fast_info method failed: {e}")
            
            if latest_shares is None:
                print(f"    Warning: No shares outstanding data found for {ticker_symbol}. Skipping.")
                continue

            # --- Data Cleaning and Preparation ---

            # Rename columns for clarity and select key rows
            fundamentals_df = quarterly_fin.rename(columns={'Net Income': 'Net_Income'}).filter(items=['Net_Income'])
            
            # Reset index to make date a column
            fundamentals_df = fundamentals_df.reset_index()
            fundamentals_df.columns = ['Date', 'Net_Income']
            
            # Add columns for Ticker and Shares
            fundamentals_df['Ticker'] = ticker_symbol
            fundamentals_df['Shares_Outstanding'] = latest_shares

            # Drop rows with missing values
            fundamentals_df.dropna(subset=['Net_Income'], inplace=True)

            # Format Date and append to the list
            fundamentals_df['Date'] = fundamentals_df['Date'].dt.strftime('%Y-%m-%d')
            fundamentals_data.append(fundamentals_df)
            print(f"    ✅ Successfully fetched data for {ticker_symbol}")

        except Exception as e:
            print(f"    ❌ Error fetching fundamentals for {ticker_symbol}: {e}")
            continue

    if fundamentals_data:
        final_fundamentals_df = pd.concat(fundamentals_data, ignore_index=True)
        print("Fundamentals data fetching complete.")
        return final_fundamentals_df
    else:
        print("No fundamentals data was successfully fetched.")
        return pd.DataFrame()

# --- 6. SQL SAVING FUNCTION ---

def save_to_sql(df, table_name, engine, if_exists_mode='replace'):
    """
    Saves a Pandas DataFrame to the SQLite database using the SQLAlchemy engine.

    Args:
        df (pd.DataFrame): The data to save.
        table_name (str): The name of the table in the database.
        engine: The SQLAlchemy engine object.
        if_exists_mode (str): How to handle existing table ('replace' or 'append').
    """
    if df.empty:
        print(f"Skipping save for '{table_name}'. DataFrame is empty.")
        return

    print(f"Saving {len(df)} rows to SQL table: {table_name}...")
    try:
        # The core pandas function to interact with SQL
        df.to_sql(
            table_name,
            con=engine,
            if_exists=if_exists_mode,
            index=False
        )
        print(f"✅ Success: Data saved to SQL table '{table_name}'.")

    except Exception as e:
        print(f"❌ Error saving data to SQL table '{table_name}': {e}")

# --- 7. VALUATION CALCULATION FUNCTION ---

def calculate_valuation_metrics(engine):
    """
    Executes a SQL query to join price and fundamentals data,
    calculates TTM Net Income Per Share (as a proxy for FFO/Share),
    and determines the P/FFO multiple.
    """
    print("\nCalculating TTM P/FFO and valuation metrics...")

    # We use a Common Table Expression (CTE) to calculate Trailing Twelve Months (TTM) Net Income.
    # We use the latest closing price from the ticker_prices table for the numerator.
    VALUATION_QUERY = """
    WITH TTM_FINANCIALS AS (
        -- Calculate the TTM Net Income (sum of the latest four quarters)
        SELECT
            Ticker,
            Date AS Latest_Fundamental_Date,
            Shares_Outstanding,
            Net_Income,
            -- SUM the last 4 quarterly Net Incomes for TTM
            SUM(Net_Income) OVER (
                PARTITION BY Ticker
                ORDER BY Date DESC
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ) AS TTM_Net_Income,
            -- Add row number to identify the most recent record per ticker
            ROW_NUMBER() OVER (
                PARTITION BY Ticker
                ORDER BY Date DESC
            ) AS rn
        FROM
            ticker_fundamentals
    )
    SELECT
        T2.Ticker,
        T2.Date AS Latest_Price_Date,
        T2.Close_Price,
        T1.TTM_Net_Income,
        T1.Shares_Outstanding,

        -- Calculate TTM Net Income Per Share (Proxy for FFO/Share)
        (T1.TTM_Net_Income * 1.0) / T1.Shares_Outstanding AS TTM_Net_Income_Per_Share,

        -- Calculate P/FFO Multiple (Using TTM Net Income Per Share as the denominator)
        (T2.Close_Price / ((T1.TTM_Net_Income * 1.0) / T1.Shares_Outstanding)) AS P_to_FFO_Multiple

    FROM
        TTM_FINANCIALS T1
    -- Join to get the latest closing price
    INNER JOIN (
        SELECT
            Ticker,
            MAX(Date) AS Date,
            Close_Price
        FROM
            ticker_prices
        GROUP BY
            Ticker
    ) T2 ON T1.Ticker = T2.Ticker
    WHERE
        -- Filter to only include the most recent TTM calculation per ticker
        T1.rn = 1
    ORDER BY
        T2.Close_Price DESC;
    """
    # Execute the query and load results directly into a pandas DataFrame
    valuation_df = pd.read_sql(VALUATION_QUERY, con=engine)
    
    # Placeholder for Dividend Yield: yfinance dividend data can be unreliable quarterly.
    # For a production project, you would manually fetch or use a dedicated API.
    valuation_df['Dividend_Yield'] = 0.0 # Placeholder for now

    print("Valuation calculation complete.")
    return valuation_df

# --- 9. PLOTLY VISUALIZATION FUNCTION ---
import plotly.express as px
import plotly.io as pio

def create_valuation_scatter_plot(valuation_df):
    """Creates and saves an interactive scatter plot for relative valuation."""
    print("\nCreating interactive Plotly visualization...")

    # Ensure P_to_FFO_Multiple is the key metric
    valuation_df['Sector_Median_P_FFO'] = valuation_df['P_to_FFO_Multiple'].median()
    valuation_df['P_FFO_Spread'] = valuation_df['P_to_FFO_Multiple'] - valuation_df['Sector_Median_P_FFO']

    fig = px.scatter(
        valuation_df,
        x='P_to_FFO_Multiple',
        y='Dividend_Yield',
        text='Ticker',
        size='Shares_Outstanding', # Use market cap/shares to determine bubble size
        color='P_FFO_Spread', # Color code by whether they are cheap or expensive relative to median
        hover_data=['TTM_Net_Income_Per_Share', 'Close_Price'],
        title='Multifamily REIT Relative Valuation: P/FFO vs. Dividend Yield',
        labels={
            'P_to_FFO_Multiple': 'P/FFO Multiple (TTM Net Income Proxy)',
            'Dividend_Yield': 'Dividend Yield (Placeholder)'
        },
        height=600
    )

    # Add Median Line for context
    fig.add_vline(x=valuation_df['Sector_Median_P_FFO'].iloc[0], line_dash="dash", annotation_text="Sector Median P/FFO")

    # Save the chart as a standalone HTML file
    HTML_FILE_PATH = 'valuation_scatter_plot.html'
    pio.write_html(fig, file=HTML_FILE_PATH, auto_open=False)

    print(f"✅ Success: Plotly chart saved to {HTML_FILE_PATH}")
    # 
    return HTML_FILE_PATH

# --- 8. MAIN EXECUTION BLOCK ---

def main():
    print("--- Starting REIT Data Pipeline (Phase 1 & 2) ---")

    # A. Fetch the historical price data for the REITs
    prices_df = fetch_historical_prices(REIT_TICKERS)
    save_to_sql(prices_df, 'ticker_prices', engine, if_exists_mode='replace')

    # B. Fetch the financial fundamentals
    fundamentals_df = fetch_fundamentals(REIT_TICKERS)
    save_to_sql(fundamentals_df, 'ticker_fundamentals', engine, if_exists_mode='replace')

    print("--- Phase 2: Data Pipeline Complete ---")

    # C. Calculate Valuation Metrics
    valuation_df = calculate_valuation_metrics(engine)
    save_to_sql(valuation_df, 'final_valuation_data', engine, if_exists_mode='replace')

    print("--- Phase 3: Valuation Calculation Complete ---")

    # D. Create Visualization (NEW STEP)
    valuation_df = pd.read_sql("SELECT * FROM final_valuation_data", con=engine)
    chart_path = create_valuation_scatter_plot(valuation_df)
    
    # You can now upload this .html file to your GitHub Pages website.

    print("--- Phase 4: Visualization Complete ---")

# Run the main function when the script is executed
if __name__ == "__main__":
    main()