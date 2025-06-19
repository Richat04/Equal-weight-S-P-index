import pandas as pd
import numpy as np
import yfinance as yf
import requests
from scipy import stats
import xlsxwriter
import math
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Get S&P 500 stocks from Wikipedia
def get_sp500_stocks():
    """
    Fetch current S&P 500 stock symbols from Wikipedia
    """
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    html = pd.read_html(url)
    sp500_stocks = html[0]
    sp500_stocks = sp500_stocks[sp500_stocks['Symbol'].notna()]
    
    # Clean up symbols for Yahoo Finance
    stocks = sp500_stocks['Symbol'].str.replace('.', '-').tolist()
    
    print(f"Retrieved {len(stocks)} S&P 500 stocks")
    return stocks

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_stock_data(symbols, period='1y'):
    """
    Download stock data for given symbols using yfinance with robust error handling
    """
    print(f"Downloading data for {len(symbols)} stocks...")
    
    stock_data = {}
    failed_symbols = []
    
    # Try downloading in smaller chunks first
    chunk_size = 50  # Smaller chunks for better reliability
    symbol_chunks = list(chunks(symbols, chunk_size))
    
    for i, symbol_chunk in enumerate(symbol_chunks):
        print(f"Processing chunk {i+1}/{len(symbol_chunks)} ({len(symbol_chunk)} symbols)")
        
        # Try batch download first
        try:
            symbol_string = ' '.join(symbol_chunk)
            data = yf.download(symbol_string, period=period, group_by='ticker', 
                             progress=False, threads=True)
            
            if not data.empty:
                if len(symbol_chunk) == 1:
                    # Single symbol
                    symbol = symbol_chunk[0]
                    if 'Adj Close' in data.columns:
                        price = data['Adj Close'].iloc[-1]
                        if not pd.isna(price) and price > 0:
                            stock_data[symbol] = float(price)
                            print(f"  ✓ {symbol}: ${price:.2f}")
                else:
                    # Multiple symbols
                    for symbol in symbol_chunk:
                        try:
                            if hasattr(data.columns, 'levels') and len(data.columns.levels) > 0:
                                if symbol in data.columns.levels[0]:
                                    price = data[symbol]['Adj Close'].iloc[-1]
                                    if not pd.isna(price) and price > 0:
                                        stock_data[symbol] = float(price)
                                        print(f"  ✓ {symbol}: ${price:.2f}")
                                    else:
                                        failed_symbols.append(symbol)
                                else:
                                    failed_symbols.append(symbol)
                            else:
                                failed_symbols.append(symbol)
                        except Exception as e:
                            print(f"  ✗ {symbol}: Error - {str(e)[:50]}")
                            failed_symbols.append(symbol)
            else:
                print(f"  ✗ Chunk {i+1}: No data returned")
                failed_symbols.extend(symbol_chunk)
                
        except Exception as e:
            print(f"  ✗ Chunk {i+1}: Batch download failed - {str(e)[:100]}")
            failed_symbols.extend(symbol_chunk)
    
    # Retry failed symbols individually
    if failed_symbols and len(stock_data) < 50:  # Only retry if we have very few successful downloads
        print(f"\nRetrying {len(failed_symbols)} failed symbols individually...")
        
        for symbol in failed_symbols[:20]:  # Limit retries to avoid long waits
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period='5d')
                if not hist.empty:
                    price = hist['Close'].iloc[-1]
                    if not pd.isna(price) and price > 0:
                        stock_data[symbol] = float(price)
                        print(f"  ✓ Retry {symbol}: ${price:.2f}")
            except Exception as e:
                print(f"  ✗ Retry {symbol}: {str(e)[:50]}")
                continue
    
    print(f"\nDownload Summary:")
    print(f"  Successfully retrieved: {len(stock_data)} stocks")
    print(f"  Failed: {len(symbols) - len(stock_data)} stocks")
    
    # If we still have very few stocks, use a fallback sample
    if len(stock_data) < 10:
        print("\n⚠️  Very few stocks downloaded. Using fallback sample...")
        stock_data = get_fallback_stock_data()
    
    return stock_data

def get_fallback_stock_data():
    """
    Fallback function with major S&P 500 stocks for demo purposes
    """
    major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 
                   'UNH', 'JNJ', 'V', 'PG', 'JPM', 'MA', 'HD', 'CVX', 'ABBV', 'PFE',
                   'KO', 'PEP', 'AVGO', 'TMO', 'COST', 'WMT', 'DIS', 'ACN', 'VZ', 'ADBE',
                   'NFLX', 'CRM', 'NKE', 'MRK', 'ABT', 'TXN', 'QCOM', 'DHR', 'RTX', 'HON']
    
    fallback_data = {}
    print("Attempting to download fallback stocks individually...")
    
    for symbol in major_stocks:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period='5d')
            if not hist.empty:
                price = hist['Close'].iloc[-1]
                if not pd.isna(price) and price > 0:
                    fallback_data[symbol] = float(price)
                    print(f"  ✓ {symbol}: ${price:.2f}")
            
            # Stop if we get enough for demo
            if len(fallback_data) >= 20:
                break
                
        except Exception as e:
            print(f"  ✗ {symbol}: {str(e)[:30]}")
            continue
    
    if len(fallback_data) == 0:
        # Last resort: use mock data for demo
        print("\n⚠️  All downloads failed. Using mock data for demonstration...")
        mock_prices = [150, 300, 2800, 3200, 800, 250, 450, 400, 500, 160,
                      220, 140, 180, 350, 320, 120, 90, 45, 65, 75]
        for i, symbol in enumerate(major_stocks[:20]):
            fallback_data[symbol] = mock_prices[i]
            print(f"  📊 {symbol}: ${mock_prices[i]:.2f} (mock)")
    
    return fallback_data

def calculate_equal_weight_positions(stock_data, portfolio_value):
    """
    Calculate equal weight positions for each stock
    """
    if not stock_data:
        print("❌ Error: No stock data available. Cannot calculate positions.")
        return []
    
    num_stocks = len(stock_data)
    print(f"Calculating positions for {num_stocks} stocks with ${portfolio_value:,} portfolio")
    
    if num_stocks == 0:
        print("❌ Error: Number of stocks is zero. Check data download.")
        return []
    
    position_size = portfolio_value / num_stocks
    print(f"Target position size per stock: ${position_size:,.2f}")
    
    positions = []
    total_invested = 0
    
    for symbol, price in stock_data.items():
        if price <= 0:
            print(f"⚠️  Skipping {symbol}: Invalid price {price}")
            continue
            
        shares_to_buy = math.floor(position_size / price)
        position_value = shares_to_buy * price
        total_invested += position_value
        
        positions.append({
            'Ticker': symbol,
            'Stock Price': round(price, 2),
            'Market Cap': 'N/A',  # We'll add this if needed
            'Shares to Buy': shares_to_buy,
            'Position Value': round(position_value, 2)
        })
    
    print(f"Total positions created: {len(positions)}")
    print(f"Total invested: ${total_invested:,.2f}")
    print(f"Cash remaining: ${portfolio_value - total_invested:,.2f}")
    
    return positions

def get_market_cap_data(symbols):
    """
    Get market cap data for stocks (simplified version)
    """
    market_caps = {}
    
    # Process in smaller chunks for market cap data
    for chunk in chunks(symbols, 50):
        try:
            tickers = yf.Tickers(' '.join(chunk))
            for symbol in chunk:
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    market_cap = info.get('marketCap', 0)
                    if market_cap:
                        market_caps[symbol] = market_cap
                except:
                    continue
        except:
            continue
    
    return market_caps

def create_equal_weight_portfolio(portfolio_value=1000000):
    """
    Main function to create equal weight S&P 500 portfolio
    """
    print("=== S&P 500 Equal Weight Index Fund Builder ===\n")
    
    # Step 1: Get S&P 500 stocks
    print("Step 1: Fetching S&P 500 stock list...")
    try:
        sp500_stocks = get_sp500_stocks()
        print(f"✓ Retrieved {len(sp500_stocks)} S&P 500 symbols")
    except Exception as e:
        print(f"❌ Error fetching S&P 500 list: {e}")
        print("Using fallback major stocks list...")
        sp500_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 
                       'UNH', 'JNJ', 'V', 'PG', 'JPM', 'MA', 'HD', 'CVX', 'ABBV', 'PFE',
                       'KO', 'PEP', 'AVGO', 'TMO', 'COST', 'WMT', 'DIS', 'ACN', 'VZ', 'ADBE',
                       'NFLX', 'CRM', 'NKE', 'MRK', 'ABT', 'TXN', 'QCOM', 'DHR', 'RTX', 'HON']
    
    # Step 2: Get current stock prices
    print(f"\nStep 2: Getting current stock prices...")
    stock_data = get_stock_data(sp500_stocks, period='5d')  # Get recent data
    
    # Validate stock data
    if not stock_data:
        print("❌ No stock data retrieved. Cannot proceed.")
        return pd.DataFrame()
    
    # Filter out stocks without valid data
    valid_stocks = {k: v for k, v in stock_data.items() if v > 0}
    print(f"✓ Valid stocks with price data: {len(valid_stocks)}")
    
    if len(valid_stocks) == 0:
        print("❌ No valid stock prices found. Cannot create portfolio.")
        return pd.DataFrame()
    
    # Step 3: Calculate equal weight positions
    print(f"\nStep 3: Calculating equal weight positions for ${portfolio_value:,} portfolio...")
    positions = calculate_equal_weight_positions(valid_stocks, portfolio_value)
    
    if not positions:
        print("❌ No positions calculated. Cannot create portfolio.")
        return pd.DataFrame()
    
    # Step 4: Create DataFrame
    df = pd.DataFrame(positions)
    df = df.sort_values('Ticker')
    df = df.reset_index(drop=True)
    
    # Add some analytics
    total_position_value = df['Position Value'].sum()
    avg_position_size = df['Position Value'].mean()
    
    print(f"\n=== Portfolio Summary ===")
    print(f"Target Portfolio Value: ${portfolio_value:,}")
    print(f"Total Position Value: ${total_position_value:,.2f}")
    print(f"Cash Remaining: ${portfolio_value - total_position_value:,.2f}")
    print(f"Number of Positions: {len(df)}")
    print(f"Average Position Size: ${avg_position_size:,.2f}")
    print(f"Target Position Size: ${portfolio_value/len(df):,.2f}")
    
    return df

def save_to_excel(df, filename='sp500_equal_weight_portfolio.xlsx'):
    """
    Save portfolio to Excel with formatting
    """
    print(f"\nSaving portfolio to {filename}...")
    
    # Create a Pandas Excel writer using XlsxWriter
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    
    # Write the dataframe to Excel
    df.to_excel(writer, sheet_name='Equal Weight Portfolio', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['Equal Weight Portfolio']
    
    # Add formats
    money_format = workbook.add_format({'num_format': '$#,##0.00'})
    percent_format = workbook.add_format({'num_format': '0.00%'})
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#D7E4BC',
        'border': 1
    })
    
    # Format columns
    worksheet.set_column('A:A', 12)  # Ticker
    worksheet.set_column('B:B', 15, money_format)  # Stock Price
    worksheet.set_column('C:C', 20)  # Market Cap
    worksheet.set_column('D:D', 15)  # Shares to Buy
    worksheet.set_column('E:E', 18, money_format)  # Position Value
    
    # Write headers with formatting
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
    
    writer.close()
    print(f"Portfolio saved to {filename}")

def analyze_portfolio_performance(df):
    """
    Analyze the equal weight portfolio performance characteristics
    """
    print("\n=== Portfolio Analysis ===")
    
    # Basic statistics
    total_positions = len(df)
    total_value = df['Position Value'].sum()
    
    # Position size distribution
    position_sizes = df['Position Value']
    
    print(f"Portfolio Statistics:")
    print(f"- Total Positions: {total_positions}")
    print(f"- Total Portfolio Value: ${total_value:,.2f}")
    print(f"- Average Position Size: ${position_sizes.mean():,.2f}")
    print(f"- Position Size Std Dev: ${position_sizes.std():,.2f}")
    print(f"- Min Position Size: ${position_sizes.min():,.2f}")
    print(f"- Max Position Size: ${position_sizes.max():,.2f}")
    
    # Calculate position weights
    df['Weight'] = df['Position Value'] / total_value
    target_weight = 1.0 / total_positions
    
    print(f"\nWeight Analysis:")
    print(f"- Target Weight per Stock: {target_weight:.4f} ({target_weight*100:.2f}%)")
    print(f"- Actual Weight Range: {df['Weight'].min():.4f} to {df['Weight'].max():.4f}")
    print(f"- Weight Std Dev: {df['Weight'].std():.6f}")
    
    return df

def rebalance_portfolio(df, new_portfolio_value):
    """
    Rebalance portfolio to equal weights with new portfolio value
    """
    print(f"\n=== Rebalancing Portfolio to ${new_portfolio_value:,} ===")
    
    # Get current prices (in real implementation, you'd fetch new prices)
    current_stocks = df['Ticker'].tolist()
    
    # For demo, we'll use the same prices (in practice, fetch new data)
    stock_data = dict(zip(df['Ticker'], df['Stock Price']))
    
    # Recalculate positions
    new_positions = calculate_equal_weight_positions(stock_data, new_portfolio_value)
    new_df = pd.DataFrame(new_positions)
    new_df = new_df.sort_values('Ticker').reset_index(drop=True)
    
    # Calculate changes
    old_shares = dict(zip(df['Ticker'], df['Shares to Buy']))
    new_df['Old Shares'] = new_df['Ticker'].map(old_shares)
    new_df['Share Change'] = new_df['Shares to Buy'] - new_df['Old Shares']
    new_df['Action'] = new_df['Share Change'].apply(
        lambda x: 'BUY' if x > 0 else 'SELL' if x < 0 else 'HOLD'
    )
    
    # Summary of rebalancing
    buy_orders = new_df[new_df['Share Change'] > 0]
    sell_orders = new_df[new_df['Share Change'] < 0]
    
    print(f"Rebalancing Summary:")
    print(f"- Buy orders: {len(buy_orders)}")
    print(f"- Sell orders: {len(sell_orders)}")
    print(f"- Hold positions: {len(new_df) - len(buy_orders) - len(sell_orders)}")
    
    return new_df

def main():
    """
    Main execution function
    """
    # Create equal weight portfolio
    portfolio_value = 1000000  # $1 million portfolio
    
    # Build the portfolio
    portfolio_df = create_equal_weight_portfolio(portfolio_value)
    
    # Analyze the portfolio
    analyzed_df = analyze_portfolio_performance(portfolio_df)
    
    # Save to Excel
    save_to_excel(analyzed_df)
    
    # Display top 10 positions
    print(f"\n=== Top 10 Positions ===")
    print(analyzed_df[['Ticker', 'Stock Price', 'Shares to Buy', 'Position Value', 'Weight']].head(10).to_string(index=False))
    
    # Demonstrate rebalancing (optional)
    print(f"\n=== Rebalancing Demo ===")
    new_portfolio_value = 1200000  # Portfolio grew to $1.2M
    rebalanced_df = rebalance_portfolio(analyzed_df, new_portfolio_value)
    
    # Show rebalancing trades
    trades = rebalanced_df[rebalanced_df['Share Change'] != 0][['Ticker', 'Action', 'Share Change', 'Stock Price']]
    if not trades.empty:
        print(f"\nRequired Trades (showing first 10):")
        print(trades.head(10).to_string(index=False))
    
    return analyzed_df

if __name__ == "__main__":
    # Run the equal weight portfolio builder
    portfolio = main()
    
    print(f"\n=== Equal Weight S&P 500 Index Fund Complete ===")
    print(f"Portfolio has been created and saved to Excel file.")
    print(f"This represents a true equal-weight approach where each stock")
    print(f"gets the same dollar allocation regardless of market cap.")