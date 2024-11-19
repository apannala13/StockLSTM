import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

def stock_ingestion(ticker_list):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1825) # 5 years
    
    stock_data = {}
    
    for ticker in ticker_list:
        try:
            stock = yf.Ticker(ticker)
            hist_data = stock.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval='1d'
            )
            hist_data['Ticker'] = ticker
            
            stock_data[ticker] = hist_data
        except Exception as e:
            print(f'could not retrieve info for {ticker}: {str(e)}')
    
    combined_data = pd.concat(stock_data.values())
    combined_data.reset_index(inplace=True)
    combined_data = combined_data[['Ticker', 'Date'] + [col for col in combined_data.columns if col not in ['Ticker', 'Date']]]
    
    
    return combined_data
    
ticker_list = ['MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'JPM', 'V', 'MA', 'BAC', 
               'GS', 'JNJ', 'UNH', 'PFE', 'ABBV', 'WMT', 'PG','KO', 'PEP', 'CAT', 'BA', 'HON', 'DIS', 
               'NFLX', 'INTC', 'AMD', 'QCOM', 'XOM', 'CVX']
stock_data = stock_ingestion(ticker_list)
