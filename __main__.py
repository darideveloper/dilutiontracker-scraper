import os
import csv
from dotenv import load_dotenv
from logs import logger
from scraping.scraper_dt import ScrapingDilutionTracker
from database.db import Database
load_dotenv()

DEBUG = os.getenv("DEBUG") == "True"
DEBUG_TRICKERS = int(os.getenv("DEBUG_TRICKERS"))
CHROME_FOLDER = os.getenv('CHROME_FOLDER')

def main ():
    
    # Connect to database
    database = Database()
    
    # Validate chrome folder
    if CHROME_FOLDER is None or not os.path.isdir(CHROME_FOLDER):
        logger.error('CHROME_FOLDER not found env variable is not set')
        quit()
    
    # Quit if csv not found
    current_path = os.path.dirname(__file__)
    csv_path = os.path.join(current_path, 'tickers.csv')
    if not os.path.isfile(csv_path):
        logger.error('tickers.csv not found. Create a tickers.csv file with the tickers to scrape (alias, key)')
        quit()
    
    # Get tickers from csv
    with open (csv_path, 'r') as file:
        reader = csv.reader(file)
        tickers = list(reader)
        
    # Connect to dilution tracker
    scraper = ScrapingDilutionTracker(CHROME_FOLDER)
    
    # End if login failed
    is_logged = scraper.login()
    if is_logged:
        logger.info('Login success')
    else:
        logger.error('Login failed. Close the program, open chrome, login manually and try again')
        quit ()
    
    tricker_num = 0
    for tricker_name, tricker_key in tickers:
        
        tricker_num += 1
                
        logger.info (f"\n>>> Scraping {tricker_name}...")
        
        # Load and get main data 
        scraper.load_company(tricker_key)
        logger.info("scraping premarket data...")
        premarket_data = scraper.get_premarket_data()
        
        # Validate data found
        if not premarket_data["found"]:
            logger.info (f"\t* {premarket_data['dilution_data']}")
            continue
        
        database.save_premarket_data (premarket_data)
            
        # Scraper secondary data
        logger.info("scraping historical data...")
        historical_data = scraper.get_historical_data()
        database.save_historical_data (historical_data)
        
        logger.info("scraping cash data...")
        cash_data = scraper.get_cash_data()
        database.save_cash_data (cash_data)
        
        logger.info("scraping extra data...")
        extra_data = scraper.get_extra_data()
        database.save_extra_data (extra_data)
        
        # logger.info("scraping complete offering data...")
        # completed_offering_data = scraper.get_completed_offering_data()
        # database.save_completed_offering_data (completed_offering_data)
        
        # logger.info("scraping news data...")
        # news_data = scraper.get_news_data()
        # database.save_news_data (news_data)
        
        # logger.info("scraping holders data...")
        # holders_data = scraper.get_holders_data()
        # database.save_holders_data (holders_data)
        
        logger.info ("scraping filings data...")
        filings_data = scraper.get_filings_data()
        database.save_filings_data (filings_data)
        
        # Extract no complant data
        logger.info ("Scraping noncompliant data...\n")
        noncompliant_data = scraper.get_noncompliant_data (tricker_key.lower().strip())
        database.save_noncompliant_data (noncompliant_data)
                
        # End in debug mode
        if DEBUG and DEBUG_TRICKERS == tricker_num:
            logger.info ("Debug mode: ending...")
            break

if __name__ == '__main__':
    main()