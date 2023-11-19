import os
import csv
from dotenv import load_dotenv
from logs import logger
from scraper import ScrapingDilutionTracker
load_dotenv()

DEBUG = os.getenv("DEBUG") == "True"
DEBUG_TRICKERS = int(os.getenv("DEBUG_TRICKERS"))

def main ():
    
    # Get tickers from csv
    current_path = os.path.dirname(__file__)
    csv_path = os.path.join(current_path, 'tickers.csv')
    with open (csv_path, 'r') as file:
        reader = csv.reader(file)
        tickers = list(reader)
        
    # Scraper data
    scraper = ScrapingDilutionTracker()
    tricker_num = 0
    for tricker_name, tricker_key in tickers[1:]:
        
        tricker_num += 1
                
        logger.info (f"Scraping {tricker_name}...")
        
        # TODO: CATCH ERRORS
        
        # Load and get main data 
        scraper.login()
        scraper.load_company(tricker_key)
        premarket_data = scraper.get_premarket_data()
        
        # Validate data found
        if not premarket_data["found"]:
            logger.info (f"\t{premarket_data["dilution_data"]}")
            continue
            
        # Scraper secondary data
        historical_data = scraper.get_historical_data()
        cash_data = scraper.get_cash_data()
        
        # TODO: save data in database
        
        # End in debug mode
        if DEBUG and DEBUG_TRICKERS == tricker_num:
            logger.info ("Debug mode: ending...")
            break

if __name__ == '__main__':
    main()