import os
import csv
from time import sleep
from dotenv import load_dotenv
from logs import logger
from scraping.scraper_dt import ScrapingDilutionTracker
load_dotenv()

DEBUG = os.getenv("DEBUG") == "True"
DEBUG_TRICKERS = int(os.getenv("DEBUG_TRICKERS"))
CHROME_FOLDER = os.getenv('CHROME_FOLDER')

def main ():
    
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
                
        logger.info (f">>> Scraping {tricker_name}...")
        
        # Load and get main data 
        scraper.load_company(tricker_key)
        logger.info("scraping premarket data...")
        premarket_data = scraper.get_premarket_data()
        
        # Validate data found
        if not premarket_data["found"]:
            logger.info (f"\t{premarket_data["dilution_data"]}")
            continue
            
        # Scraper secondary data
        logger.info("scraping historical data...")
        historical_data = scraper.get_historical_data()
        
        logger.info("scraping cash data...")
        cash_data = scraper.get_cash_data()
        
        logger.info("scraping extra data...")
        extra_data = scraper.get_extra_data()
        
        logger.info("scraping complete offering data...")
        complete_offering_data = scraper.get_complete_offering_data()
        
        logger.info("scraping news data...")
        news_data = scraper.get_news_data()
        
        logger.info("scraping holders data...")
        holders_data = scraper.get_holders_data()
        
        # TODO: save data in database
        
        # End in debug mode
        if DEBUG and DEBUG_TRICKERS == tricker_num:
            logger.info ("Debug mode: ending...")
            break
        
        # Wait before next tricker
        logger.info ("waiting 60 seconds...")
        sleep (60)

if __name__ == '__main__':
    main()