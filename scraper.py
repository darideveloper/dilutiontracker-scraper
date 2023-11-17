import os
from dotenv import load_dotenv
from logs import logger
from scraping.web_scraping import WebScraping
from selenium.webdriver.common.by import By

# read env variables
load_dotenv()
CHROME_FOLDER = os.getenv('CHROME_FOLDER')


class ScrapingDilutionTracker (WebScraping):

    def __init__(self):

        # Scraping pages
        self.pages = {
            "home": "https://dilutiontracker.com"
        }

        # Validate env variables
        if CHROME_FOLDER is None:
            logger.error('CHROME_FOLDER env variable is not set')
            quit()

        # Start chrome instance with chrome data
        super().__init__(
            chrome_folder=CHROME_FOLDER,
            start_killing=True,
        )

    def login(self):
        """ Validate correct login and go to app page """

        selectors = {
            "btn_app": 'nav .btn-deepred',
        }

        error_login = "Login failed. Close the program, open chrome, login manually and try again"

        # Load home page
        self.set_page(self.pages["home"])
        self.refresh_selenium()

        # Validte if exists "go to app" button
        button_text = self.get_text(selectors["btn_app"]).lower().strip()
        if button_text != "go to app":
            logger.error(error_login)
            quit()

        # Click on "go to app" button
        old_page = self.driver.current_url
        self.click(selectors["btn_app"])

        # Validate page change
        current_page = self.driver.current_url
        if old_page == current_page:
            logger.error(error_login)
            quit()

        logger.info("Login successful")

    def load_company (self, company:str):
        """ Load company page

        Args:
            company (str): company ticker
        """

        url = f"{self.pages["home"]}/app/search/{company}"
        self.set_page(url)
        self.refresh_selenium()

    def get_premarket_data(self) -> dict:
        """ Get premarket data from dilution tracker

        Returns:
            dict: premarket data
            Structure:
                {
                    name: str,
                    sector: str,
                    industry: str,
                    mkt_cap: float,
                    float_cap: float,
                    est_cash_sh: float,
                    t25_inst_own: float,
                    si: float,
                    description_company: str,
                    dilution_data: str,
                    overall_risk: str,
                    offering_abillity: str,
                    dilution_amt_ex_shelf: str,
                    historical: str
                    cash_need: str,
                    out_take: str,
                    update_info: str,                    
                }
        """
        
        logger.info ("scraping premarket data...")
        
        selectors = {
            "name": 'h1',
            "header": {
                "wrapper_texts": ".mw-1010:nth-child(1) .cursor-default > div",
                "wrapper_counters": ".mw-1010:nth-child(2) .cursor-default > div",
                "info": "> span",
            },
            "description": {
                "show_more_btn":  '#showMoreBtn',
                "info": '#companyDesc > div'
            },
            "adjectives": {
                "wrappers": '.dilutionRatingSingleWrapper',
                "name": "> span:first-child",
                "info": "> span:last-child",
            },
            "our_take": {
                "wrappers": '.ourTakeSingleContainer',
                "datetime": "span:first-child",
                "info": "span:last-child",
            },
            "update_info": "#results-os-chart > p:nth-child(2)"
        }
        data = {}
        
        # Get company name
        data["name"] = self.get_text(selectors["name"])
        
        # Get header texts
        headers_text_num = len(self.get_elems(selectors["header"]["wrapper_texts"]))
        for header_index in range (headers_text_num):
            
            selector_header = f"{selectors["header"]["wrapper_texts"]}:nth-child({header_index+1})"
            selector_texts = f"{selector_header} {selectors["header"]["info"]}"
            texts = self.get_elems(selector_texts)
            
            key = texts[0].text.lower()
            info = texts[1].text.lower()
            
            if "sector" in key:
                data["sector"] = info
            elif "industry" in key:
                data["industry"] = info
        
        # Get headers counters
        headers_counters_num = len(self.get_elems(selectors["header"]["wrapper_counters"]))
        for header_index in range (headers_counters_num):
            
            selector_header = f"{selectors["header"]["wrapper_counters"]}:nth-child({header_index+2})"
            selector_counters = f"{selector_header} {selectors["header"]["info"]}"
            counters = self.get_elems(selector_counters)
            
            key = counters[0].text.lower()
            info = counters[1].text.lower()
            
            if "mkt cap" in key:
                data["mkt_cap"] = info.split("m")[0]
            elif "float" in key:
                data["float_cap"] = info.split("m")[0]
            elif "est" in key:
                data["est_cash_sh"] = info
            elif "t25" in key:
                data["t25_inst_own"] = info.replace("%", "")
            elif "si" in key:
                data["si"] = info.replace("%", "")      
        
        # Company description
        self.click(selectors["description"]["show_more_btn"])
        self.refresh_selenium ()
        data["description_company"] = self.get_text(selectors["description"]["info"])
        
        # TODO: dilution data 
        
        # Adjectives
        adjectives_num = len(self.get_elems(selectors["adjectives"]["wrappers"]))
        for adjective_index in range (adjectives_num):
            
            selector_adjective = f"{selectors["adjectives"]["wrappers"]}:nth-child({adjective_index+1})"
            selector_name = f"{selector_adjective} {selectors["adjectives"]["name"]}"
            selector_info = f"{selector_adjective} {selectors["adjectives"]["info"]}"
            name = self.get_text(selector_name).lower()
            info = self.get_text(selector_info).lower()
            
            if "overall risk" in name:
                data["overall_risk"] = info
            elif "offering ability" in name:
                data["offering_abillity"] = info
            elif "overhead supply" in name:
                data["dilution_amt_ex_shelf"] = info
            elif "historical" in name:
                data["historical"] = info
            elif "cash need" in name:
                data["cash_need"] = info
                
        # Our take
        our_take_lines = ""
        our_take_num = len(self.get_elems(selectors["our_take"]["wrappers"]))
        for our_take_index in range (our_take_num):
            
            selector_our_take = f"{selectors["our_take"]["wrappers"]}:nth-child({our_take_index+2})"
            selector_datetime = f"{selector_our_take} {selectors["our_take"]["datetime"]}"
            selector_info = f"{selector_our_take} {selectors["our_take"]["info"]}"
            
            datetime = self.get_text(selector_datetime).lower()
            info = self.get_text(selector_info).lower()
            
            line = f"{datetime}  {info}"
            our_take_lines += f"{line}\n"
            
        data["out_take"] = our_take_lines.strip()
        
        # Update info
        data["update_info"] = self.get_text(selectors["update_info"])
         
        return data


if __name__ == "__main__":
    # Start scraping (main worlflow)
    scraping_dilution_tracker = ScrapingDilutionTracker()
    scraping_dilution_tracker.login()
    scraping_dilution_tracker.load_company("GMBL?a=3kxbzw")
    data = scraping_dilution_tracker.get_premarket_data()
