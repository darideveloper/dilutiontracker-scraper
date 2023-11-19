import re
from datetime import datetime as dt
from scraping.web_scraping import WebScraping

class ScrapingDilutionTracker (WebScraping):

    def __init__(self, chrome_folder:str):
        """ Connect to WebScraping class and start chrome instance

        Args:
            chrome_folder (str): chrome data folder path
        """

        # Scraping pages
        self.pages = {
            "home": "https://dilutiontracker.com"
        }
        
        # Start chrome instance with chrome data
        super().__init__(
            chrome_folder=chrome_folder,
            start_killing=True,
        )
        
    def __get_column_value__ (self, column_height:float, graph_height:int, max_value:float ) -> float:
        """ Get column value from graph

        Args:
            column_height (float): column height in px
            graph_height (int): height of graph in px
            max_value (float): max value of graph

        Returns:
            float: column value
        """

        # Calculate column value (with 20 decimals)
        value = (column_height * max_value / graph_height)
        value = value * 100
        value = round(value, 2)
        value = value / 100
        
        return value 

    def __get_columns_data__ (self, selector_columns_wrapper:str, selector_column:str,
                             graph_height:int, max_value:float) -> list:
        """ Get regular columns data from graph

        Args:
            selector_columns_wrapper (str): columns weapper
            selector_columns (str): each column
            graph_height (int): height of graph in px
            max_value (float): max value of graph

        Returns:
            dict: columns data
            
            Structure:
                [
                    {
                        "id": int,
                        "date": datetime,
                        "hos": float,
                    },
                    ...
                ]
        """

        # Loop each column
        columns_num = len(self.get_elems(selector_columns_wrapper))
        columns_data = []
        for column_index in range(columns_num):

            selector_current_column = f"{selector_columns_wrapper}:nth-child({column_index+1}) {selector_column}"

            # Skip empty columns
            column = self.get_elems(selector_current_column)
            if not column:
                continue

            # Get data from current column
            column_name = self.get_attrib(selector_current_column, "name")
            column_height = float(self.get_attrib(selector_current_column, "height"))   

            column_value = self.__get_column_value__ (column_height, graph_height, max_value)

            # Format date and detect when columns ends
            last_column = False
            try:
                date = dt.strptime(column_name, "%m/%d/%Y")
            except:
                date = dt.now()
                last_column = True

            # Save column data
            columns_data.append({
                "id": column_index,
                "date": date,
                "hos": column_value,
            })
            
            if last_column:
                break

        return columns_data

    def login(self):
        """ Validate correct login and go to app page """

        selectors = {
            "btn_app": 'nav .btn-deepred',
        }

        # Load home page
        self.set_page(self.pages["home"])
        self.refresh_selenium()

        # Validte if exists "go to app" button
        button_text = self.get_text(selectors["btn_app"]).lower().strip()
        if button_text != "go to app":
            return False

        # Click on "go to app" button
        old_page = self.driver.current_url
        self.click(selectors["btn_app"])

        # Validate page change
        current_page = self.driver.current_url
        if old_page == current_page:
            return False

        return True

    def load_company(self, company: str):
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
                    found: bool,
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
        
        selectors = {
            "not_found": '#filingNotInCoverageIcon + div',
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
                "info": "span:nth-child(2)",
            },
            "update_info": "#results-os-chart > p:nth-child(2)"
        }
        
        # Initial data
        data = {
            "found": True,
            "dilution_data": None,
            "name": None,
            "sector": None,
            "industry": None,
            "mkt_cap": None,
            "float_cap": None,
            "est_cash_sh": None,
            "t25_inst_own": None,
            "si": None,
            "description_company": None,
            "overall_risk": None,
            "offering_abillity": None,
            "dilution_amt_ex_shelf": None,
            "historical": None,
            "cash_need": None,
            "out_take": None,
            "update_info": None,
        }

        # Validate not found data and save in dilution_data
        not_found = self.get_text(selectors["not_found"])
        if not_found:
            data["dilution_data"] = not_found
            data["found"] = False   
            return data

        # Get company name
        data["name"] = self.get_text(selectors["name"])

        # Get header texts
        headers_text_num = len(self.get_elems(
            selectors["header"]["wrapper_texts"]))
        for header_index in range(headers_text_num):

            # Get key and info
            selector_header = f"{
                selectors["header"]["wrapper_texts"]}:nth-child({header_index+1})"
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
        for header_index in range(headers_counters_num):

            # Get keys and info
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
        self.refresh_selenium()
        data["description_company"] = self.get_text(
            selectors["description"]["info"])

        # Adjectives
        adjectives_num = len(self.get_elems(
            selectors["adjectives"]["wrappers"]))
        for adjective_index in range(adjectives_num):

            # Get each adjective and category
            selector_adjective = f"{
                selectors["adjectives"]["wrappers"]}:nth-child({adjective_index+1})"
            selector_name = f"{selector_adjective} {
                selectors["adjectives"]["name"]}"
            selector_info = f"{selector_adjective} {
                selectors["adjectives"]["info"]}"
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
        for our_take_index in range(our_take_num):

            # Get each line of our take
            selector_our_take = f"{
                selectors["our_take"]["wrappers"]}:nth-child({our_take_index+2})"
            selector_datetime = f"{selector_our_take} {selectors["our_take"]["datetime"]}"
            selector_info = f"{selector_our_take} {selectors["our_take"]["info"]}"

            datetime = self.get_text(selector_datetime).lower()
            info = self.get_text(selector_info).lower()

            # Save as text
            line = f"{datetime}  {info}"
            our_take_lines += f"{line}\n"

        data["out_take"] = our_take_lines.strip()

        # Update info
        data["update_info"] = self.get_text(selectors["update_info"])

        return data

    def get_historical_data(self) -> list:
        """ Get historical from main columns in graph

        Returns:
            list: historical data
            Structure:
            {
                columns_data:   [
                    {
                        "id": int,
                        "date": datetime,
                        "hos": float,
                    },
                ].
                atm: float,
                warrant: float,
                convertible_preferred: float,
                convertible_note: float,
                equality_line: float,
                s1_offering: float,
            }

        """
        
        selectors = {
            "columns_wrapper": '#results-os-chart .recharts-bar-rectangles .recharts-bar-rectangle',
            "column": 'path',
            "height": '.yAxis .recharts-cartesian-axis-tick:last-child > text',
            "max_value": '.yAxis .recharts-cartesian-axis-tick:last-child > text tspan',
            "extra_columns": '#results-os-chart path[name="Fully Diluted"]'
        }
        
        columns_colors = {
            "#2CA1CF": "atm",
            "#8CD2E8": "warrant",
            "#FFD876": "convertible_preferred",
            "#FFC107": "convertible_note",
            "#BCC0C4": "equality_line",
            "#D1D5D8": "s1_offering",
        }
        
        # Start with empty extra columns
        data = {}
        for column_name in columns_colors.values():
            data[column_name] = None
        
        # Get graph info
        graph_height = int(self.get_attrib(selectors["height"], "height"))
        max_value = float(self.get_text(selectors["max_value"]))

        # Data from regylar columns
        data["columns_data"] = self.__get_columns_data__(
            selectors["columns_wrapper"],
            selectors["column"],
            graph_height,
            max_value,
        )
    
        # Data frome extra columns
        extra_columns = self.get_elems(selectors["extra_columns"])
        for column in extra_columns:
            
            # Get color
            column_color = column.get_attribute("fill")
            column_height = float(column.get_attribute("height"))
            
            # Get column value
            column_value = self.__get_column_value__ (column_height, graph_height, max_value)
            
            # Identify name with color
            column_name = columns_colors.get(column_color, None)
            if not column_name:
                continue
            
            # Save column data
            data[column_name] = column_value
            

        return data 

    def get_cash_data(self) -> list:
        """ Get cash from main columns in graph

        Returns:
            list: historical data
            Structure:
            {
                columns_data:   [
                    {
                        "id": int,
                        "date": datetime,
                        "hos": float,
                    },
                    ...
                ]
            }

        """
        
        selectors = {}
        selectors["graph"] = '.results-cash-bar-chart' # graph wrapper
        selectors["columns_wrapper"] = f'{selectors["graph"]} .yAxis + g .recharts-layer'
        selectors["column"] = 'path'
        selectors["values_wrapper"] = f'{selectors["graph"]} .yAxis .recharts-cartesian-axis-tick'
        selectors["height"] = f'{selectors["values_wrapper"]}:last-child > text'
        selectors["max_value"] = f'{selectors["values_wrapper"]}:last-child > text tspan'
        selectors["min_value"] = selectors["max_value"].replace("last-child", "first-child")
        selectors["extra_columns"] = {
            "prorated_operating": f'{selectors["graph"]} [name="OpCF"]:not([fill="none"])',
            "capital_rise": f'{selectors["graph"]} [name="Cap Raise"]:not([fill="none"])',
            "current_cash_sheet": f'{selectors["graph"]} [name="Current Est"]:not([fill="none"])',                        
        }
        selectors["description"] = '#results-os-chart + p + p'
        
        data = {}
        
        # Get graph info
        graph_height = int(self.get_attrib(selectors['height'], "height"))
        max_value = float(self.get_text(selectors['max_value']))
        min_value = float(self.get_text(selectors['min_value']))
        range_value = max_value + abs(min_value)

        # Data from regylar columns
        data["columns_data"] = self.__get_columns_data__(
            selectors["columns_wrapper"],
            selectors["column"],
            graph_height,
            range_value,
        )
    
        # Data from extra columns
        for column_name, selector in selectors["extra_columns"].items():
            
            # Get and validate column height
            column_height_str = self.get_attrib(selector, "height")
            if column_height_str:
                # Calculate value
                column_height = float(column_height_str)
                column_value = self.__get_column_value__ (column_height, graph_height, range_value)
            else:
                # Same value
                column_value = None
                
            data[column_name] = column_value
                
        # Get data from description
        data["cash_description"] = self.get_text(selectors["description"])
        data["months_of_cash"] = None
        data["quarterly_cash_burn_m"] = None
        data["current_cash_m"] = None
        data["m"] = None
        numbers = re.findall(r'-?\d+(\.\d+)?', data["cash_description"])
        if len (numbers) == 3:
            data["months_of_cash"] = numbers[0]
            data["quarterly_cash_burn_m"] = numbers[1]
            data["current_cash_m"] = numbers[2]
        elif len (numbers) == 1:
            data["m"] = numbers[0]
    
        return data