from time import sleep
from datetime import datetime as dt, timedelta
from scraping.web_scraping import WebScraping


class ScrapingDilutionTracker (WebScraping):

    def __init__(self, chrome_folder: str):
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

    def __get_column_value__(self, column_height: float, graph_height: int,
                             max_value: float) -> float:
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

    def __get_columns_data__(self, selector_columns_wrapper: str, selector_column: str,
                             graph_height: int, max_value: float) -> list:
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
                    "position": int,
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

            selector_current_column = f'{selector_columns_wrapper}:nth-child({column_index+1}) {selector_column}'

            # Skip empty columns
            column = self.get_elems(selector_current_column)
            if not column:
                continue

            # Get data from current column
            column_name = self.get_attrib(selector_current_column, "name")
            column_height = float(self.get_attrib(
                selector_current_column, "height"))

            column_value = self.__get_column_value__(
                column_height, graph_height, max_value)

            # Format date and detect when columns ends
            last_column = False
            try:
                date = dt.strptime(column_name, "%m/%d/%Y")
            except:
                date = dt.now()
                last_column = True

            # Save column data
            columns_data.append({
                "position": column_index,
                "date": date,
                "hos": column_value,
            })

            if last_column:
                break

        return columns_data

    def __delete_icons__(self):
        """ Delete extra/no required icons """

        selectors = [".dtCardInfoIcon', '.dilutionRatingInfoIcon"]
        for selector in selectors:
            self.driver.execute_script(f"""
                document.querySelectorAll ('{selector}').forEach (icon => {{
                    icon.remove()
                }})
            """)

    def __get_table_data__(self, selector_rows: str, column: dict,
                           start_row: int = 1, end_row: int = -1) -> list:
        """ get data from table structure

        Args:
            selector_rows (str): selector of each row of table
            column (dict): column data: selector, datatype and optional extra
            start_row (int, optional): start row index (inclusive). Defaults to 1
            end_row (int, optional): end row index (no inclusive). Defaults to -1

        Returns:
            list: table data with dynamic structure (based on column dict)
        """

        data = []

        rows_num = len(self.get_elems(selector_rows))
        for index in range(rows_num):

            # End loop if end row is reached
            if index + start_row == end_row:
                break

            selector_row = f'{selector_rows}:nth-child({index + start_row})'

            data_row = {}
            for colum_name, column_data in column.items():

                selector_column = f'{selector_row} {column_data["selector"]}'
                data_type_column = column_data["data_type"]

                # Extract links
                extra = column_data.get("extra", {})
                if extra.get("is_link", False):
                    value = self.get_attrib(selector_column, "href")
                    data_row[colum_name] = value
                    continue

                # Extract texts
                replace_chats = [
                    "\\",
                    "'",
                    '"'
                ]
                value = self.get_text(selector_column)
                for chat in replace_chats:
                    value = value.replace(chat, "")

                # Skip empty values
                if not value:
                    data_row[colum_name] = "NULL"
                    continue

                # Convert numeric fields
                if data_type_column in [int, float]:
                    value = value.replace(",", "").replace(
                        "%", "").replace("$", "")

                # Convert date format 2023-09-30
                if data_type_column == dt:

                    # Get format frome extra data
                    format_date = column_data["extra"]["format"]
                    value = dt.strptime(value, format_date)

                data_row[colum_name] = value

            data.append(data_row)

        return data
    
    def __clean_chars__ (self, text: str, chars: list) -> str:
            """ Remove extra chars from text

            Args:
                text (str): text to clean
                chars (list): chars to remove

            Returns:
                str: cleaned text
            """
            
            for char in chars:
                text = text.replace (char, "")
                
            return text

    def login(self) -> bool:
        """ Validate correct login and go to app page

        Returns:
            bool: True if login success
        """

        selectors = {
            "nav_items": 'nav li',
            "close_modal": '.intercom-post-close'
        }

        # Load home page
        self.set_page(self.pages["home"])
        self.refresh_selenium()

        # Validte if exists "go to app" button
        go_to_app_found = False
        buttons_texts = self.get_texts(selectors["nav_items"])
        if "Go to App" in buttons_texts:

            # Go to home page
            old_page = self.driver.current_url
            self.set_page (f'{self.pages["home"]}/app')
        
            go_to_app_found = True
            
        # Detect if login failed
        if not go_to_app_found:
            return False

        # Validate page change
        current_page = self.driver.current_url
        if old_page == current_page:
            return False

        # Close modal
        modal_elem = self.get_elems(selectors["close_modal"])
        if modal_elem:
            self.click_js(selectors["close_modal"])
            self.refresh_selenium()

        return True

    def load_company(self, company: str):
        """ Load company page

        Args:
            company (str): company ticker
        """

        url = f'{self.pages["home"]}/app/search/{company}'
        self.set_page(url)
        self.refresh_selenium()

        # Delete extra icons
        self.__delete_icons__()
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
                "wrapper_counters": '.mw-1010:nth-child(2) [class="cursor-default"] > div',
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
            if not_found == "We haven't indexed this ticker yet": 
                data["found"] = False
                return data

        # Get company name
        data["name"] = self.get_text(selectors["name"])

        # Get header texts
        headers_text_num = len(self.get_elems(
            selectors["header"]["wrapper_texts"]))
        for header_index in range(headers_text_num):

            # Get key and info
            selector_header = f'{selectors["header"]["wrapper_texts"]}:nth-child({header_index+1})'
            selector_texts = f'{selector_header} {selectors["header"]["info"]}'
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
            selector_header = f'{selectors["header"]["wrapper_counters"]}:nth-child({header_index+1})'
            selector_counters = f'{selector_header} {selectors["header"]["info"]}'
            counters = self.get_elems(selector_counters)

            key = counters[0].text.lower()
            info = counters[1].text.lower()

            if "mkt cap" in key:
                data["mkt_cap"] = info.split("m")[0].split("t")[0]
            elif "float" in key:
                data["float_cap"] = info.split("m")[0]
            elif "est" in key:
                data["est_cash_sh"] = info
            elif "t25" in key:
                data["t25_inst_own"] = info.replace("%", "")
            elif "si" in key:
                data["si"] = info.replace("%", "")

        # Company description
        try:
            self.click(selectors["description"]["show_more_btn"])
        except:
            pass
        else:
            self.refresh_selenium()
        data["description_company"] = self.get_text(
            selectors["description"]["info"])

        # Adjectives
        adjectives_num = len(self.get_elems(
            selectors["adjectives"]["wrappers"]))
        for adjective_index in range(adjectives_num):

            # Get each adjective and category
            selector_adjective = f'{selectors["adjectives"]["wrappers"]}:nth-child({adjective_index+1})'
            selector_name = f'{selector_adjective} {selectors["adjectives"]["name"]}'
            selector_info = f'{selector_adjective} {selectors["adjectives"]["info"]}'
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
            selector_our_take = f'{selectors["our_take"]["wrappers"]}:nth-child({our_take_index+2})'
            selector_datetime = f'{selector_our_take} {selectors["our_take"]["datetime"]}'
            selector_info = f'{selector_our_take} {selectors["our_take"]["info"]}'

            datetime = self.get_text(selector_datetime).lower()
            info = self.get_text(selector_info).lower()

            # Save as text
            line = f'{datetime}  {info}'
            our_take_lines += f'{line}\n'

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
                        "position": int,
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
            column_value = self.__get_column_value__(
                column_height, graph_height, max_value)

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
                        "position": int,
                        "date": datetime,
                        "hos": float,
                    },
                ].
                prorated_operating: float,
                capital_rise: float,
                current_cash_sheet: float,
                cash_description: str,
                months_of_cash: float,
                quarterly_cash_burn_m: float,
                current_cash_m: float,
                m: float,
            }

        """

        selectors = {}
        selectors["graph"] = '.results-cash-bar-chart'  # graph wrapper
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
        selectors["description_item"] = f"{selectors['description']} > strong"

        data = {}

        # Get graph info
        graph_height = int(self.get_attrib(selectors["height"], "height"))
        max_value = float(self.get_text(selectors["max_value"]))
        min_value = float(self.get_text(selectors["min_value"]))
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
                column_value = self.__get_column_value__(
                    column_height, graph_height, range_value)
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
        description_items = self.get_texts(selectors["description_item"])
        
        # Clean description items
        remove_chars = [" months", "-", "$", "M"]
        description_items = list(map(lambda item: self.__clean_chars__(item, remove_chars), description_items))
        
        if len(description_items) == 3:
            data["months_of_cash"] = description_items[0]
            data["quarterly_cash_burn_m"] = description_items[1]
            data["current_cash_m"] = description_items[2][:-1]
        elif len(description_items) == 1:
            data["m"] = description_items[0]

        return data

    def get_extra_data(self) -> list:
        """ Get extra data from company page (details tables)

        Returns:
            list: extra data

            Structure:
            [
                {
                    "origin": str,
                    "status": str,
                    "name": str,
                    "title": str,
                    "value": str,
                    "position": int,
                },
                ...
            ]
        """

        selectors = {
            "content_section": "#dashContentWrapper > div",
            "extras_wrapper": '.my-3',
            "title": '.heading-filing-category',
            "table": {
                "wrapper": '.card',
                "title": "h5",
                "status": ".opacity-7",
                "data": {
                    "wrapper": "ul > li",
                    "item": "span:first-child",
                    "value": "span:last-child",
                }
            }

        }

        data = []

        # Identify position of first extra table
        first_extra_index = 0
        content_sections_num = len(
            self.get_elems(selectors["content_section"]))
        for section_index in range(content_sections_num):
            selector_section = f'{selectors["content_section"]}:nth-child({section_index+1}){selectors["extras_wrapper"]}'
            section = self.get_elems(selector_section)
            if section:
                first_extra_index = section_index
                break

        # Loop each extra table
        extras_num = len(self.get_elems(selectors["extras_wrapper"]))
        for extra_index in range(extras_num):

            # Get extra title
            selector_extra = f'{selectors["extras_wrapper"]}:nth-child({extra_index+first_extra_index+1})'
            selector_title = f'{selector_extra} {selectors["title"]}'
            title = self.get_text(selector_title)

            # Loop data tables
            selectors_table = selectors["table"]
            selector_tables = f'{selector_extra} {selectors_table["wrapper"]}'
            tables_num = len(self.get_elems(selector_tables))
            for table_index in range(tables_num):

                selector_table = f'{selector_tables}:nth-child({table_index+1})'

                # Get table title
                selector_table_title = f'{selector_table} {selectors_table["title"]}'
                table_title = self.get_text(selector_table_title)

                # Get table status
                selector_table_status = f'{selector_table} {selectors_table["status"]}'
                table_status = self.get_text(selector_table_status)

                # Get table info
                selector_rows = f'{selector_table} {selectors_table["data"]["wrapper"]}'
                rows_num = len(self.get_elems(selector_rows))
                for row_index in range(rows_num):

                    selector_row = f'{selector_rows}:nth-child({row_index+1})'

                    # Get row data
                    selector_item = f'{selector_row} {selectors_table["data"]["item"]}'
                    selector_value = f'{selector_row} {selectors_table["data"]["value"]}'
                    item = self.get_text(selector_item)
                    value = self.get_text(selector_value)

                    # Save data
                    data.append({
                        "origin": title,
                        "status": table_status,
                        "name": table_title,
                        "title": item,
                        "value": value,
                        "position": table_index + 1,
                    })

        return data

    def get_completed_offering_data(self) -> list:
        """ Get data from complete offering table

        Returns:
            list: complete offering data

            Structure:
            [
                {
                    "type": str,
                    "method": str,
                    "share_equivalent": int
                    "price": float,
                    "warrants": int,
                    "offering_amt": int,
                    "bank": str,
                    "investors": str,
                    "date": datetime,
                },
                ...
            ]
        """

        selector_rows = f'#stickyTableHeadingExtraTopWhite + table tbody tr'
        columns = {
            "type": {
                "selector": f'td:nth-child(1)',
                "data_type": str,
            },
            "method": {
                "selector": f'td:nth-child(2)',
                "data_type": str,
            },
            "share_equivalent": {
                "selector": f'td:nth-child(3)',
                "data_type": int,
            },
            "price": {
                "selector": f'td:nth-child(4)',
                "data_type": float,
            },
            "warrants": {
                "selector": f'td:nth-child(5)',
                "data_type": int,
            },
            "offering_amt": {
                "selector": f'td:nth-child(6)',
                "data_type": int,
            },
            "bank": {
                "selector": f'td:nth-child(7)',
                "data_type": str,
            },
            "investors": {
                "selector": f'td:nth-child(8)',
                "data_type": str,
            },
            "date": {
                "selector": f'td:nth-child(9)',
                "data_type": dt,
                "extra": {"format": "%Y-%m-%d %H:%M"}
            },
        }

        table_data = self.__get_table_data__(
            selector_rows,
            columns
        )

        return table_data

    def get_news_data(self) -> list:
        """ Get last 5 registers from news tab

        Returns:
            list: news data

            Structure:
            [
                {
                    "time_ago_number": int,
                    "time_ago_label": str,
                    "datetime": datetime,
                    "headline": str,
                    "link": str,
                },
                ...
            ]
        """

        selector_btn = '#result-tab-news'
        selector_rows = f'.mb-5:last-child .my-2'

        columns = {
            "time_ago": {
                "selector": 'span:nth-child(1)',
                "data_type": str,
            },
            "datetime": {
                "selector": 'span:nth-child(3)',
                "data_type": dt,
                "extra": {"format": "%m/%d/%Y, %I:%M:%S %p"}
            },
            "headline": {
                "selector": 'a',
                "data_type": str,
            },
            "link": {
                "selector": 'a',
                "data_type": str,
                "extra": {"is_link": True}
            }
        }

        # Move to tab
        self.click(selector_btn)
        self.refresh_selenium()

        # Get table data
        table_data = self.__get_table_data__(
            selector_rows,
            columns,
            start_row=2,
            end_row=7
        )

        # Separate time ago
        for row in table_data:

            time_ago_parts = row["time_ago"].split(" ")
            row["time_ago_number"] = int(time_ago_parts[0])
            row["time_ago_label"] = time_ago_parts[1]

            del row["time_ago"]

        return table_data

    def get_holders_data(self) -> list:
        """ Get data from holders tab
        
        Args:
            database (Database): database instance

        Returns:
            list: holders data

            Structure:
            [
                {
                    "institution_name": str,
                    "percentage": float,
                    "shares": int,
                    "change": float,
                    "form": str,
                    "efective": datetime,
                    "field": datetime,
                },
                ...
            ]

        """

        selector_btn = '#result-tab-inst-own'
        selector_rows = f'.instOwnTable tbody tr'
        columns = {
            "institution_name": {
                "selector": "td:nth-child(1)",
                "data_type": str,
            },
            "percentage": {
                "selector": "td:nth-child(2)",
                "data_type": float,
            },
            "shares": {
                "selector": "td:nth-child(3)",
                "data_type": int,
            },
            "change": {
                "selector": "td:nth-child(4)",
                "data_type": float,
            },
            "form": {
                "selector": "td:nth-child(5)",
                "data_type": str,
            },
            "efective": {
                "selector": "td:nth-child(6)",
                "data_type": dt,
                "extra": {"format": "%Y/%m/%d"}
            },
            "field": {
                "selector": "td:nth-child(7)",
                "data_type": dt,
                "extra": {"format": "%Y/%m/%d"}
            }
        }

        # Move to tab
        self.click(selector_btn)
        self.refresh_selenium()

        table_data = self.__get_table_data__(
            selector_rows,
            columns
        )

        return table_data

    def get_filings_data(self) -> list:
        """ Get filings from the last 10 days, data from filings tab
        in Chronological table

        Returns:
            list: filings data

            Structure:
            [
                {
                    "name": str,
                    "headline": str,
                    "date": datetime,
                    "link": str,
                },
                ...
            ]

        """

        selector_btn = '#result-tab-filings'
        selector_display_all = '.secFilingFetchMoreRow'
        selector_table = '.row > div:first-child .secFilingResultSingleContainer:first-child'
        selector_rows = '.secFilingTableWrapper > div'
        selector_name = '.secFilingFormType'
        selector_headline = '.secFilingDescription'
        selector_date = '.secFilingFiledAt'
        selector_link = '> div'

        # Move to tab
        self.click(selector_btn)
        self.refresh_selenium()

        # Load add results
        for _ in range(4):

            display_all_btn = self.get_elems(selector_display_all)
            if not display_all_btn:
                break

            self.click_js(selector_display_all)

            self.refresh_selenium()

        # Get 10 days ago fromn today
        days_ago = 10
        today = dt.now()
        last_date = today - timedelta(days=days_ago)

        # Loop rows
        data = []
        selector_row = f'{selector_table} {selector_rows}'
        rows_num = len(self.get_elems(selector_row))
        for index in range(rows_num):

            # Get data
            selector_current_name = f'{selector_row}:nth-child({index+1}) {selector_name}'
            selector_current_headline = f'{selector_row}:nth-child({index+1}) {selector_headline}'
            selector_current_date = f'{selector_row}:nth-child({index+1}) {selector_date}'

            name = self.get_text(selector_current_name)
            headline = self.get_text(selector_current_headline)
            date = self.get_text(selector_current_date)

            # Format date like 11/16/23
            date = dt.strptime(date, "%m/%d/%y")

            # Get link openning in a new tab
            selector_current_link = f'{selector_row}:nth-child({index+1}) {selector_link}'
            self.click_js(selector_current_link)
            sleep(5)
            self.switch_to_tab(1)
            link = self.driver.current_url
            self.close_tab()
            self.switch_to_tab(0)

            # End when found the last date
            if date < last_date:
                break

            data.append({
                "name": name,
                "headline": headline,
                "date": date,
                "link": link,
            })

        return data

    def get_noncompliant_data(self, tricker :str) -> list:
        """ Get data from noncompliantcompanylist page
        
        Args:
            tricker (str): company tricker (identifier)

        Returns:
            list: no complaint data

            Structure:
            [
                {
                    "company": str,
                    "deficiency": str,
                    "market": str,
                    "notification_date": datetime,
                },
                ...
            ]
        """

        selectors = {
            "dispay_btn": 'th [type="button"]',
            "rows": '.rgMasterTable tbody tr',
            "company": 'td[colspan="4"] p',
            "tricker": 'td:nth-child(2)',
            "deficiency": 'td:nth-child(3)',
            "market": 'td:nth-child(4)',
            "notification_date": 'td:nth-child(5)',
        }

        # Load page and open registers
        self.set_page(
            "https://listingcenter.nasdaq.com/noncompliantcompanylist.aspx")
        self.click_js(selectors["dispay_btn"])
        sleep(5)
        self.refresh_selenium()

        # Loop each row
        rows_num = len(self.get_elems(selectors["rows"]))
        current_company = ""
        data = []
        for index in range(rows_num):

            selector_row = f'{selectors["rows"]}:nth-child({index+1})'
            selector_company = f'{selector_row} {selectors["company"]}'

            # Detect new company
            company = self.get_text(selector_company)
            if company:
                current_company = company
                continue

            # Validate company tricker
            selector_tricker = f'{selector_row} {selectors["tricker"]}'
            current_tricker = self.get_text(selector_tricker).lower().strip()
            if tricker not in current_tricker:
                continue
            
            # Extract row data
            selector_deficiency = f'{selector_row} {selectors["deficiency"]}'
            selector_market = f'{selector_row} {selectors["market"]}'
            selector_notification_date = f'{selector_row} {selectors["notification_date"]}'

            deficiency = self.get_text(selector_deficiency)
            market = self.get_text(selector_market)
            notification_date = self.get_text(selector_notification_date)

            # Format date
            notification_date = dt.strptime(notification_date, "%m/%d/%Y")

            # Save data
            data.append({
                "company": current_company,
                "deficiency": deficiency,
                "market": market,
                "notification_date": notification_date,
            })

        return data
