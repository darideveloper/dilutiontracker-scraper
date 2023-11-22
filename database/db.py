import os
from database.mysql import MySQL
from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")


class Database (MySQL):

    def __init__(self):

        # Connect to mysql
        super().__init__(DB_HOST, DB_NAME, DB_USER, DB_PASS)

        self.premarket_id = None

    def __get_dict_table__(self, table_name: str) -> dict:
        """ Get registers from dictionary table (tables with only name and id)

        Args:
            table_name (str): table name

        Returns:
            list: registers found

            Structure:
            {
                "str (name)": int (index),
            }
        """

        sql = f"""
            SELECT * FROM {table_name}
        """

        data = self.run_sql(sql)

        # Format data
        data_dict = {}
        for row in data:
            data_dict[row["name"]] = row["id"]

        return data_dict

    def __inert_dict_table__(self, table_name: str, name: str):
        """ Insert a name a dictionary table (tables with only name and id)

        Args:
            table_name (str): table name
            name (str): name to insert
        """

        # Insert new name
        sql = f"""
            INSERT INTO {table_name} (name)
            VALUES ("{name}")
        """

        self.run_sql(sql, auto_commit=False)

    def __get_column_origin__(self, origin: str) -> int:
        """ Get a register from columns_origins table
            (create if not exists)

        Args:
            origin (str): origin name

        Returns:
            int: origin id
        """

        origin = "historical"
        columns_origins = self.__get_dict_table__("columns_origins")
        colunms_origin_id = columns_origins.get(origin, None)
        if not origin in columns_origins.keys():
            self.__inert_dict_table__("columns_origins", origin)
            colunms_origin_id = self.cursor.lastrowid

        return colunms_origin_id

    def __save_columns__(self, columns_data: list, colunms_origin: str):
        """ Save columns in database

        Args:
            columns_data (list): columns data
                Structure:
                [
                    {
                        "position": int,
                        "date": datetime,
                        "hos": float,
                    },
                ]
            colunms_origin (str): columns origin name
        """

        colunms_origin_id = self.__get_column_origin__(colunms_origin)

        for column in columns_data:

            sql = f"""
                INSERT INTO columns (
                    origin_id,
                    premarket_id,
                    position,
                    date,
                    hos
                ) values (
                    {colunms_origin_id},
                    {self.premarket_id},
                    {column["position"]},
                    "{column["date"].strftime("%Y-%m-%d")}",
                    {column["hos"]}
                )
            """

            self.run_sql(sql, auto_commit=False)

    def __get_dict_tables_data__(self, tables: dict, values: dict, ids: dict = {}) -> dict:
        """ Query multiple dicts (tables with only name and id) from database
            and create registers if not exists, and return ids.

            All keys from tables must be in values dict

        Args:
            tables (dict): tables names
                Structure:
                {
                    "str (field_name)": "str (table_name_db)",    
                }
            values (dict): values to insert
                Structure:
                {
                    "str (table_name)": "str (scraping value)",
                    ...
                }
            ids (dict, optional): Dictionary to save final ids. Defaults to [].

        Returns:
            dict: tables ids
                Structure:
                {
                    "table_name": "id",
                    ...
                }
        """

        # Insert new dict data in tables
        for field, table in tables.items():

            # Get new sectors
            fields = self.__get_dict_table__(table)
            fields_names = fields.keys()
            premarket_field = values[field]

            # Save fields data
            ids[field] = fields

            # Save new sector
            if not premarket_field in fields_names:
                self.__inert_dict_table__(table, premarket_field)

                # Update fields data
                ids[field][premarket_field] = self.cursor.lastrowid

        return ids

    def save_premarket_data(self, premarket_data: dict):
        """ Save in database the premarket data

        Args:
            premarket_data (dict): premarket data

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

        tables = {
            "sector": "premarket_sectors",
            "industry": "premarket_industries",
            "dilution_data": "premarket_dilution_data",
            "overall_risk": "premarket_adjectives",
            "offering_abillity": "premarket_adjectives",
            "dilution_amt_ex_shelf": "premarket_adjectives",
            "historical": "premarket_adjectives",
            "cash_need": "premarket_adjectives",
        }

        dict_tables_data = self.__get_dict_tables_data__(
            tables, premarket_data)

        # Insert new dict data in tables
        for field, table in tables.items():

            # Get new sectors
            fields = self.__get_dict_table__(table)
            fields_names = fields.keys()
            premarket_field = premarket_data[field]

            # Save fields data
            dict_tables_data[field] = fields

            # Save new sector
            if not premarket_field in fields_names:
                self.__inert_dict_table__(table, premarket_field)

                # Update fields data
                dict_tables_data[field][premarket_field] = self.cursor.lastrowid

        # Save premarket data
        sql = f"""
            INSERT INTO premarket (
                name,
                sector_id,
                industry_id,
                mkt_cap,
                float_cap,
                est_cash_sh,
                t25_inst_own,
                si,
                description_company,
                dilution_data_id,
                overall_risk,
                offering_ability,
                dilution_amt_ex_shelf,
                historical,
                cash_need,
                our_take,
                update_info
            ) values (
                {self.get_clean_text(premarket_data["name"])},
                {dict_tables_data["sector"][premarket_data["sector"]]},
                {dict_tables_data["industry"][premarket_data["industry"]]},
                {premarket_data["mkt_cap"]},
                {premarket_data["float_cap"]},
                {premarket_data["est_cash_sh"]},
                {premarket_data["t25_inst_own"]},
                {premarket_data["si"]},
                {self.get_clean_text(premarket_data["description_company"])},
                {dict_tables_data["dilution_data"][premarket_data["dilution_data"]]},
                {dict_tables_data["overall_risk"][premarket_data["overall_risk"]]},
                {dict_tables_data["offering_abillity"][premarket_data["offering_abillity"]]},
                {dict_tables_data["dilution_amt_ex_shelf"][premarket_data["dilution_amt_ex_shelf"]]},
                {dict_tables_data["historical"][premarket_data["historical"]]},
                {dict_tables_data["cash_need"][premarket_data["cash_need"]]},
                {self.get_clean_text(premarket_data["out_take"], "\n")},
                {self.get_clean_text(premarket_data["update_info"])}
            )
        """
        self.run_sql(sql, auto_commit=False)

        # Get premarket id
        self.premarket_id = self.cursor.lastrowid

        # Commit changes
        self.commit_close()

    def save_historical_data(self, historical_data: dict):
        """ Save in database the historial data

        Args:
            historical_data (dict): historial data

            Structure:
            {
                columns_data:   [
                    {
                        "id": position,
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

        # Save historial data
        sql = f"""
            INSERT INTO historical (
                premarket_id,
                atm,
                warrant,
                convertible_preferred,
                convertible_note,
                equality_line,
                s1_offering
            ) values (
                {self.premarket_id},
                {historical_data["atm"]},
                {historical_data["warrant"]},
                {historical_data["convertible_preferred"]},
                {historical_data["convertible_note"]},
                {historical_data["equality_line"]},
                {historical_data["s1_offering"]}
            )
        """
        self.run_sql(sql, auto_commit=False)

        # Insert columns data
        columns_data = historical_data["columns_data"]
        self.__save_columns__(columns_data, "historical")

        # Commit changes
        self.commit_close()

    def save_cash_data(self, cash_data: dict):
        """ Save in database the cash data

        Args:
            cash_data (dict): cash data

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

        # Save cash data
        sql = f"""
            INSERT INTO cash (
                premarket_id,
                cash_description,
                months_of_cash,
                quarterly_cash_burn_m,
                current_cash_m,
                m,
                prorated_operating,
                capital_rise,
                current_cash_sheet
            ) values (
                {self.premarket_id},
                {self.get_clean_text(cash_data["cash_description"])},
                {cash_data["months_of_cash"]},
                {cash_data["quarterly_cash_burn_m"]},
                {cash_data["current_cash_m"]},
                {cash_data["m"]},
                {cash_data["prorated_operating"]},
                {cash_data["capital_rise"]},
                {cash_data["current_cash_sheet"]}
            )
        """
        self.run_sql(sql, auto_commit=False)

        # Insert columns data
        columns_data = cash_data["columns_data"]
        self.__save_columns__(columns_data, "cash")

        # Commit changes
        self.commit_close()

    def save_extra_data(self, extra_data: list):
        """ Save in database the extra data

        Args:
            extra_data (list): dictionaries with extra data from details tables

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

        tables = {
            "origin": "extras_origins",
            "status": "extras_status",
            "name": "extras_names",
        }

        # Save each row
        dict_tables_data = {}
        for extra_data_row in extra_data:

            self.__get_dict_tables_data__(
                tables, extra_data_row, dict_tables_data)

            # Save row data
            sql = f"""
                INSERT INTO extras (
                    premarket_id,
                    origin_id,
                    status_id,
                    name_id,
                    position,
                    title,
                    item
                ) values (
                    {self.premarket_id},
                    {dict_tables_data["origin"][extra_data_row["origin"]]},
                    {dict_tables_data["status"][extra_data_row["status"]]},
                    {dict_tables_data["name"][extra_data_row["name"]]},
                    {extra_data_row["position"]},
                    {self.get_clean_text(extra_data_row["title"])},
                    {self.get_clean_text(extra_data_row["value"])}
                )
            """
            self.run_sql(sql, auto_commit=False)

        # Commit changes
        self.commit_close()

    def save_completed_offering_data(self, completed_offering_data: list):
        """ Save in database the complete offering data

        Args:
            completed_offering_data (list): dictionaries with complete offering data from details tables

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

        tables = {
            "type": "completed_offerings_types",
            "method": "completed_offerings_methods",
            "investors": "completed_offerings_investors",
        }

        # Save each row
        dict_tables_data = {}
        for completed_data_row in completed_offering_data:

            self.__get_dict_tables_data__(
                tables, completed_data_row, dict_tables_data)

            # Save row data
            sql = f"""
                INSERT INTO completed_offerings (
                    premarket_id,
                    type_id,
                    method_id,
                    share_equivalent,
                    price,
                    warrants,
                    offering_amt,
                    bank,
                    investors,
                    date
                ) values (
                    {self.premarket_id},
                    {dict_tables_data["type"][completed_data_row["type"]]},
                    {dict_tables_data["method"][completed_data_row["method"]]},
                    {completed_data_row["share_equivalent"]},
                    {completed_data_row["price"]},
                    {completed_data_row["warrants"]},
                    {completed_data_row["offering_amt"]},
                    {self.get_clean_text(completed_data_row["bank"])},
                    {dict_tables_data["investors"][completed_data_row["investors"]]},
                    "{completed_data_row["date"].strftime("%Y-%m-%d")}"
                )
            """
            self.run_sql(sql, auto_commit=False)

        # Commit changes
        self.commit_close()

    def save_news_data(self, news_data: list):
        """ Save in database the news data

        Args:
            news_data (list): dictionaries with news data from details tables

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

        # Save each row
        for news_data_row in news_data:

            # Save row data
            sql = f"""
                INSERT INTO news (
                    premarket_id,
                    time_ago_number,
                    time_ago_label,
                    datetime,
                    headline,
                    link
                ) values (
                    {self.premarket_id},
                    {news_data_row["time_ago_number"]},
                    {self.get_clean_text(news_data_row["time_ago_label"])},
                    "{news_data_row["datetime"].strftime("%Y-%m-%d %H:%M:%S")}",
                    {self.get_clean_text(news_data_row["headline"])},
                    "{news_data_row["link"]}"
                )
            """
            self.run_sql(sql, auto_commit=False)

        # Commit changes
        self.commit_close()

    def save_holders_data (self, holders_data: list):
        """ Save in database the holders data data

        Args:
            holders (list): dictionaries with holders data

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

        tables = {
            "institution_name": "holders_institutions",
            "form": "holders_form_types",
        }

        # Save each row
        dict_tables_data = {}
        for holders_data_row in holders_data:

            self.__get_dict_tables_data__(
                tables, 
                holders_data_row,
                dict_tables_data
            )

            # Save row data
            sql = f"""
                INSERT INTO holders (
                    premarket_id,
                    institution_id,
                    percentage,
                    shares,
                    change_,
                    form_type_id,
                    efective,
                    field_
                ) values (
                    {self.premarket_id},
                    {dict_tables_data["institution_name"][holders_data_row["institution_name"]]},
                    {holders_data_row["percentage"]},
                    {holders_data_row["shares"]},
                    {holders_data_row["change"]},
                    {dict_tables_data["form"][holders_data_row["form"]]},
                    "{holders_data_row["efective"].strftime("%Y-%m-%d")}",
                    "{holders_data_row["field"].strftime("%Y-%m-%d")}"
                )
                   
            """
            self.run_sql(sql, auto_commit=False)

        # Commit changes
        self.commit_close()

    def save_filings_data (self, fillings_data:list):
        """ Save in database the filings data

        Args:
            fillings_data (list): dictionaries with filings data
            
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
        
        tables = {
            "name": "fillings_names"
        }
        
        dict_tables_data = {}
        for fillings_data_row in fillings_data:

            self.__get_dict_tables_data__(
                tables, 
                fillings_data_row,
                dict_tables_data
            )

            # Save row data
            sql = f"""
                INSERT INTO fillings (
                    premarket_id,
                    name_id,
                    headline,
                    date,
                    link
                ) values (
                    {self.premarket_id},
                    {dict_tables_data["name"][fillings_data_row["name"]]},
                    {self.get_clean_text(fillings_data_row["headline"])},
                    "{fillings_data_row["date"].strftime("%Y-%m-%d")}",
                    "{fillings_data_row["link"]}"
                )                      
            """
            self.run_sql(sql, auto_commit=False)

        # Commit changes
        self.commit_close()