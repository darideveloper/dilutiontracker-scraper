import os
from database.mysql import MySQL
from dotenv import load_dotenv
load_dotenv ()

DB_HOST = os.getenv ("DB_HOST")
DB_NAME = os.getenv ("DB_NAME")
DB_USER = os.getenv ("DB_USER")
DB_PASS = os.getenv ("DB_PASS")

class Database (MySQL):
    
    def __init__ (self):
        
        # Connect to mysql
        super().__init__ (DB_HOST, DB_NAME, DB_USER, DB_PASS)
        
    def __get_dict_table__ (self, table_name:str) -> dict:
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
        
        data = self.run_sql (sql)
        
        # Format data
        data_dict = {}
        for row in data:
            data_dict [row ["name"]] = row ["id"]
        
        return data_dict
    
    def __inert_dict_table__ (self, table_name:str, name:str):
        """ Insert a name a dictionary table (tables with only name and id)
        
        Args:
            table_name (str): table name
            name (str): name to insert
        """
        
        # Remove quotes from None values and convert to NULL
        value = "NULL" if name is None else f'"{name}"'
        
        # Insert new name
        sql = f"""
            INSERT INTO {table_name} (name)
            VALUES ({value})
        """
        
        self.run_sql (sql)
    
    def save_premarket_data (self, premarket_data):
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
        
        dict_tables_data = {}
        
        # Insert new dict data in tables
        for field, table in tables.items():
        
            # Get new sectors
            fields = self.__get_dict_table__ (table)
            fields_names = fields.keys ()
            premarket_field = premarket_data [field]
            
            # Save fields data
            dict_tables_data[field] = fields
            
            # Save new sector
            if not premarket_field in fields_names:
                self.__inert_dict_table__ (table, premarket_field)
                
                # Update fields data
                fields = self.__get_dict_table__ (table)
                dict_tables_data[field] = fields
                        
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
                "{self.get_clean_text(premarket_data["name"])}",
                {dict_tables_data["sector"][premarket_data["sector"]]},
                {dict_tables_data["industry"][premarket_data["industry"]]},
                {premarket_data["mkt_cap"]},
                {premarket_data["float_cap"]},
                {premarket_data["est_cash_sh"]},
                {premarket_data["t25_inst_own"]},
                {premarket_data["si"]},
                "{self.get_clean_text(premarket_data["description_company"])}",
                {dict_tables_data["dilution_data"][premarket_data["dilution_data"]]},
                {dict_tables_data["overall_risk"][premarket_data["overall_risk"]]},
                {dict_tables_data["offering_abillity"][premarket_data["offering_abillity"]]},
                {dict_tables_data["dilution_amt_ex_shelf"][premarket_data["dilution_amt_ex_shelf"]]},
                {dict_tables_data["historical"][premarket_data["historical"]]},
                {dict_tables_data["cash_need"][premarket_data["cash_need"]]},
                "{self.get_clean_text(premarket_data["out_take"], "\n")}",
                "{self.get_clean_text(premarket_data["update_info"])}"
            )
        """
        
        self.run_sql (sql)
           
        print ()
                
            
        
        