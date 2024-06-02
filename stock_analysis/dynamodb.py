import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from enum import Enum
import pandas as pd
from pprint import pprint
import json
from typing import Any, Dict, List, Optional


class FinancialStatements(Enum):
    INCOME = "Income"
    BALANCE_SHEET = "BalanceSheet"
    CASH_FLOW = "CashFlow"
    RATIOS = "Ratios"

PAGE_TO_DB = {
    '': FinancialStatements.INCOME.value,
    'balance-sheet': FinancialStatements.BALANCE_SHEET.value,
    'cash-flow-statement': FinancialStatements.CASH_FLOW.value,
    'ratios': FinancialStatements.RATIOS.value,
}

class FinanceTermKeys(Enum):
    STOCK_TICKER = "stock_ticker"
    YEAR = "year"
    TREASURY_STOCK_RAW = "Treasury Stock"
    TREASURY_STOCK = "Treasury_Stock"
    NET_INCOME = "Net_Income"
    TOTAL_LONG_TERM_LIABILITIES = "Total_Long-Term_Liabilities"
    SHAREHOLDERS_EQUITY = "Shareholders'_Equity"
    TOTAL_LIABILITIES = "Total_Liabilities"
    NET_EARNINGS_TO_LONG_TERM_DEBT = "Net_Earnings_to_Long_Term_Debt"
    TRUE_SHAREHOLDERS_EQUITY = "True_Shareholders'_Equity"
    ADJUSTED_DEBT_TO_TRUE_SHAREHOLDERS_EQUITY_RATIO = "Adjusted_Debt_to_True_Shareholders'_Equity_Ratio"
    ADJUSTED_RETURN_ON_TRUE_SHAREHOLDERS_EQUITY_RATIO = "Adjusted_Return_on_True_Shareholders'_Equity_Ratio"
    CAPEX_RATIO = "Capex_Ratio"
    CAPITAL_EXPENDITURES = "Capital_Expenditures"
    PRETAX_EPS = "Pretax_EPS"
    PRETAX_INCOME = "Pretax_Income"
    SHARES_OUTSTANDING_DILUTED = "Shares_Outstanding_Diluted"


def create_tables_if_not_exist():
    client = get_low_level_db_client()
    table_names = client.list_tables()['TableNames']
    for _, table_name in PAGE_TO_DB.items():
        if table_name not in table_names:
            client.create_table(
                AttributeDefinitions=[
                    {
                        "AttributeName": "stock_ticker",
                        "AttributeType": "S",
                    },
                    {
                        "AttributeName": "year",
                        "AttributeType": "S",
                    },
                ],
                TableName=table_name,
                KeySchema=[
                    {
                        "AttributeName": FinanceTermKeys.STOCK_TICKER.value,
                        "KeyType": "HASH",
                    },
                    {
                        "AttributeName": FinanceTermKeys.YEAR.value,
                        "KeyType": "RANGE",
                    },
                ],
                BillingMode="PAY_PER_REQUEST"
            )


def put_pandas_dataframe(stock_ticker: str, table_name: str, df, ignore_if_exists: bool = False):
    client = get_table_client(table_name)
    with client.batch_writer() as batch:
        for year, row in df.iterrows():
            row_dict = json.loads(row.to_json(), parse_float=Decimal)
            row_dict[FinanceTermKeys.STOCK_TICKER.value] = stock_ticker
            row_dict[FinanceTermKeys.YEAR.value] = year
            if ignore_if_exists:
                response = client.query(
                    KeyConditionExpression=Key(FinanceTermKeys.STOCK_TICKER.value).eq(stock_ticker) & Key(FinanceTermKeys.YEAR.value).eq(year)
                )
                if "Items" in response:
                    print(f"Skipping stock {stock_ticker} year {year}, as it is already in table {table_name}...")
                    continue
            batch.put_item(
                Item=row_dict,
            )


def put_stock_info(stock_info: Dict[str, List[Dict[str, Any]]]):
    for table_name, statements_list in stock_info.items():
        client = get_table_client(table_name)
        with client.batch_writer() as batch:
            for statement in statements_list:
                batch.put_item(
                    Item=statement,
                )


def query_stock(stock_ticker: str, year: Optional[int] = None):
    stock_info = {}
    for _, table_name in PAGE_TO_DB.items():
        client = get_table_client(table_name)
        key_condition_expression = Key(FinanceTermKeys.STOCK_TICKER.value).eq(stock_ticker)
        if year:
            key_condition_expression = key_condition_expression & Key(FinanceTermKeys.YEAR.value).eq(str(year))
        response = client.query(KeyConditionExpression=key_condition_expression)
        stock_info[table_name] = response["Items"] if "Items" in response else []
    return stock_info


def to_attribute_numbers_map(row_dict: Dict[str, str]):
    attribute_map = {}
    for key, val in row_dict.items():
        attribute_map[key] = {
            'N': str(val)
        }
    return attribute_map


def get_low_level_db_client():
    from config import USE_LOCAL_DDB
    if USE_LOCAL_DDB:
        return boto3.client(
            'dynamodb',
            endpoint_url='http://localhost:8000'
        )
    else:
        return boto3.client('dynamodb')


def get_table_resource():
    """Open a new database connection.
    """
    from config import USE_LOCAL_DDB
    if USE_LOCAL_DDB:
        return boto3.resource(
            'dynamodb',
            endpoint_url='http://localhost:8000'
        )
    else:
        return boto3.resource('dynamodb')


def get_table_client(table_name):
    """Open a new database connection.
    """
    return get_table_resource().Table(table_name)


def close_db(sqlite_db):
    """Close the database at the end of a request.
    """
    if sqlite_db is not None:
        sqlite_db.commit()
        sqlite_db.close()

