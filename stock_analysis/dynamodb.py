import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import pandas as pd
import json
from typing import Dict

PAGE_TO_DB = {
    '': "Income",
    'balance-sheet': "BalanceSheet",
    'cash-flow-statement': "CashFlow",
    'ratios': "Ratios",
}
STOCK_TICKER_KEY = "stock_ticker"
YEAR_KEY = "year"


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
                        "AttributeName": STOCK_TICKER_KEY,
                        "KeyType": "HASH",
                    },
                    {
                        "AttributeName": YEAR_KEY,
                        "KeyType": "RANGE",
                    },
                ],
                BillingMode="PAY_PER_REQUEST"
            )


def put_pandas_dataframe(stock_ticker: str, table_name: str, df):
    client = get_table_client(table_name)
    for year, row in df.iterrows():
        row_dict = json.loads(row.to_json(), parse_float=Decimal)
        row_dict[STOCK_TICKER_KEY] = stock_ticker
        row_dict[YEAR_KEY] = year
        with client.batch_writer() as batch:
            response = client.query(
                KeyConditionExpression=Key(STOCK_TICKER_KEY).eq(stock_ticker) & Key(YEAR_KEY).eq(year)
            )
            if "Items" in response:
                print(f"Skipping stock {stock_ticker} year {year}, as it is already in table {table_name}...")
                continue
            batch.put_item(
                Item=row_dict,
            )


def query_stock(stock_ticker: str):
    stock_info = {}
    for _, table_name in PAGE_TO_DB.items():
        client = get_table_client(table_name)
        response = client.query(KeyConditionExpression=Key(STOCK_TICKER_KEY).eq(stock_ticker))
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

