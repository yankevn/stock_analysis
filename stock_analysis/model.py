"""Stock analysis model (database) API."""

import boto3

def get_db_client():
    """Open a new database connection.
    """
    from config import USE_LOCAL_DDB
    if USE_LOCAL_DDB:
        return boto3.client(
            'dynamodb',
            endpoint_url='http://localhost:8000'
        )
    else:
        return boto3.client('dynamodb')


def close_db(sqlite_db):
    """Close the database at the end of a request.
    """
    if sqlite_db is not None:
        sqlite_db.commit()
        sqlite_db.close()
