import os
import sys
import logging
import json
import snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logs
snowflake.connector.paramstyle = 'qmark'


def connect_snow():
    private_key = os.getenv("PRIVATE_KEY")
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-insert'}, 
    )


def save_to_snowflake(snow, message):
    if not message.strip():  # Check for empty or whitespace-only message
        logging.warning("Empty message received, skipping.")
        return

    try:
        record = json.loads(message)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON: {e}. Message: {message}")
        return

    logging.debug('Inserting record to db')
    row = (
        record['txid'], record['rfid'], record["resort"], record["purchase_time"],
        record["expiration_time"], record['days'], record['name'],
        json.dumps(record['address']), record['phone'], record['email'],
        json.dumps(record['emergency_contact'])
    )
    snow.cursor().execute(
        "INSERT INTO INGEST.INGEST.LIFT_TICKETS_PY_INSERT "
        "(\"TXID\", \"RFID\", \"RESORT\", \"PURCHASE_TIME\", \"EXPIRATION_TIME\", \"DAYS\", "
        "\"NAME\", \"ADDRESS\", \"PHONE\", \"EMAIL\", \"EMERGENCY_CONTACT\") "
        "SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?)", row
    )
    logging.debug(f"Inserted ticket: {record}")


if __name__ == "__main__":
    snow = connect_snow()
    for message in sys.stdin:
        if message.strip():  # Skip empty lines
            save_to_snowflake(snow, message)
        else:
            break
    snow.close()
    logging.info("Ingest complete")
