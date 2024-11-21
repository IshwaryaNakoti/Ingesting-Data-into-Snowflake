import os
import sys
import logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.INFO)


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
        session_parameters={'QUERY_TAG': 'py-copy-into'}, 
    )


def save_to_snowflake(snow, batch, temp_dir):
    logging.debug("Inserting batch to Snowflake")
    pandas_df = pd.DataFrame(
        batch,
        columns=[
            "TXID",
            "RFID",
            "RESORT",
            "PURCHASE_TIME",
            "EXPIRATION_TIME",
            "DAYS",
            "NAME",
            "ADDRESS",
            "PHONE",
            "EMAIL",
            "EMERGENCY_CONTACT",
        ],
    )
    arrow_table = pa.Table.from_pandas(pandas_df)
    
    # Properly formatted file path for Windows
    temp_file = os.path.join(temp_dir.name, f"{uuid.uuid1()}.parquet")
    pq.write_table(arrow_table, temp_file, use_dictionary=False, compression="SNAPPY")
    
    # `PUT` command with the correct path
    put_command = f"PUT 'file://{temp_file.replace(os.sep, '/')}' @%LIFT_TICKETS_PY_COPY_INTO"
    snow.cursor().execute(put_command)
    os.unlink(temp_file)
    
    # Execute `COPY INTO` to move data into Snowflake
    snow.cursor().execute(
        "COPY INTO LIFT_TICKETS_PY_COPY_INTO FILE_FORMAT=(TYPE='PARQUET') MATCH_BY_COLUMN_NAME=CASE_SENSITIVE PURGE=TRUE"
    )
    logging.debug(f"Inserted {len(batch)} tickets")


if __name__ == "__main__":
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()

    for message in sys.stdin:
        message = message.strip()  # Remove any extraneous whitespace or newline
        if message and message.startswith("{"):  # Ensure message is not empty
            try:
                record = json.loads(message)
                batch.append(
                    (
                        record["txid"],
                        record["rfid"],
                        record["resort"],
                        record["purchase_time"],
                        record["expiration_time"],
                        record["days"],
                        record["name"],
                        record["address"],
                        record["phone"],
                        record["email"],
                        record["emergency_contact"],
                    )
                )
                if len(batch) == batch_size:
                    save_to_snowflake(snow, batch, temp_dir)
                    batch = []
            except json.JSONDecodeError as e:
                logging.warning(f"Skipping invalid JSON line: {message} - Error: {e}")
        else:
            logging.warning(f"Skipping non-JSON line: {message}")

    # Process remaining records in batch
    if batch:
        save_to_snowflake(snow, batch, temp_dir)

    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")
