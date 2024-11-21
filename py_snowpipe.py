import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager, StagedFile
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.INFO)

def connect_snow():
    # Formatting private key correctly
    private_key = os.getenv("PRIVATE_KEY")
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'}
    )

def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug("Inserting batch to Snowflake")
    # Convert batch to DataFrame and then to Arrow Table
    pandas_df = pd.DataFrame(batch, columns=[
        "TXID", "RFID", "RESORT", "PURCHASE_TIME", "EXPIRATION_TIME", "DAYS",
        "NAME", "ADDRESS", "PHONE", "EMAIL", "EMERGENCY_CONTACT"
    ])
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{uuid.uuid1()}.parquet"
    out_path = os.path.join(temp_dir.name, file_name)
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    
    # PUT and delete the temporary file
    snow.cursor().execute(f"PUT 'file://{out_path.replace(os.sep, '/')}' @%LIFT_TICKETS_PY_SNOWPIPE")
    os.unlink(out_path)
    
    # Use Snowpipe to ingest data
    response = ingest_manager.ingest_files([StagedFile(file_name, None)])
    logging.info(f"Response from Snowflake for file {file_name}: {response['responseCode']}")

if __name__ == "__main__":
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()

    # Configure SimpleIngestManager for Snowpipe
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    private_key = os.getenv("PRIVATE_KEY")
    ingest_manager = SimpleIngestManager(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=host,
        user=os.getenv("SNOWFLAKE_USER"),
        pipe='INGEST.INGEST.LIFT_TICKETS_PIPE',
        private_key=private_key
    )

    for message in sys.stdin:
        if message.strip():  # Ignore empty lines
            try:
                record = json.loads(message)
                batch.append((
                    record['txid'], record['rfid'], record["resort"], record["purchase_time"],
                    record["expiration_time"], record['days'], record['name'], record['address'],
                    record['phone'], record['email'], record['emergency_contact']
                ))
                if len(batch) == batch_size:
                    save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                    batch = []
            except json.JSONDecodeError as e:
                logging.warning(f"Skipping invalid JSON line: {message.strip()} - Error: {e}")
    
    # Save any remaining records in the batch
    if batch:
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)
    
    # Clean up
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")
