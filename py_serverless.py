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

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.WARN)

def connect_snow():
    # Create the private key string
    private_key = os.getenv("PRIVATE_KEY")
    # Load the private key
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Connect to Snowflake
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-serverless'}
    )

def save_to_snowflake(snow, batch, temp_dir):
    logging.debug("Inserting batch to Snowflake")
    # Convert batch to DataFrame
    pandas_df = pd.DataFrame(batch, columns=[
        "TXID", "RFID", "RESORT", "PURCHASE_TIME", "EXPIRATION_TIME",
        "DAYS", "NAME", "ADDRESS", "PHONE", "EMAIL", "EMERGENCY_CONTACT"
    ])
    
    # Convert DataFrame to Arrow table and save as Parquet
    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path = os.path.join(temp_dir.name, f"{uuid.uuid1()}.parquet")
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    
    # Format the path with forward slashes for Snowflake
    formatted_path = out_path.replace(os.sep, '/')
    
    # Upload Parquet file to Snowflake stage
    snow.cursor().execute(f"PUT 'file://{formatted_path}' @%LIFT_TICKETS_PY_SERVERLESS")
    os.unlink(out_path)
    
    # Execute serverless task
    snow.cursor().execute("EXECUTE TASK LIFT_TICKETS_PY_SERVERLESS")
    logging.debug(f"{len(batch)} tickets staged")

if __name__ == "__main__":
    # Parse command-line arguments
    args = sys.argv[1:]
    batch_size = int(args[0]) if args else 1000  # Default batch size if none is specified
    
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    
    for message in sys.stdin:
        if message.strip() and message.startswith("{"):  # Skip empty lines
            try:
                record = json.loads(message)
                # Append data to batch
                batch.append((
                    record['txid'], record['rfid'], record['resort'],
                    record['purchase_time'], record['expiration_time'],
                    record['days'], record['name'], record['address'],
                    record['phone'], record['email'], record['emergency_contact']
                ))
                # Save batch to Snowflake if batch size limit is reached
                if len(batch) == batch_size:
                    save_to_snowflake(snow, batch, temp_dir)
                    batch = []
            except json.JSONDecodeError as e:
                logging.warning(f"Skipping invalid JSON line: {message.strip()} - Error: {e}")
        else:
            logging.warning(f"Skipping non-JSON line: {message}")
    
    # Process remaining records in batch
    if batch:
        save_to_snowflake(snow, batch, temp_dir)
    
    # Clean up temporary directory and close Snowflake connection
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")
