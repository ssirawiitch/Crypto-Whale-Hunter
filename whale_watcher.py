import json
import websocket
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

KEY_FILE = 'key.json'
PROJECT_ID = 'cryptods'
DATASET_ID = 'crypto_ds'
TABLE_ID = 'raw_whale_trades'
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

WHALE_THRESHOLD_USD = 100000  # 100000 dollars

credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def upload_to_bigquery(data_list):
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
        )
        load_job = client.load_table_from_json(data_list, TABLE_FULL_ID, job_config=job_config)
        load_job.result() 
        print(f"✅ Data uploaded to BigQuery.")
    except Exception as e:
        print(f"❌ BigQuery Error: {e}")

def on_message(ws, message):
    """
    THis function will do every time a new message is received from the websocket.
    """
    data = json.loads(message)
    
    # Fetch price and amount from binance
    price = float(data['p']) # Price
    amount = float(data['q']) # Quantity
    symbol = data['s'] # Side flag   
    total_value = price * amount
    
    # check if the trade is a whale trade
    if total_value >= WHALE_THRESHOLD_USD:
        side = "SELL" if data['m'] else "BUY"
        
        print(f"🐋 WHALE DETECTED: {symbol} | ${total_value:,.2f} | {side} at {price}")

        whale_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "price": price,
            "total_value_usd": total_value,
            "exchange": "Binance",
            "from_address": "Binance_Hot_Wallet",
            "to_address": "Binance_Internal",
            "transaction_hash": str(data['a']) 
        }

        upload_to_bigquery([whale_data])

def on_error(ws, error):
    print(f"Connection Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection Closed. Reconnecting...")

def main():
    # URL only for ETHUSDT aggregate trades
    socket_url = "wss://stream.binance.com:9443/ws/ethusdt@aggTrade"
    
    print(f"🚀 Real-time Whale Tracker started (Threshold: ${WHALE_THRESHOLD_USD:,.0f})")
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    main()