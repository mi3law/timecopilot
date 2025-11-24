import asyncio
import websockets
import json
import pandas as pd
import numpy as np

# An asyncio-safe queue to pass data between coroutines
data_queue_async = asyncio.Queue()

# A list to buffer records before turning them into a DataFrame
# This avoids frequent DataFrame re-creations which can be inefficient
# You might want to cap its size or process it after N items.
records_buffer = []
df_lock_async = asyncio.Lock() # For async-safe DataFrame access

# Global DataFrame
current_df_async = pd.DataFrame()


async def receive_websocket_data(uri, initial_message):
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {uri}")
            initial_message = json.dumps(initial_message)
            await websocket.send(initial_message)
            print(f"Sent: {initial_message}")

            while True:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    await data_queue_async.put(data) # Put data into the async queue
                    # print(f"Async: Received and queued: {data}")
                except websockets.exceptions.ConnectionClosedOK:
                    print("Connection closed gracefully.")
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    print(f"Connection closed with error: {e}")
                    break
                except json.JSONDecodeError:
                    print(f"Async: Received non-JSON message: {response}")
                except Exception as e:
                    print(f"Async: Error in receive loop: {e}")
    except Exception as e:
        print(f"Could not connect to WebSocket at {uri}: {e}")


async def update_dataframe_task(seconds=5, num_samples=False):
    """Periodically takes data from the queue and updates the DataFrame."""
    global current_df_async, records_buffer

    while True:
        # Check for new data in the queue
        while not data_queue_async.empty():
            item = await data_queue_async.get()
            records_buffer.append(item)
            # You can add logic here to process/discard older items if buffer gets too large

        # Update DataFrame periodically or when buffer reaches a certain size
        if records_buffer: # Only update if there's new data
            # Use a copy of the buffer to allow new items to be added while processing
            records_to_process = list(records_buffer)
            records_buffer.clear() # Clear the buffer after copying

            new_df_segment = pd.DataFrame(records_to_process)

            # Sample the dataframe, since the websocket gives too much information
            if num_samples:
                # Calculate evenly spaced indices
                evenly_spaced_indices = np.linspace(0, len(new_df_segment) - 1, num_samples).astype(int)

                # Select rows from the DataFrame using these indices
                new_df_segment = new_df_segment.iloc[evenly_spaced_indices]


            async with df_lock_async: # Acquire async lock
                if current_df_async.empty:
                    current_df_async = new_df_segment
                else:
                    current_df_async = pd.concat([current_df_async, new_df_segment], ignore_index=True)
                print(f"\nAsync: DataFrame updated. Current size: {len(current_df_async)} rows.")
                print("Async: Current DataFrame head:")
                print(current_df_async.tail()) # Show last few rows

        await asyncio.sleep(seconds) # Check and update every x seconds


async def main_async(initial_message):

    # --- URI Construction ---
    import os
    from dotenv import load_dotenv
    from urllib.parse import urlencode, urlunparse

    load_dotenv()  # This loads variables from .env into os.environ
    coinapi_apikey = os.getenv("coinapi_apikey")

    ### basic websocket
    # coinapi_wss = "ws.coinapi.io/v1"
    ### advanced websocket, enabling easy switching between data exchanges or sources
    exchange_name = "coinbase"
    coinapi_wss = exchange_name+".ws-ds.md.coinapi.io"

    base_uri_ws = coinapi_wss
    api_key_ws = coinapi_apikey
    query_params_ws = {
        # "": "BTC",
        "apikey": api_key_ws, # The parameter name might vary by API
    }
    encoded_query_params_ws = urlencode(query_params_ws)
    websocket_uri_ws = urlunparse(
        ("wss", base_uri_ws, "BTC", "", encoded_query_params_ws, "")
    )
    # --- End URI Construction ---

    # Start the WebSocket client and DataFrame updater as concurrent tasks
    consumer_task = asyncio.create_task(receive_websocket_data(websocket_uri_ws, initial_message))
    updater_task = asyncio.create_task(update_dataframe_task(seconds=5, num_samples=5))

    # Keep the event loop running
    await asyncio.gather(consumer_task, updater_task)

# Sending hello message as per: https://docs.coinapi.io/market-data/websocket-ds/general

# initial_message = {
#     "type": "hello",
#     "heartbeat": False,
#     "subscribe_data_type": ["quote"],
#     "subscribe_filter_asset_id": ["BTC"],
#     "subscribe_filter_symbol_id": [
#         "COINBASE_SPOT_BTC_USD",
#     ]
# }

initial_message = {
  "type": "hello",
  "heartbeat": False,
  "subscribe_data_type": ["trade"],
  "subscribe_filter_asset_id": ["BTC"],
  "subscribe_filter_symbol_id": [
    "COINBASE_SPOT_BTC_USD",
  ]
}

if __name__ == "__main__":
    try:
        asyncio.run(main_async(initial_message))
    except KeyboardInterrupt:
        print("\nAsync: Stopping program.")

        
df = current_df_async
df.to_csv('bitcoin_sample_data.csv', index=False)


# Import libraries
from timecopilot import TimeCopilot

# Load the dataset
# The DataFrame must include at least the following columns:
# - unique_id: Unique identifier for each time series (string)
# - ds: Date column (datetime format)
# - y: Target variable for forecasting (float format)
# The pandas frequency will be inferred from the ds column, if not provided.
# If the seasonality is not provided, it will be inferred based on the frequency. 
# If the horizon is not set, it will default to 2 times the inferred seasonality.

# Initialize the forecasting agent
# You can use any LLM by specifying the model parameter
tc = TimeCopilot(
    llm="openai:gpt-4o",
    retries=3,
)
# Generate forecast
# You can optionally specify the following parameters:
# - freq: The frequency of your data (e.g., 'D' for daily, 'M' for monthly)
# - h: The forecast horizon, which is the number of periods to predict
# - seasonality: The seasonal period of your data, which can be inferred if not provided
df = df.rename(columns={'time_exchange': 'ds', "ask_price": "y"})
result = tc.forecast(df=df, freq="MS")

df_subset = df.tail(500) # by rows
result = tc.forecast(df=df_subset, freq="MS")


# The output contains:
# - tsfeatures_results: List of calculated time series features
# - tsfeatures_analysis: Natural language analysis of the features
# - selected_model: The best performing model chosen
# - model_details: Technical details about the selected model
# - cross_validation_results: Performance comparison of different models
# - model_comparison: Analysis of why certain models performed better/worse
# - is_better_than_seasonal_naive: Boolean indicating if model beats baseline
# - reason_for_selection: Explanation for model choice
# - forecast: List of future predictions with dates
# - forecast_analysis: Interpretation of the forecast results
# - user_query_response: Response to the user prompt, if any
print(result.output)

# You can also access the forecast results in the same shape of the
# provided input dataframe.  
print(result.fcst_df)
