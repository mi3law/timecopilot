import os
from dotenv import load_dotenv
from urllib.parse import urlencode, urlunparse

load_dotenv()  # This loads variables from .env into os.environ
coinapi_apikey = os.getenv("coinapi_apikey")

### basic websocket
# coinapi_wss = "ws.coinapi.io/v1"
### their advanced websocket, enabling easily switching between data sources
exchange_name = "coinbase"
coinapi_wss = exchange_name+".ws-ds.md.coinapi.io"

# --- URI Construction (as shown above) ---
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


import asyncio
import websockets
import json

async def connect_to_websocket_with_apikey(uri):
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {uri}")

            # The API key has already been sent in the URI query parameter during connection.
            # You might send an initial message if the API expects one.
            initial_message = json.dumps({
                                         "type": "hello",
                                         "heartbeat": False,
                                         "subscribe_data_type": ["quote"],
                                         "subscribe_filter_asset_id": ["BTC"]
                                         })
            await websocket.send(initial_message)
            print(f"Sent: {initial_message}")

            while True:
                try:
                    response = await websocket.recv()
                    print(f"Received: {response}")
                    # Process the received message
                except websockets.exceptions.ConnectionClosedOK:
                    print("Connection closed gracefully.")
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    print(f"Connection closed with error: {e}")
                    break
    except Exception as e:
        print(f"Could not connect to WebSocket at {uri}: {e}")

if __name__ == "__main__":
    asyncio.run(connect_to_websocket_with_apikey(websocket_uri_ws))



# # Import libraries
# import pandas as pd
# from timecopilot import TimeCopilot

# # Load the dataset
# # The DataFrame must include at least the following columns:
# # - unique_id: Unique identifier for each time series (string)
# # - ds: Date column (datetime format)
# # - y: Target variable for forecasting (float format)
# # The pandas frequency will be inferred from the ds column, if not provided.
# # If the seasonality is not provided, it will be inferred based on the frequency. 
# # If the horizon is not set, it will default to 2 times the inferred seasonality.
# df = pd.read_csv("https://timecopilot.s3.amazonaws.com/public/data/air_passengers.csv")

# # Initialize the forecasting agent
# # You can use any LLM by specifying the model parameter
# tc = TimeCopilot(
#     llm="openai:gpt-4o",
#     retries=3,
# )

# # Generate forecast
# # You can optionally specify the following parameters:
# # - freq: The frequency of your data (e.g., 'D' for daily, 'M' for monthly)
# # - h: The forecast horizon, which is the number of periods to predict
# # - seasonality: The seasonal period of your data, which can be inferred if not provided
# result = tc.forecast(df=df, freq="MS")

# # The output contains:
# # - tsfeatures_results: List of calculated time series features
# # - tsfeatures_analysis: Natural language analysis of the features
# # - selected_model: The best performing model chosen
# # - model_details: Technical details about the selected model
# # - cross_validation_results: Performance comparison of different models
# # - model_comparison: Analysis of why certain models performed better/worse
# # - is_better_than_seasonal_naive: Boolean indicating if model beats baseline
# # - reason_for_selection: Explanation for model choice
# # - forecast: List of future predictions with dates
# # - forecast_analysis: Interpretation of the forecast results
# # - user_query_response: Response to the user prompt, if any
# print(result.output)

# # You can also access the forecast results in the same shape of the
# # provided input dataframe.  
# print(result.fcst_df)

# """
#         unique_id         ds       Theta
# 0   AirPassengers 1961-01-01  440.969208
# 1   AirPassengers 1961-02-01  429.249237
# 2   AirPassengers 1961-03-01  490.693176
# ...
# 21  AirPassengers 1962-10-01  472.164032
# 22  AirPassengers 1962-11-01  411.458160
# 23  AirPassengers 1962-12-01  462.795227
# """



# # Ask specific questions about the forecast
# result = tc.forecast(
#     df=df,
#     freq="MS",
#     query="how many air passengers are expected in the next 12 months?",
# )


# result = tc.forecast(
#     df=df,
#     freq="MS",
#     query="Which months have peak passenger traffic?",
# )


# result = tc.forecast(
#     df=df,
#     freq="MS",
#     query="Please show me the plot of the forecasts.",
# )


# # The output will include:
# # - All the standard forecast information
# # - user_query_response: Detailed answer to your specific question
# #   analyzing the forecast in the context of your query
# print(result.output.user_query_response)

# """
# The total expected air passengers for the next 12 months is approximately 5,919.
# """


# # # I need to understand passenger demand for the next 12 months. Using this dataset: https://timecopilot.s3.amazonaws.com/public/data/air_passengers.csv Please give me the total number for the next year, and highlight any peaks, seasonal swings, or potential risks that could impact planning.