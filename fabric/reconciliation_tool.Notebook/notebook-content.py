# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dca2f9e1-bd39-45f5-b70f-849bf07ca21f",
# META       "default_lakehouse_name": "portfolio_lakehouse",
# META       "default_lakehouse_workspace_id": "e748d594-cc84-40e7-a05b-2240ac9c7241",
# META       "known_lakehouses": [
# META         {
# META           "id": "dca2f9e1-bd39-45f5-b70f-849bf07ca21f"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "03b8d8de-2939-addc-4a2d-6d2d1898cc5c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

#%pip install openai
import json
from pyspark.sql.functions import *
from openai import AzureOpenAI
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#reconciliation code (Pyspark)
def reconciliation_menu(target_date: str) -> str:
    """compares settled transactions numbers from daily files
    against the delta table"""
    print(f"--> [SYSTEM] Starting Pyspark job for {target_date}...")
    source_path = f"abfss://AI_Portfolio@onelake.dfs.fabric.microsoft.com/portfolio_lakehouse.Lakehouse/Files/SourceFiles/Sales_Out_SIP_{target_date}.csv"
    try:
        df_csv = spark.read.format("csv").option("header","true").option("inferSchema","true").load(source_path)
    except Exception as e:
        return json.dumps({"error": f"Could not load source file for date {target_date}. Details: {str(e)}"})
    
    #getting settled data from source file
    df_settled = df_csv.filter(
        (col("DailyClosing") == "Y") &
        (col("MNEPRCSTA") == "SMF") &
        ((col("SUBPRCSTA1").isNull()) | (col("SUBPRCSTA1") == ""))
    )
    df_settled = df_settled.fillna(0,subset=["Adj_UNI_SUU", "Adj_FINPRI"])

    validation_metrics_source = df_settled.agg(
        round(sum("Adj_UNI_SUU"),2).alias("volume"),
        round(sum("Adj_FINPRI"),2).alias("revenue")
    ).collect()[0]

    source_volume = validation_metrics_source["volume"] or 0
    source_revenue = validation_metrics_source["revenue"] or 0

    #getting from delta table in lakehouse
    date_filter = to_date(lit(target_date), "yyMMdd")
    df_processed_data = spark.read.format("delta").table("portfolio_lakehouse.Sales.settleddata")
    df_processed = df_processed_data.filter(col("SettlementDate") == date_filter)

    validation_metrics_delta = df_processed.agg(
        round(sum("Adj_UNI_SUU"),2).alias("volume"),
        round(sum("Adj_FINPRI"),2).alias("revenue")
    ).collect()[0]

    delta_volume = validation_metrics_delta["volume"] or 0
    delta_revenue = validation_metrics_delta["revenue"] or 0

    #compare numbers
    if source_volume == delta_volume and source_revenue == delta_revenue:
        return json.dumps({
            "status": "match",
            "date": target_date,
            "message": "All settled transactions match successfully between CSV and Delta Table."
        }
        )

    #when mismatch happens
    # joining_conditions = (
    #     (df_settled["DOCNUM"]==df_processed["Docnum"]) & 
    #     (df_settled["OUTNUM"]==df_processed["OUTNUM"]) & 
    #     (df_settled["ARTNUM"] == df_processed["ARTNUM"]))

    # df_missing = df_settled.join(df_processed, on=joining_conditions, how="left_anti")\
    #                         .select(df_settled["DOCNUM"], df_settled["OUTNUM"], df_settled["ARTNUM"], df_settled["Adj_UNI_SUU"], df_settled["Adj_FINPRI"])
    
    #when mismatch happens
    
    df_settled_orders = df_settled.groupBy("DOCNUM").agg(
        round(sum("Adj_UNI_SUU"),2).alias("settled_volume"),
        round(sum("Adj_FINPRI"),2).alias("settled_revenue")
    )
    df_processed_orders = df_processed.groupBy("DOCNUM").agg(
        round(sum("Adj_UNI_SUU"),2).alias("processed_volume"),
        round(sum("Adj_FINPRI"),2).alias("processed_revenue")
    )
    joining_conditions = (
        (df_settled_orders["DOCNUM"]==df_processed_orders["Docnum"])) #& 
        # (df_settled["OUTNUM"]==df_processed["OUTNUM"]) & 
        # (df_settled["ARTNUM"] == df_processed["ARTNUM"])

    df_mismatched = df_settled_orders.join(df_processed_orders, on=joining_conditions, how="inner").filter(
        (df_settled_orders["settled_volume"] != df_processed_orders["processed_volume"]) |
        (df_settled_orders["settled_revenue"] != df_processed_orders["processed_revenue"]))\
        .select(
            df_settled_orders["DOCNUM"], 
            df_settled_orders["settled_volume"].alias("source_volume"), 
            df_processed_orders["processed_volume"].alias("table_volume"), 
            df_settled_orders["settled_revenue"].alias("source_revenue"),
            df_processed_orders["processed_revenue"].alias("table_revenue"))
    

    mismatched_order_details = [
        {
            "DOCNUM": row["DOCNUM"],
            "source_volume":row["source_volume"], 
            "table_volume":row["table_volume"],
            "source_revenue":row["source_revenue"],
            "table_revenue":row["table_revenue"]
        }
        for row in df_mismatched.collect()
    ]

    return json.dumps(
        {
            "status": "mismatch",
            "date": target_date,
            "source_revenue": source_revenue,
            "delta_revenue": delta_revenue,
            "source_volume": source_volume,
            "delta_volume": delta_volume,
            "revenue_variance": source_revenue - delta_revenue,
            "volume_variance": source_volume - delta_volume,
            "mismatched_orders": mismatched_order_details
        }
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # 1. Define the exact date you want to test (using your YYMMDD format)
# test_date = "240527"

# # 2. Call the function and store the resulting JSON string
# result_string = reconciliation_menu(test_date)

# # 3. Parse the string back into a Python dictionary and print it beautifully
# import json

# try:
#     # Load the string into a dictionary
#     parsed_result = json.loads(result_string)
    
#     # Print it with a 4-space indent so it's highly readable
#     print(json.dumps(parsed_result, indent=4))
    
# except Exception as e:
#     # Just in case the function returned a raw error string instead of JSON
#     print("Error parsing result:")
#     print(result_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#AI Agent Logic
#getting api key and endpoint from key vault
#key_vault_name = "azure-ai-portfolio-vault"
key_vault_name = "https://azure-ai-portfolio-vault.vault.azure.net/"
api_key = mssparkutils.credentials.getSecret(key_vault_name, "AzureOpenAIKey")
endpoint = mssparkutils.credentials.getSecret(key_vault_name, "AzureOpenAIEndpoint")

client = AzureOpenAI(
    azure_endpoint = endpoint,
    api_key = api_key,
    api_version = "2024-02-01"
)

deployment_model = "gpt-4o-agent"

#tool definition
reconciliation_tool = [
    {
        "type": "function",
        "function": {
            "name": "reconciliation_menu",
            "description": "Compares settled transactions volume and revenue from daily CSV source files against the processed Delta table in the Lakehouse to identify mismatches or variances.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_date":{
                        "type": "string",
                        "description": "The date to run the reconciliation for. MUST be formatted as YYMMDD (e.g., '231025' for October 25, 2023)."
                    }
                },
                "required": ["target_date"]
            }
        }
    }
]

#setting system & user messages

messages = [
    {
        "role": "system",
        "content": "You are a Data Quality Assistant for the BI team. You summarize daily sales reconciliation results. If the tool returns an 'error' indicating a file could not be loaded, politely inform the BI team that the daily CSV source file has not been dropped into the Lakehouse yet. If there are mismatches, format them clearly using bullet points or a small markdown table."

    },
    {
        "role": "user",
        "content": "Can you run the reconciliation check for May 24th, 2024?"
    }
]


print("Sending request to GPT-4o...")

response = client.chat.completions.create(
    model = deployment_model,
    messages=messages,
    tools = reconciliation_tool,
    tool_choice="auto"
)

response_message = response.choices[0].message
messages.append(response_message)

if response_message.tool_calls:
    for tool_call in response_message.tool_calls:
        if tool_call.function.name == "reconciliation_menu":
            
            function_args = json.loads(tool_call.function.arguments)
            date_to_check = function_args.get("target_date")
            
            print(f"--> [Agent] Triggering PySpark job for date: {date_to_check}")
            
            tool_output = reconciliation_menu(target_date=date_to_check)
            
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": "reconciliation_menu",
                "content": tool_output
            })

    print("Generating final BI summary...")

    final_response = client.chat.completions.create(
        model=deployment_model,
        messages=messages
    )

    print("\n================ AGENT SUMMARY ================\n")
    print(final_response.choices[0].message.content)
    print("\n===============================================")
else:
    print("\n" + response_message.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
