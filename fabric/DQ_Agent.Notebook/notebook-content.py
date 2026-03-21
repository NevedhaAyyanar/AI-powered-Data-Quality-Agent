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

#%pip install gradio
import json
import re
import os
import logging
from datetime import datetime
from pyspark.sql.functions import *
from openai import AzureOpenAI
from notebookutils import mssparkutils
import gradio as gr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# File-based logging (works in Gradio background threads unlike print)
log_path = "/tmp/agent_log.txt"
session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
lakehouse_log_path = f"abfss://AI_Portfolio@onelake.dfs.fabric.microsoft.com/portfolio_lakehouse.Lakehouse/Files/Logs/agent_log_{session_id}.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(log_path, mode="a"), logging.StreamHandler()]
)
logger = logging.getLogger("DQAgent")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sync_log_to_lakehouse():
    """Copy log file to Lakehouse so it persists after session ends."""
    try:
        with open(log_path, "r") as f:
            mssparkutils.fs.put(lakehouse_log_path, f.read(), True)
    except Exception:
        pass


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#reconciliation code (Pyspark) - Updated for new schema
def reconciliation_menu(target_date: str) -> str:
    """compares settled transactions numbers from daily files
    against the delta table"""
    print(f"--> [SYSTEM] Starting Pyspark job for {target_date}...")
    source_path = f"abfss://AI_Portfolio@onelake.dfs.fabric.microsoft.com/portfolio_lakehouse.Lakehouse/Files/SourceFiles/Sales_Out_{target_date}.csv"
    try:
        df_csv = spark.read.format("csv").option("header","true").option("inferSchema","true").load(source_path)
    except Exception as e:
        return json.dumps({"error": f"Could not load source file for date {target_date}. Details: {str(e)}"})
    
    # getting settled data from source file
    df_settled = df_csv.filter(col("transaction_status") == "settled")
    df_settled = df_settled.fillna(0, subset=["quantity", "revenue"])

    validation_metrics_source = df_settled.agg(
        round(sum("quantity"), 2).alias("volume"),
        round(sum("revenue"), 2).alias("revenue")
    ).collect()[0]

    source_volume = validation_metrics_source["volume"] or 0
    source_revenue = validation_metrics_source["revenue"] or 0

    # getting from delta table in lakehouse
    date_filter = to_date(lit(target_date), "yyMMdd")
    df_processed_data = spark.read.format("delta").table("portfolio_lakehouse.Sales.settled_data")
    df_processed = df_processed_data.filter(col("settlement_date") == date_filter)

    validation_metrics_delta = df_processed.agg(
        round(sum("processed_quantity"), 2).alias("volume"),
        round(sum("processed_revenue"), 2).alias("revenue")
    ).collect()[0]

    delta_volume = validation_metrics_delta["volume"] or 0
    delta_revenue = validation_metrics_delta["revenue"] or 0

    # compare numbers
    if source_volume == delta_volume and source_revenue == delta_revenue:
        return json.dumps({
            "status": "match",
            "date": target_date,
            "message": "All settled transactions match successfully between CSV and Delta Table."
        })

    # when mismatch happens
    df_settled_orders = df_settled.groupBy("order_id", "product_id").agg(
        round(sum("quantity"), 2).alias("settled_volume"),
        round(sum("revenue"), 2).alias("settled_revenue")
    )
    df_processed_orders = df_processed.groupBy("order_id", "product_id").agg(
        round(sum("processed_quantity"), 2).alias("processed_volume"),
        round(sum("processed_revenue"), 2).alias("processed_revenue")
    )
    joining_conditions = (
        (df_settled_orders["order_id"] == df_processed_orders["order_id"]) &
        (df_settled_orders["product_id"] == df_processed_orders["product_id"])
    )

    df_mismatched = df_settled_orders.join(df_processed_orders, on=joining_conditions, how="inner").filter(
        (df_settled_orders["settled_volume"] != df_processed_orders["processed_volume"]) |
        (df_settled_orders["settled_revenue"] != df_processed_orders["processed_revenue"])
    ).select(
        df_settled_orders["order_id"],
        df_settled_orders["product_id"],
        df_settled_orders["settled_volume"].alias("source_volume"),
        df_processed_orders["processed_volume"].alias("table_volume"),
        df_settled_orders["settled_revenue"].alias("source_revenue"),
        df_processed_orders["processed_revenue"].alias("table_revenue")
    )

    mismatched_order_details = [
        {
            "order_id": row["order_id"],
            "product_id": row["product_id"],
            "source_volume": row["source_volume"],
            "table_volume": row["table_volume"],
            "source_revenue": row["source_revenue"],
            "table_revenue": row["table_revenue"]
        }
        for row in df_mismatched.collect()
    ]

    return json.dumps({
        "status": "mismatch",
        "date": target_date,
        "source_revenue": source_revenue,
        "delta_revenue": delta_revenue,
        "source_volume": source_volume,
        "delta_volume": delta_volume,
        "revenue_variance": source_revenue - delta_revenue,
        "volume_variance": source_volume - delta_volume,
        "mismatched_orders": mismatched_order_details
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def list_available_dates() -> str:
    """Lists all available source files in the Lakehouse"""
    source_files_path = "abfss://AI_Portfolio@onelake.dfs.fabric.microsoft.com/portfolio_lakehouse.Lakehouse/Files/SourceFiles/"
    files = mssparkutils.fs.ls(source_files_path)
    dates = []
    for file in files:
        if file.isDir:
            match = re.search(r"Sales_Out_(\d{6})\.csv", file.name)
            if match:
                dates.append(match.group(1))
    dates.sort()
    return json.dumps({"available_dates": dates, "total_dates:": len(dates)})



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

key_vault_name = "https://azure-ai-portfolio-vault.vault.azure.net/"
api_key = mssparkutils.credentials.getSecret(key_vault_name, "AzureOpenAIKey")
endpoint = mssparkutils.credentials.getSecret(key_vault_name, "AzureOpenAIEndpoint")

def chat_with_data_agent(user_message, history):
    try:
        logger.info(f"={'='*50}")
        logger.info(f"USER: {user_message}")
        logger.info(f"HISTORY LENGTH: {len(history)} messages")

        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-02-01"
        )
        deployment_model = "gpt-4o-agent"

        agent_tools = [
            {
                "type": "function",
                "function": {
                    "name": "reconciliation_menu",
                    "description": "Compares settled transactions volume and revenue from daily CSV source file against the processed Delta table in the Lakehouse to identify mismatches or variances.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "target_date": {
                                "type": "string",
                                "description": "The date to run the reconciliation for. MUST be formatted as YYMMDD (e.g., '231025' for October 25, 2023)."
                            }
                        },
                        "required": ["target_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "list_available_dates",
                    "description": "Lists all available source file dates in the Lakehouse. Use this when the user asks about available dates, recent data, latest reconciliation, or any vague date reference.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }
            }
        ]

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a Data Quality Assistant. You help users run and understand daily sales reconciliation results.\n\n"
                    "TOOLS:\n"
                    "1. list_available_dates — returns all available source file dates from the Lakehouse.\n"
                    "2. reconciliation_menu — runs reconciliation for a specific date (YYMMDD format).\n\n"
                    "RULES (follow strictly):\n"
                    "- You do NOT know what dates are available. NEVER guess or assume dates.\n"
                    "- When the user says 'latest', 'recent', 'last few days', 'all', or any vague date reference, "
                    "you MUST call list_available_dates FIRST before doing anything else.\n"
                    "- Only call reconciliation_menu with a date that was either explicitly provided by the user in YYMMDD format, "
                    "or returned by list_available_dates.\n"
                    "- 'yesterday', 'today', 'last week' etc. refer to ACTUAL calendar dates — not 'most recent available'. "
                    "If the user says 'yesterday' and that calendar date is not in the available dates list, tell the user "
                    "that date is not available and show them which dates ARE available. Do NOT substitute a different date.\n"
                    "- If you are unsure about the user's intent, date, or any detail, ask a clarifying question — do NOT decide on your own.\n"
                    "- If reconciliation_menu returns an error about a file not loading, tell the user the CSV source file has not been dropped into the Lakehouse yet.\n"
                    "- Format mismatches clearly using bullet points or a small markdown table."
                )
            }
        ]

        # rebuild conversation history from previous turns
        for past_message in history:
            messages.append({"role": past_message["role"], "content": past_message["content"]})

        messages.append({"role": "user", "content": user_message})

        logger.info("CALLING OpenAI (initial)...")
        response = client.chat.completions.create(
            model=deployment_model,
            messages=messages,
            tools=agent_tools,
            tool_choice="auto",
            temperature=0
        )

        response_message = response.choices[0].message
        messages.append(response_message)

        has_tools = bool(response_message.tool_calls)
        logger.info(f"LLM RESPONSE: tool_calls={has_tools}, content={response_message.content[:100] if response_message.content else 'None'}")

        # tool call loop — handles sequential tool calls (e.g., list dates then reconcile)
        loop_count = 0
        while response_message.tool_calls:
            loop_count += 1
            for tool_call in response_message.tool_calls:
                logger.info(f"TOOL CALL [{loop_count}]: {tool_call.function.name}({tool_call.function.arguments})")
                if tool_call.function.name == "reconciliation_menu":
                    function_args = json.loads(tool_call.function.arguments)
                    tool_output = reconciliation_menu(target_date=function_args["target_date"])
                elif tool_call.function.name == "list_available_dates":
                    tool_output = list_available_dates()
                else:
                    tool_output = json.dumps({"error": f"Unknown tool: {tool_call.function.name}"})

                logger.info(f"TOOL RESULT [{tool_call.function.name}]: {tool_output[:200]}")
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": tool_call.function.name,
                    "content": tool_output
                })

            logger.info(f"CALLING OpenAI (after tool loop {loop_count})...")
            response = client.chat.completions.create(
                model=deployment_model,
                messages=messages,
                tools=agent_tools,
                tool_choice="auto",
                temperature=0
            )
            response_message = response.choices[0].message
            messages.append(response_message)
            logger.info(f"Whole convo: {messages}")
            logger.info(f"LLM RESPONSE [{loop_count}]: tool_calls={bool(response_message.tool_calls)}, content={response_message.content[:100] if response_message.content else 'None'}")

        final = response_message.content or "I processed your request but didn't generate a response. Please try again."
        logger.info(f"FINAL RESPONSE: {final[:150]}")
        sync_log_to_lakehouse()
        return final

    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        sync_log_to_lakehouse()
        return f"⚠️ Something went wrong: {str(e)}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Launch the Interactive UI
demo = gr.ChatInterface(
    fn=chat_with_data_agent,
    title="📊Data Quality Agent",
    description="Ask me to run PySpark reconciliation checks on the Lakehouse Delta tables!",
    textbox=gr.Textbox(placeholder="Ask about daily sales reconciliation (e.g., 'Check data for May 27, 2024')...", scale=7)
)

demo.launch(share=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
