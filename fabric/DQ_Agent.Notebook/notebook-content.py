# Fabric notebook source

# %% [markdown]
# METADATA ********************

# %% [markdown]
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

# %% [markdown]
# CELL ********************

# %%
# %pip install gradio
import json
from pyspark.sql.functions import *
from openai import AzureOpenAI
from notebookutils import mssparkutils
import gradio as gr

# %% [markdown]
# METADATA ********************

# %% [markdown]
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# %% [markdown]
# CELL ********************

# %%
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

# %% [markdown]
# METADATA ********************

# %% [markdown]
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# %% [markdown]
# CELL ********************

# %%
#AI Agent Logic
key_vault_name = "https://azure-ai-portfolio-vault.vault.azure.net/"
api_key = mssparkutils.credentials.getSecret(key_vault_name, "AzureOpenAIKey")
endpoint = mssparkutils.credentials.getSecret(key_vault_name, "AzureOpenAIEndpoint")

# %%
client = AzureOpenAI(
    azure_endpoint=endpoint,
    api_key=api_key,
    api_version="2024-02-01"
)

# %%
deployment_model = "gpt-4o-agent"

# %%
#tool definition
reconciliation_tool = [
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
    }
]

# %% [markdown]
# setting system & user messages - The Function that Gradio will call

# %%
def chat_with_data_agent(user_message):
    messages = [
        {
            "role": "system",
            "content": "You are a Data Quality Assistant. You summarize daily sales reconciliation results. If the tool returns an 'error' indicating a file could not be loaded, politely inform the user that the daily CSV source file has not been dropped into the Lakehouse yet. If there are mismatches, format them clearly using bullet points or a small markdown table."
        },
        {
            "role": "user",
            "content": user_message
        }
    ]

    response = client.chat.completions.create(
        model=deployment_model,
        messages=messages,
        tools=reconciliation_tool,
        tool_choice="auto"
    )

    response_message = response.choices[0].message
    messages.append(response_message)

    if response_message.tool_calls:
        for tool_call in response_message.tool_calls:
            if tool_call.function.name == "reconciliation_menu":
                function_args = json.loads(tool_call.function.arguments)
                date_to_check = function_args.get("target_date")
                tool_output = reconciliation_menu(target_date=date_to_check)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": "reconciliation_menu",
                    "content": tool_output
                })

        final_response = client.chat.completions.create(
            model=deployment_model,
            messages=messages
        )
        return final_response.choices[0].message.content
    else:
        return response_message.content

# %% [markdown]
# METADATA ********************

# %% [markdown]
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# %% [markdown]
# CELL ********************

# %%
# Launch the Interactive UI
demo = gr.Interface(
    fn=chat_with_data_agent,
    inputs=gr.Textbox(lines=2, placeholder="Ask about daily sales reconciliation (e.g., 'Check data for May 27, 2024')..."),
    outputs=gr.Markdown(label="Agent Summary"),
    title="📊Data Quality Agent",
    description="Ask me to run PySpark reconciliation checks on the Lakehouse Delta tables!",
    flagging_mode="never"
)

# %%
demo.launch(share=True)

# %% [markdown]
# METADATA ********************

# %% [markdown]
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
