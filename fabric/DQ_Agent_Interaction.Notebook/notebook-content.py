# Fabric notebook source

# METADATA ********************

# META {
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

#setting system & user messages - The Function that Gradio will call

def chat_with_data_agent(user_message):
    messages = [
        {
            "role": "system",
            "content": "You are a Data Quality Assistant for the BI team. You summarize daily sales reconciliation results. If the tool returns an 'error' indicating a file could not be loaded, politely inform the BI team that the daily CSV source file has not been dropped into the Lakehouse yet. If there are mismatches, format them clearly using bullet points or a small markdown table."
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
                
                # Run the actual PySpark function
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Launch the Interactive UI!
demo = gr.Interface(
    fn=chat_with_data_agent,
    inputs=gr.Textbox(lines=2, placeholder="Ask about daily sales reconciliation (e.g., 'Check data for May 27, 2024')..."),
    outputs=gr.Markdown(label="Agent Summary"),
    title="📊 BI Data Quality Agent",
    description="Ask me to run PySpark reconciliation checks on the Lakehouse Delta tables!",
    allow_flagging="never"
)

demo.launch(share=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import random

print("--> [SYSTEM] Starting Mock Data Generation...")

# ==========================================
# 1. GENERATE 30 DAYS OF DELTA TABLE DATA
# ==========================================
delta_rows = []
start_date = datetime(2024, 5, 1)

for i in range(30):
    current_date = start_date + timedelta(days=i)
    num_orders = random.randint(2, 5) # 2 to 5 orders per day
    
    for j in range(1, num_orders + 1):
        order_id = f"ORD-{current_date.strftime('%m%d')}-{j}"
        num_items = random.randint(1, 5) # 1 to 5 products per order
        unique_product_numbers = random.sample(range(10000, 99999), num_items)
        
        for prod_num in unique_product_numbers:
            delta_rows.append(Row(
                order_id=order_id,
                product_id=f"PROD-{prod_num}",
                settlement_date=current_date.date(),
                processed_quantity=random.randint(1, 15),
                processed_revenue=round(random.uniform(15.0, 300.0), 2)
            ))

df_delta = spark.createDataFrame(delta_rows)

# Save as Delta Table (Overwrite ensures a clean slate every time you run this)
df_delta.write.format("delta").mode("overwrite").saveAsTable("portfolio_lakehouse.Sales.settled_data")
print("--> [SUCCESS] 30-Day Delta Table created successfully!")

# ==========================================
# 2. GENERATE 5 DAYS OF CSV SOURCE FILES
# ==========================================
base_path = "abfss://AI_Portfolio@onelake.dfs.fabric.microsoft.com/portfolio_lakehouse.Lakehouse/Files/SourceFiles/Sales_Out_"

# Map each day to a specific testing scenario
csv_scenarios = {
    "240501": "perfect_match",
    "240502": "mismatch_quantity",
    "240503": "mismatch_revenue",
    "240504": "missing_in_delta",
    "240505": "has_unsettled_records"
}

for date_str, scenario in csv_scenarios.items():
    dt = datetime.strptime(date_str, "%y%m%d").date()
    
    # Grab the EXACT records from the Delta table for this day to ensure matching IDs
    daily_delta_data = df_delta.filter(col("settlement_date") == dt).collect()
    
    csv_rows = []
    for row in daily_delta_data:
        # Baseline: Perfect copy of the Delta table data
        csv_qty = row["processed_quantity"]
        csv_rev = row["processed_revenue"]
        status = "settled"
        
        # INJECT ERRORS: Target only specific orders (e.g., any order ending in "-1")
        if scenario == "mismatch_quantity" and row["order_id"].endswith("-1"):
            csv_qty += 2 # CSV claims we sold 2 more items than the database shows
            
        elif scenario == "mismatch_revenue" and row["order_id"].endswith("-1"):
            csv_rev += 50.0 # CSV claims we made $50 more than the database shows
            
        # Add the mapped (and potentially altered) record to the CSV list
        csv_rows.append(Row(
            order_id=row["order_id"],
            product_id=row["product_id"],
            transaction_status=status,
            quantity=csv_qty,
            revenue=round(csv_rev, 2)
        ))
        
    # INJECT EXTRA ROWS (For missing and unsettled scenarios)
    if scenario == "missing_in_delta":
        # Add a record to the CSV that doesn't exist in the Delta table at all
        csv_rows.append(Row(
            order_id=f"ORD-GHOST-999", 
            product_id="PROD-99999", 
            transaction_status="settled", 
            quantity=5, 
            revenue=250.00
        ))
        
    elif scenario == "has_unsettled_records":
        # Add a record with 'unsettled' status (Your script should safely ignore this!)
        csv_rows.append(Row(
            order_id=f"ORD-IGNORE-888", 
            product_id="PROD-88888", 
            transaction_status="unsettled", 
            quantity=10, 
            revenue=1000.00
        ))

    # Save the daily CSV
    df_csv = spark.createDataFrame(csv_rows)
    file_path = f"{base_path}{date_str}.csv"
    
    # coalesce(1) forces it to write as a single CSV file instead of multiple partitions
    df_csv.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(file_path)
    print(f"--> [SUCCESS] CSV for {date_str} generated! (Scenario tested: {scenario})")

print("--> [SYSTEM] Data Generation Complete! You are ready to run the reconciliation script.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
