# 📊 AI-Driven Data Quality Agent for Microsoft Fabric

![PySpark](https://img.shields.io/badge/PySpark-Data_Processing-orange.svg)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric_OneLake-blue.svg)
![Azure OpenAI](https://img.shields.io/badge/Azure-OpenAI_GPT4o-brightgreen.svg)
![Gradio](https://img.shields.io/badge/UI-Gradio-ff69b4.svg)
![Azure Key Vault](https://img.shields.io/badge/Security-Azure_Key_Vault-blueviolet.svg)

## 📌 Executive Summary
An interactive, AI-powered data quality assistant designed to automate daily sales reconciliation. Built natively within **Microsoft Fabric**, this project uses a **GPT-4o agent** (via Azure OpenAI) to interpret natural language requests from users. The agent utilizes "Tool Calling" to autonomously execute a **PySpark** engine that compares raw daily sales CSV files residing in OneLake against processed Delta tables, instantly identifying volume and revenue discrepancies at the line-item level.

## 🏗️ Architecture & Workflow

1. **User Interface:** A user asks a natural language question via a **Gradio** web interface (e.g., *"Did the sales numbers tie out for May 27th?"*).
2. **Intent Translation (AI Agent):** **Azure OpenAI** processes the request, extracts the target date, and translates it into the required `YYMMDD` parameter using OpenAI Tool Calling.
3. **Secure Execution:** Credentials for the LLM are securely retrieved at runtime via **Azure Key Vault** using `mssparkutils`.
4. **Data Processing (PySpark):** The `reconciliation_menu` function executes in Microsoft Fabric:
   - Reads the raw source CSV (`Sales_Out_YYMMDD.csv`) from OneLake.
   - Filters and aggregates "settled" transaction volumes and revenues.
   - Queries the processed `settled_data` Delta Lake table.
   - Performs inner joins and anti-joins to isolate mismatched `order_id` and `product_id` records.
5. **Intelligent Summary:** The PySpark job returns a JSON payload to the AI Agent, which formats the raw variance data into a clear, human-readable markdown summary for the user.

## ✨ Key Technical Features
* **LLM Tool Calling:** Demonstrates advanced agentic behavior where the LLM does not just generate text, but acts as an orchestrator to trigger complex data pipelines.
* **Line-Item Reconciliation:** Uses PySpark DataFrames to perform multi-column grouping and variance calculations at scale.
* **Enterprise Security:** Hardcoded API keys are avoided; secrets are managed entirely through Azure Key Vault.
* **Lakehouse Integration:** Directly reads and writes to Microsoft Fabric OneLake using the `abfss://` protocol and Delta table formats.
* **Interactive Frontend:** Deploys a shareable Gradio UI directly from the notebook environment.

## 💻 Tech Stack
* **Compute / Data Engine:** PySpark
* **Data Storage:** Microsoft Fabric (OneLake, Delta Lake)
* **AI Orchestration:** Azure OpenAI (GPT-4o)
* **Security:** Azure Key Vault
* **Frontend:** Gradio

## 📂 Repository Structure
```text
├── notebooks/
│   └── Data_Quality_Agent.ipynb  # The primary Fabric notebook containing PySpark and AI logic
├── requirements.txt              # Required libraries (gradio, openai)
└── README.md
