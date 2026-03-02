# 📊 AI-Driven Data Quality Agent for Microsoft Fabric

![PySpark](https://img.shields.io/badge/PySpark-Data_Processing-orange.svg)
![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric_Workspace-blue.svg)
![Azure OpenAI](https://img.shields.io/badge/Azure-OpenAI_GPT4o-brightgreen.svg)
![Gradio](https://img.shields.io/badge/UI-Gradio-ff69b4.svg)
![Azure Key Vault](https://img.shields.io/badge/Security-Azure_Key_Vault-blueviolet.svg)

## 📌 Executive Summary
An interactive, AI-powered data quality assistant designed to automate daily sales reconciliation. Built entirely within **Microsoft Fabric**, this project uses a **GPT-4o agent** (via Azure OpenAI) to interpret natural language requests. The agent utilizes "Tool Calling" to autonomously execute a **PySpark** engine that compares raw daily sales CSV files residing in OneLake against processed Delta tables, instantly identifying volume and revenue discrepancies at the line-item level.

## 🏗️ Architecture & Workflow

1. **User Interface:** A user asks a natural language question via a **Gradio** web interface (e.g., *"Check the reconciliation data for May 27th"*).
2. **Intent Translation (AI Agent):** **Azure OpenAI** processes the request, extracts the target date, and translates it into the required `YYMMDD` parameter using OpenAI Tool Calling.
3. **Secure Execution:** Credentials for the LLM are securely retrieved at runtime via **Azure Key Vault** using `mssparkutils`.
4. **Data Processing (PySpark):** The PySpark backend executes within Fabric to:
   - Read the raw source CSV from OneLake.
   - Filter and aggregate "settled" transaction volumes and revenues.
   - Query the processed Delta Lake table.
   - Perform inner joins and anti-joins to isolate mismatched records.
5. **Intelligent Summary:** The PySpark job returns a JSON payload to the AI Agent, which formats the raw variance data into a clear, human-readable markdown summary for the user.

## 📂 Fabric Workspace Structure (Git Integration)
This repository is synced directly with a Microsoft Fabric Workspace. The production-ready assets are structured as follows:

```text
├── fabric/
│   ├── AI-Agent-Env.Environment/         # Fabric Custom Environment (houses pip dependencies like gradio, openai)
│   ├── DQ_Agent.Notebook/                # 🌟 MAIN: Contains the Gradio UI, AI Agent logic, and tool calling orchestration
│   └── portfolio_lakehouse.Lakehouse/    # Fabric Lakehouse definition, schemas, and OneLake metadata
├── .gitignore
├── LICENSE
└── README.md
```

## ✨ Key Technical Features
* **Native Fabric Git Integration:** Demonstrates enterprise CI/CD practices by syncing workspace artifacts directly to GitHub.
* **LLM Tool Calling:** The LLM does not just generate text; it acts as an orchestrator to trigger complex PySpark data pipelines.
* **Line-Item Reconciliation:** Uses PySpark DataFrames to perform multi-column grouping and variance calculations at scale.
* **Enterprise Security:** Hardcoded API keys are avoided; secrets are managed entirely through Azure Key Vault.

## 💻 Tech Stack
* **Compute / Data Engine:** PySpark
* **Data Storage:** Microsoft Fabric (OneLake, Delta Lake)
* **AI Orchestration:** Azure OpenAI (GPT-4o)
* **Security:** Azure Key Vault
* **Frontend:** Gradio

## ⚙️ Setup & Deployment (Via Microsoft Fabric)
Because this project utilizes native Fabric features (OneLake `abfss://` paths and `mssparkutils`), it must be run within a Microsoft Fabric workspace, not a local Python environment.

**Prerequisites:**
* A Microsoft Fabric Workspace with Git integration enabled.
* An Azure OpenAI resource (with a deployed `gpt-4o` model).
* An Azure Key Vault storing your OpenAI endpoint and API key.

**Deployment Steps:**
1. Connect your Fabric Workspace to this GitHub repository via **Workspace Settings -> Git Integration**.
2. Sync the workspace to automatically generate the Lakehouse, Environment, and Notebooks.
3. Open `AI-Agent-Env.Environment` and click **Publish** to install the required `gradio` and `openai` libraries.
4. Open the `DQ_Agent.Notebook`. Update the `key_vault_name` and `source_path` variables with your specific Azure URI and OneLake paths.
5. Run the notebook to launch the interactive Gradio UI.

## 🚀 Usage Example
**User Input:**
> "Please check the reconciliation data for May 27, 2024."

**Agent Output:**
> "The reconciliation for May 27, 2024, shows a mismatch. 
> * **Source Revenue:** $15,000 | **Delta Revenue:** $14,500 (Variance: $500)
> * **Source Volume:** 150 units | **Delta Volume:** 145 units (Variance: 5 units)
> 
> **Mismatched Orders:**
> | Order ID | Product ID | Source Vol | Table Vol | Source Rev | Table Rev |
> |---|---|---|---|---|---|
> | 1045 | A-101 | 10 | 5 | $1000 | $500 |"

## 🤝 Connect with Me
* [LinkedIn](https://www.linkedin.com/in/nevedha-ayyanar/)

---
*If you find this project interesting, feel free to give it a ⭐!*
