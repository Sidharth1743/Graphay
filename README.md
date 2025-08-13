# **Graphay**: AI-Powered Financial Process Automation
Graphay, is an AI-powered financial process automation system designed to streamline and manage the entire lifecycle of an invoice, from initial receipt to final payment confirmation. Leveraging the LangGraph library, this system orchestrates a multi-step workflow, integrating various services and including human-in-the-loop checkpoints to ensure accuracy and compliance.
---

## Project Overview & Core Functionality

Graphay automates a complex accounts payable workflow by creating a stateful agent that intelligently manages the process for each invoice. The core of the system is a **Python**-based application using **LangGraph** to define and execute the workflow as a stateful graph. This approach allows for robust, concurrent processing of multiple invoices while handling asynchronous human-in-the-loop tasks efficiently.

### Key features include:

  * **Automated Invoice Ingestion**: Polling a dedicated email inbox for new invoices.
  * **AI-Powered Data Extraction**: Using a **Document Intelligence Service** to parse and extract key data from invoices.
  * **Automated Validation & Follow-Up**: Validating extracted data and automatically sending follow-up emails for missing information.
  * **Human-in-the-Loop Approvals**: Staging invoices for approval and monitoring a **Discord server** for an approval decision from a designated team.
  * **Payment Verification**: Monitoring for payment confirmation and verifying transactions via a **public API** (e.g., Etherscan).
  * **Centralized Tracking**: Recording all invoice data, status, and decisions in a **Google Sheets** tracking system.

---

## **Workflow Overview**

### **Automated Steps**

1. **Invoice Ingestion** → Detects new invoice emails & downloads files.
2. **AI-Powered Extraction** → Extracts vendor, date, amounts, line items, payment details.
3. **Validation** → Checks for missing data, requests additional info if needed.
4. **Approval Staging** → Logs invoice to Google Sheets & posts request in Discord.
5. **Human Approval** → Approvers approve/reject & provide cost center.
6. **Decision Recording** → Updates Google Sheets with decision.
7. **Payment Confirmation** → Waits for transaction ID, verifies via Etherscan.
8. **Finalization** → Records payment confirmation & marks invoice complete.

---
## **Setup & Installation**
* Python
* Google Cloud project with:
  * Project Creation in Google cloud [YouTube](https://youtu.be/0eZYbw9QgyE)
  * Gmail API enabled [YouTube](https://youtu.be/FxoQ41RGmeI)
  * Google Drive API enabled [YouTube](https://youtu.be/5j73RBgogAs)
  * Google Sheets API enabled [Step 1](https://youtu.be/WzNnt_eoX38) [Step 2](https://youtu.be/s2kBmyN5X9E)
* Discord bot with channel & thread access
* Etherscan API key

### **To Get Credentials**

| Variable                      | How to Get                                                                                                                                                                                                              |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **DISCORD\_TOKEN**            | Discord Developer Portal → Applications → Select your bot → **Bot** → Reset Token → Copy                                                                                                                                |
| **DISCORD\_CHANNEL\_ID**      | In Discord server, right-click on the channel → **Copy Channel ID**                                                                                                                                                     |
| **APPROVING\_TEAM\_ROLE\_ID** | Discord server → Server Settings → Roles → Create Role → Set Permissions: *View Channel, Manage Channel, Manage Role, Create Expressions, Create Invite, Manage Messages, Manage Threads & Posts, Read Message History* |
| **SPREADSHEET\_ID**           | In Google Sheets URL, copy the value between `d/` and `/edit`                                                                                                                                                           |
| **GDRIVE\_FOLDER\_ID**        | In Google Drive folder URL, copy the value between `/folder/` and the next slash                                                                                                                                        |

---

## **Installation**

```bash
# Clone the repository
git clone https://github.com/Sidharth1743/Graphay.git
cd graphay

# Install dependencies
pip install -r req.txt

# Copy environment variables
cp .env.example .env
```
---
## Technical Details & Design

### Orchestration with LangGraph

The heart of this system is a **stateful graph** built with **LangGraph**. The graph's state is a dictionary that holds all relevant invoice data at each step (e.g., vendor name, status, approval decision, transaction ID). Each step in the workflow is a **node** in the graph, with edges defining the flow based on the current state.

This design enables:

  * **Asynchronous Checkpoints**: Nodes like `await_approval` are designed to pause the graph's execution for a specific invoice until a condition is met (e.g., a Discord message is received). This non-blocking approach ensures the system can continue processing other invoices concurrently.
  * **Error Handling & Retries**: The graph can be configured with error-handling logic to manage failed tool calls or invalid data, routing the process to a specific node for resolution.

---
## 🤝 Contributing
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to branch
5. Open a Pull Request
