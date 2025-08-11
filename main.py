from colorama import Fore, Style
from src.graph import Workflow
from dotenv import load_dotenv
import os
import time

# Load all env variables
load_dotenv()

# config 
config = {'recursion_limit': 100}

workflow = Workflow()
app = workflow.app

initial_state = {
    "emails": [],
    "current_email": {
      "id": "",
      "threadId": "",
      "messageId": "",
      "references": "",
      "sender": "",
      "subject": "",
      "body": "",
      "attachments": []
    },
    "email_category": "",
    "generated_email": "",
    "rag_queries": [],
    "retrieved_documents": "",
    "writer_messages": [],
    "sendable": False,
    "trials": 0,
    "invoice_emails": [],
    "processed_attachments": [],
    "gdrive_upload_result": {},
    "invoice_processing_result": {},
    "discord_state": {
        "thread_id": None,
        "message_id": None,
        "approval_status": "pending",
        "cost_center": None,
        "approver": None,
        "rejection_reason": None,
        "payment_status": "pending",
        "transaction_id": None,
        "reminder_count": 0,
        "last_reminder": None,
        "sla_message_id": None,
        "approval_sla_hours": 24
    }
}

# Run the automation
print(Fore.GREEN + "Starting workflow..." + Style.RESET_ALL)

try:
    for output in app.stream(initial_state, config):
        for key, value in output.items():
            print(Fore.CYAN + f"Finished running: {key}:" + Style.RESET_ALL)

    # Keep the process alive briefly to allow background workers (invoice processing / Discord agent)
    keep_alive_seconds = int(os.getenv("KEEP_ALIVE_SECONDS", "300"))
    if keep_alive_seconds > 0:
        print(Fore.CYAN + f"\nKeeping process alive for {keep_alive_seconds}s to allow background tasks to complete..." + Style.RESET_ALL)
        for remaining in range(keep_alive_seconds, 0, -1):
            time.sleep(1)
            if remaining % 10 == 0 or remaining <= 5:
                print(Fore.CYAN + f"Background wait: {remaining}s remaining" + Style.RESET_ALL)

except KeyboardInterrupt:
    print(Fore.YELLOW + "\nShutting down gracefully..." + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"\nError in workflow: {e}" + Style.RESET_ALL)
finally:
    # Optional Discord cleanup (defaults to off to avoid shutting down background executors prematurely)
    if os.getenv("DISCORD_CLEANUP_ON_EXIT", "false").lower() in ("1", "true", "yes", "on"):
        from src.discord_nodes import DiscordNodes
        DiscordNodes.cleanup()
    print(Fore.GREEN + "Cleanup completed." + Style.RESET_ALL)
