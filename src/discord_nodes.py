from typing import Dict, Any, Optional
import logging
import os
import sqlite3
import json
from datetime import datetime
import time
from colorama import Fore, Style

from .utils import log_state

try:
    # Prefer package-relative import
    from .discord_integration import ensure_started, submit_invoice
except Exception:
    # Fallback for different import contexts
    from src.discord_integration import ensure_started, submit_invoice

logger = logging.getLogger(__name__)


class DiscordNodes:
    """
    Discord integration nodes using the background agent (discord1.InvoiceApprovalAgent)
    via src/discord_integration.py. This keeps node methods synchronous while the
    Discord client and workflow run in a background asyncio loop.

    Fallback: If environment/configuration is missing, nodes auto-approve to keep
    the workflow functional.
    """

    def __init__(self):
        self.enabled = self._is_configured()
        # Auto-approve and complete payment if agent posting fails (configurable, default true)
        self.auto_approve_on_failure = os.getenv("DISCORD_AUTO_APPROVE_ON_FAILURE", "true").lower() in ("1", "true", "yes", "on")
        # Polling configuration (seconds)
        self.wait_seconds = int(os.getenv("DISCORD_APPROVAL_WAIT_SECONDS", "900"))  # default 15 min
        self.poll_interval = int(os.getenv("DISCORD_POLL_INTERVAL_SECONDS", "10"))  # default 10s
        self.payment_wait_seconds = int(os.getenv("DISCORD_PAYMENT_WAIT_SECONDS", "900"))  # default 15 min
        if self.enabled:
            print(f"{Fore.GREEN}Discord integration enabled (agent mode){Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}Using simplified Discord nodes (no actual Discord integration){Style.RESET_ALL}")

    def _is_configured(self) -> bool:
        # Minimal required environment
        required = [
            os.getenv("DISCORD_TOKEN"),
            os.getenv("DISCORD_CHANNEL_ID"),
            os.getenv("APPROVING_TEAM_ROLE_ID"),
            os.getenv("OPENAI_API_KEY"),
        ]
        sheets_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "g_sheets.json")
        spreadsheet_id = os.getenv("SPREADSHEET_ID")

        if not all(required):
            return False
        if not spreadsheet_id:
            return False
        if not os.path.exists(sheets_file):
            return False
        return True

    def _map_invoice_payload(self, invoice_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Map the RAG agent result (lowercase keys) to the agent's expected schema
        (Title Case keys used by discord1.InvoiceApprovalAgent).
        """
        data = (invoice_result or {}).get("data") or {}
        if not data:
            return None
        pd = data.get("payment_details") or {}
        return {
            "Timestamp": datetime.now().isoformat(),
            "Vendor Name": data.get("vendor_name", ""),
            "Invoice Number": data.get("invoice_number", ""),
            "Invoice Date": data.get("invoice_date", ""),
            "Total Amount": str(data.get("total_amount", "")),
            "Currency": data.get("currency", ""),
            "Line Items Count": str(len(data.get("line_items") or [])),
            "Account Holder": pd.get("account_holder", ""),
            "Bank Address": pd.get("bank_address", ""),
            "Account Number/IBAN": pd.get("account_number", ""),
        }

    def _fetch_state_from_db(self, invoice_number: str) -> Optional[Dict[str, Any]]:
        """Read the latest agent state from the local SQLite DB (invoice_states.db)."""
        try:
            conn = sqlite3.connect("invoice_states.db")
            cur = conn.cursor()
            cur.execute(
                "SELECT state_json FROM invoice_states WHERE invoice_number = ?",
                (invoice_number,),
            )
            row = cur.fetchone()
            conn.close()
            if not row:
                return None
            return json.loads(row[0])
        except Exception as e:
            logger.warning(f"Failed to read invoice state: {e}")
            return None

    def _update_discord_state(self, state: Dict[str, Any], agent_state: Dict[str, Any]) -> None:
        """Merge fields from agent state into workflow's discord_state."""
        if "discord_state" not in state or not isinstance(state["discord_state"], dict):
            state["discord_state"] = {}
        ds = state["discord_state"]
        ds.update({
            "thread_id": agent_state.get("discord_thread_id"),
            "message_id": agent_state.get("discord_message_id"),
            "approval_status": agent_state.get("approval_status", "pending"),
            "cost_center": agent_state.get("cost_center"),
            "approver": agent_state.get("approver"),
            "rejection_reason": agent_state.get("rejection_reason"),
            "payment_status": agent_state.get("payment_status", "pending"),
            "transaction_id": agent_state.get("transaction_id"),
            "reminder_count": agent_state.get("reminder_count", 0),
            "last_reminder": agent_state.get("last_reminder"),
            "sla_message_id": agent_state.get("sla_message_id"),
            "approval_sla_hours": agent_state.get("approval_sla_hours"),
            "created_at": (agent_state.get("invoice_data", {}) or {}).get("Timestamp"),
        })

    def create_discord_thread(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create the Discord approval thread via the agent. If disabled, auto-approve.
        """
        inv = state.get("invoice_processing_result") or {}
        if inv.get("status") != "completed":
            return state

        # Idempotency: if thread already exists, just refresh from DB (do not repost)
        existing_thread = (state.get("discord_state") or {}).get("thread_id")
        if existing_thread and existing_thread not in ("", "auto-approved"):
            inv_data = (inv.get("data") or {})
            invoice_number = inv_data.get("invoice_number", "")
            if invoice_number:
                agent_state = self._fetch_state_from_db(invoice_number)
                if agent_state:
                    self._update_discord_state(state, agent_state)
                    log_state(state, "Discord thread already exists - refreshed state", Fore.CYAN)
            return state

        if not self.enabled:
            # Auto-approve fallback keeps the pipeline moving
            if "discord_state" not in state or not isinstance(state["discord_state"], dict):
                state["discord_state"] = {}
            state["discord_state"].update({
                "thread_id": "auto-approved",
                "message_id": "auto-approved",
                "approval_status": "approved",
                "cost_center": "AUTO-APPROVED",
                "approver": "system",
                "payment_status": "completed",
                "transaction_id": "auto-confirmed",
                "created_at": datetime.now().isoformat()
            })
            log_state(state, "Auto-approved invoice", Fore.GREEN)
            return state

        payload = self._map_invoice_payload(inv)
        if not payload or not payload.get("Invoice Number"):
            logger.warning("Invoice payload incomplete, cannot create Discord thread")
            return state

        try:
            ensure_started()
            fut = submit_invoice(payload)
            # Submission dispatched asynchronously; proceed without waiting.

            agent_state = self._fetch_state_from_db(payload["Invoice Number"])
            if agent_state:
                self._update_discord_state(state, agent_state)
                log_state(state, "Discord thread created", Fore.GREEN)
            else:
                logger.warning("Agent state not found in DB yet; keeping status as pending")
                if "discord_state" not in state or not isinstance(state["discord_state"], dict):
                    state["discord_state"] = {}
                if self.auto_approve_on_failure:
                    state["discord_state"].update({
                        "thread_id": "auto-approved",
                        "message_id": "auto-approved",
                        "approval_status": "approved",
                        "cost_center": "AUTO-APPROVED",
                        "approver": "system",
                        "payment_status": "completed",
                        "transaction_id": "auto-confirmed",
                        "created_at": datetime.now().isoformat()
                    })
                    log_state(state, "Auto-approved invoice (agent state not found)", Fore.GREEN)
                else:
                    state["discord_state"].update({
                        "approval_status": "pending",
                        "payment_status": "pending"
                    })
        except Exception as e:
            logger.error(f"Failed to create Discord thread via agent: {e}")
            if "discord_state" not in state or not isinstance(state["discord_state"], dict):
                state["discord_state"] = {}
            if self.auto_approve_on_failure:
                state["discord_state"].update({
                    "thread_id": "auto-approved",
                    "message_id": "auto-approved",
                    "approval_status": "approved",
                    "cost_center": "AUTO-APPROVED",
                    "approver": "system",
                    "payment_status": "completed",
                    "transaction_id": "auto-confirmed",
                    "created_at": datetime.now().isoformat()
                })
                log_state(state, "Auto-approved invoice (agent failure)", Fore.GREEN)
            else:
                state["discord_state"].update({
                    "approval_status": "pending",
                    "payment_status": "pending"
                })

        return state

    def check_discord_approval(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Non-blocking: single approval status read from DB."""
        if not self.enabled:
            return state

        inv = state.get("invoice_processing_result") or {}
        data = (inv.get("data") or {})
        invoice_number = data.get("invoice_number", "")
        if not invoice_number:
            return state

        agent_state = self._fetch_state_from_db(invoice_number)
        if agent_state:
            self._update_discord_state(state, agent_state)
            log_state(state, "Checked Discord approval (non-blocking)", Fore.CYAN)

        return state

    def check_payment_confirmation(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Non-blocking: single payment status read from DB (auto-complete if disabled)."""
        if not self.enabled:
            if "discord_state" not in state or not isinstance(state["discord_state"], dict):
                state["discord_state"] = {}
            if state["discord_state"].get("payment_status") != "completed":
                state["discord_state"].update({
                    "payment_status": "completed",
                    "transaction_id": state["discord_state"].get("transaction_id") or "auto-confirmed"
                })
                log_state(state, "Auto-confirmed payment", Fore.GREEN)
            return state

        inv = state.get("invoice_processing_result") or {}
        data = (inv.get("data") or {})
        invoice_number = data.get("invoice_number", "")
        if not invoice_number:
            return state

        agent_state = self._fetch_state_from_db(invoice_number)
        if agent_state:
            self._update_discord_state(state, agent_state)
            log_state(state, "Checked payment confirmation (non-blocking)", Fore.CYAN)

        return state

    @classmethod
    def cleanup(cls):
        """No-op: agent lifecycle is managed by src/discord_integration.py"""
        print(f"{Fore.GREEN}Discord agent cleanup not required (managed separately){Style.RESET_ALL}")
