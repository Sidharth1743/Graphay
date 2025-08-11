import os
import threading
import asyncio
import time
from concurrent.futures import Future
from typing import Dict, Optional
import logging

# Import agent class from root discord1.py
from discord1 import Config, InvoiceApprovalAgent

_agent_lock = threading.Lock()
_agent: Optional[InvoiceApprovalAgent] = None
_loop: Optional[asyncio.AbstractEventLoop] = None
_thread: Optional[threading.Thread] = None
_start_future: Optional[Future] = None


def _run_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def ensure_started() -> None:
    global _agent, _loop, _thread
    with _agent_lock:
        if _agent is not None:
            return
        # Build config from environment
        cfg = Config(
            discord_token=os.getenv("DISCORD_TOKEN"),
            discord_channel_id=int(os.getenv("DISCORD_CHANNEL_ID", "0")),
            approving_team_role_id=int(os.getenv("APPROVING_TEAM_ROLE_ID", "0")),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            google_sheets_credentials_file=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "g_sheets.json"),
            google_drive_credentials_file=os.getenv("GOOGLE_DRIVE_CREDENTIALS_FILE", "gdrive.json"),
            spreadsheet_id=os.getenv("SPREADSHEET_ID"),
            worksheet_name=os.getenv("WORKSHEET_NAME", "Invoice-agent"),
            reminder_interval_hours=int(os.getenv("REMINDER_INTERVAL_HOURS", "24")),
            max_reminders=int(os.getenv("MAX_REMINDERS", "5")),
            fallback_channel_name=os.getenv("FALLBACK_CHANNEL_NAME")
        )
        _agent = InvoiceApprovalAgent(cfg)
        # Start asyncio loop in background
        _loop = asyncio.new_event_loop()
        _thread = threading.Thread(target=_run_loop, args=(_loop,), daemon=True)
        _thread.start()
        # Wait until the loop is running to avoid "no running event loop" race
        for _ in range(500):  # up to ~5 seconds
            if _loop.is_running():
                break
            time.sleep(0.01)
        # Start Discord client on the background loop
        try:
            fut = asyncio.run_coroutine_threadsafe(_agent.start(), _loop)
        except Exception as e:
            logging.error(f"Failed to schedule Discord agent start: {e}")
            raise
        else:
            # Store future and log outcome if it fails/stops
            global _start_future
            _start_future = fut
            def _on_done(f: Future):
                try:
                    result = f.result()
                    logging.info(f"Discord agent finished: {result}")
                except Exception as ex:
                    logging.error(f"Discord agent failed to start or crashed: {ex}")
            try:
                fut.add_done_callback(_on_done)
            except Exception:
                pass
            print("✓ Discord agent start scheduled on background loop")


def submit_invoice(invoice_data: Dict) -> Future:
    """Submit an invoice to the Discord agent workflow. Returns a Future for the result message."""
    ensure_started()
    assert _agent is not None and _loop is not None
    return asyncio.run_coroutine_threadsafe(_agent.process_invoice(invoice_data), _loop)
