from colorama import Fore , Style
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
from .agents import Agents
from .tools.gmail_tool import GmailTool 
from .state import BaseGraph , Email
from .database import EmailDatabase
# FileManager removed - using direct Google Drive uploads
from .gdrive_uploader import GDriveUploader
from .gdrive_invoice_processor import GDriveInvoiceProcessor
from .discord_nodes import DiscordNodes
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    from .discord_integration import ensure_started, submit_invoice
except Exception:
    from src.discord_integration import ensure_started, submit_invoice

class Nodes:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.agents = Agents()
        self.gmail_tools = GmailTool()
        self.email_db = EmailDatabase()
        # FileManager removed - using direct Google Drive uploads
        self.gdrive_uploader = GDriveUploader(token_file="token1.json")
        
        # State tracking for approvals and payments
        self.approval_states = {"approved", "rejected", "pending"}
        self.payment_states = {"completed", "pending"}
        
        # Initialize Discord integration wrapper (agent runs in background if configured)
        self.discord_nodes = DiscordNodes()
        
        # Track which GDrive files we've already attempted to process to avoid duplicates
        self.processed_gdrive_ids: set[str] = set()
        
        # Get the specific Google Drive folder ID from environment
        self.gdrive_folder_id = os.getenv('GDRIVE_FOLDER_ID')
        if not self.gdrive_folder_id:
            print(Fore.YELLOW + "Warning: GDRIVE_FOLDER_ID not found in .env file. Files will be uploaded to root folder." + Style.RESET_ALL)
        
        # Initialize the Google Drive Invoice Processor
        openai_api_key = os.getenv('OPENAI_API_KEY')
        if openai_api_key:
            try:
                self.invoice_processor = GDriveInvoiceProcessor(
                    openai_api_key=openai_api_key,
                    google_creds_path="gdrive.json",
                    token_file="token1.json",
                    sheet_name="Invoice"
                )
                print(Fore.GREEN + "✓ Invoice RAG Agent initialized successfully" + Style.RESET_ALL)
            except Exception as e:
                print(Fore.YELLOW + f"⚠️ Failed to initialize Invoice RAG Agent: {e}" + Style.RESET_ALL)
                self.invoice_processor = None
        else:
            print(Fore.YELLOW + "⚠️ OPENAI_API_KEY not found in .env file. Invoice processing will be disabled." + Style.RESET_ALL)
            self.invoice_processor = None

        # Start follow-up reminder scheduler (daemon) to enforce 3-business-day reminder
        try:
            self._start_followup_scheduler()
        except Exception as e:
            print(Fore.YELLOW + f"⚠️ Failed to start follow-up scheduler: {e}" + Style.RESET_ALL)

    def load_new_emails(self, state: BaseGraph) -> BaseGraph:
        """Loads unread emails (top 10) from Gmail and updates the state."""
        print(Fore.YELLOW + "Loading unread emails (top 10)...\n" + Style.RESET_ALL)
        unread_emails = self.gmail_tools.fetch_unread_emails(max_results=10)
        emails = [Email(**email) for email in unread_emails]
        return {"emails": emails}

    def check_new_emails(self, state: BaseGraph) -> str:
        """Checks if there are new emails to process."""
        if len(state['emails']) == 0:
            print(Fore.RED + "No new emails" + Style.RESET_ALL)
            return "empty"
        else:
            print(Fore.GREEN + "New emails to process" + Style.RESET_ALL)
            return "process"
        
    def is_email_inbox_empty(self, state: BaseGraph) -> BaseGraph:
        return state

    def categorize_email(self, state: BaseGraph) -> BaseGraph:
        """Categorizes the current email using the categorize_email agent."""
        print(Fore.YELLOW + "Checking email category...\n" + Style.RESET_ALL)
        
        # Get the last email
        current_email = state["emails"][-1]
        result = self.agents.categorize_email.invoke({"email": current_email.body})
        print(Fore.MAGENTA + f"Email category: {result.category.value}" + Style.RESET_ALL)
        
        return {
            "email_category": result.category.value,
            "current_email": current_email
        }

    def route_email_based_on_category(self, state: BaseGraph) -> BaseGraph:
        """Routes the email based on its category (state passthrough; decision via route_decision)."""
        print(Fore.YELLOW + "Routing email based on category...\n" + Style.RESET_ALL)
        return state

    def route_decision(self, state: BaseGraph) -> str:
        """Condition function used by the graph to choose the next node based on category."""
        category = state["email_category"]
        if category == "product_enquiry":
            return "product related"
        elif category == "unrelated":
            return "unrelated"
        else:
            return "not product related"

    def construct_rag_queries(self, state: BaseGraph) -> BaseGraph:
        """Constructs RAG queries based on the email content."""
        print(Fore.YELLOW + "Designing RAG query...\n" + Style.RESET_ALL)
        email_content = state["current_email"].body
        query_result = self.agents.design_rag_queries.invoke({"email": email_content})
        
        return {"rag_queries": query_result.queries}

    def retrieve_from_rag(self, state: BaseGraph) -> BaseGraph:
        """Retrieves information from internal knowledge based on RAG questions."""
        print(Fore.YELLOW + "Retrieving information from internal knowledge...\n" + Style.RESET_ALL)
        final_answer = ""
        for query in state["rag_queries"]:
            query_str = str(query)
            rag_result = self.agents.generate_rag_answer.invoke({"question": query_str})
            final_answer += query + "\n" + rag_result + "\n\n"
        
        return {"retrieved_documents": final_answer}

    def write_draft_email(self, state: BaseGraph) -> BaseGraph:
        """Writes a draft email based on the current email and retrieved information."""
        print(Fore.YELLOW + "Writing draft email...\n" + Style.RESET_ALL)
        
        # Format input to the writer agent
        inputs = (
            f'# **EMAIL CATEGORY:** {state["email_category"]}\n\n'
            f'# **EMAIL CONTENT:**\n{state["current_email"].body}\n\n'
            f'# **INFORMATION:**\n{state["retrieved_documents"]}' # Empty for feedback or complaint
        )
        
        # Get messages history for current email
        writer_messages = state.get('writer_messages', [])
        
        # Write email
        draft_result = self.agents.email_writer.invoke({
            "email_information": inputs,
            "history": writer_messages
        })
        email = draft_result.email
        trials = state.get('trials', 0) + 1

        # Append writer's draft to the message list
        writer_messages.append(f"**Draft {trials}:**\n{email}")

        return {
            "generated_email": email, 
            "trials": trials,
            "writer_messages": writer_messages
        }

    def verify_generated_email(self, state: BaseGraph) -> BaseGraph:
        """Verifies the generated email using the proofreader agent."""
        print(Fore.YELLOW + "Verifying generated email...\n" + Style.RESET_ALL)
        review = self.agents.email_proofreader.invoke({
            "initial_email": state["current_email"].body,
            "generated_email": state["generated_email"],
        })

        writer_messages = state.get('writer_messages', [])
        writer_messages.append(f"**Proofreader Feedback:**\n{review.feedback}")

        return {
            "sendable": review.send,
            "writer_messages": writer_messages
        }

    def must_rewrite(self, state: BaseGraph) -> str:
        """Determines if the email needs to be rewritten based on the review and trial count."""
        email_sendable = state["sendable"]
        if email_sendable:
            print(Fore.GREEN + "Email is good, ready to be sent!!!" + Style.RESET_ALL)
            state["writer_messages"] = []
            return "send"
        elif state["trials"] >= 3:
            print(Fore.RED + "Email is not good, we reached max trials must stop!!!" + Style.RESET_ALL)
            state["emails"].pop()
            state["writer_messages"] = []
            return "stop"
        else:
            print(Fore.RED + "Email is not good, must rewrite it..." + Style.RESET_ALL)
            return "rewrite"

    def create_draft_response(self, state: BaseGraph) -> BaseGraph:
        """Creates a draft response in Gmail and processes attachments."""
        print(Fore.YELLOW + "Creating draft email...\n" + Style.RESET_ALL)
        
        # Store current email in database
        current_email = state["current_email"]
        email_data = {
            'id': current_email.id,
            'threadId': current_email.threadId,
            'messageId': current_email.messageId,
            'references': current_email.references,
            'sender': current_email.sender,
            'subject': current_email.subject,
            'body': current_email.body
        }
        
        # Store email in database
        self.email_db.store_email(email_data)
        
        # Process attachments if any
        processed_attachments = self._process_attachments(current_email, "product")
        all_processed = (state.get("processed_attachments", []) or []) + processed_attachments
        
        # Create draft response
        self.gmail_tools.create_draft_reply(state["current_email"], state["generated_email"])
        
        # Mark email as processed
        self.email_db.mark_email_processed(current_email.id)
        # Mark as read in Gmail to avoid reprocessing
        try:
            self.gmail_tools.mark_as_read(current_email.id)
        except Exception:
            pass
        
        # Remove the processed email from the list
        state["emails"].pop()
        
        return {
            "retrieved_documents": "", 
            "trials": 0,
            "processed_attachments": all_processed
        }

    def send_email_response(self, state: BaseGraph) -> BaseGraph:
        """Sends the email response directly using Gmail and processes attachments."""
        print(Fore.YELLOW + "Sending email...\n" + Style.RESET_ALL)
        
        # Store current email in database
        current_email = state["current_email"]
        email_data = {
            'id': current_email.id,
            'threadId': current_email.threadId,
            'messageId': current_email.messageId,
            'references': current_email.references,
            'sender': current_email.sender,
            'subject': current_email.subject,
            'body': current_email.body
        }
        
        # Store email in database
        self.email_db.store_email(email_data)
        
        # Process attachments if any
        processed_attachments = self._process_attachments(current_email, "feedback")
        all_processed = (state.get("processed_attachments", []) or []) + processed_attachments
        
        # Send email response
        self.gmail_tools.send_reply(state["current_email"], state["generated_email"])
        
        # Mark email as processed
        self.email_db.mark_email_processed(current_email.id)
        # Mark as read in Gmail to avoid reprocessing
        try:
            self.gmail_tools.mark_as_read(current_email.id)
        except Exception:
            pass
        
        return {
            "retrieved_documents": "", 
            "trials": 0,
            "processed_attachments": all_processed
        }
    
    def _process_attachments(self, current_email, email_type: str = "general") -> List[Dict]:
        """Helper method to process attachments for any email type."""
        processed_attachments = []
        
        if current_email.attachments:
            print(Fore.CYAN + f"Found {len(current_email.attachments)} attachments" + Style.RESET_ALL)
            
            # Use the specific Google Drive folder ID from environment
            gdrive_folder_id = self.gdrive_folder_id
            
            if not gdrive_folder_id:
                print(Fore.RED + "Error: No Google Drive folder ID configured. Skipping attachment upload." + Style.RESET_ALL)
                return processed_attachments
            
            print(Fore.CYAN + f"Uploading to Google Drive folder ID: {gdrive_folder_id}" + Style.RESET_ALL)
            
            # Collect freshly uploaded PDF attachments to kick off immediate processing
            files_to_process: List[Dict[str, str]] = []
            
            for attachment in current_email.attachments:
                # Upload attachment directly to Google Drive
                upload_result = self.gdrive_uploader.upload_attachment_directly(
                    attachment_data=attachment,
                    email_id=current_email.id,
                    folder_id=gdrive_folder_id
                )
                
                if upload_result['success']:
                    # Create attachment info for database
                    attachment_info = {
                        'filename': upload_result['file_name'],
                        'original_filename': attachment.get('filename', 'unknown'),
                        'mime_type': attachment.get('mimeType', 'application/octet-stream'),
                        'file_path': f"gdrive://{upload_result['file_id']}",  # Virtual path for Google Drive
                        'file_size': upload_result.get('file_size_mb', 0) * 1024 * 1024,  # Convert MB to bytes
                        'category': 'gdrive_upload',
                        'file_hash': '',  # Not applicable for direct upload
                        'saved_date': datetime.now().isoformat(),
                        'gdrive_file_id': upload_result['file_id'],
                        'gdrive_folder_id': gdrive_folder_id
                    }
                    
                    # Store attachment info in database
                    self.email_db.store_attachment(current_email.id, attachment_info)
                    processed_attachments.append(attachment_info)
                    print(Fore.GREEN + f"✓ {upload_result['message']}" + Style.RESET_ALL)
                    
                    # If PDF, enqueue for immediate background processing to Discord
                    file_lower = (upload_result['file_name'] or "").lower()
                    mime_lower = (attachment.get('mimeType') or "").lower()
                    if mime_lower == "application/pdf" or file_lower.endswith(".pdf"):
                        files_to_process.append({
                            "id": upload_result['file_id'],
                            "name": upload_result['file_name'],
                        })
                else:
                    print(Fore.RED + f"✗ {upload_result['message']}" + Style.RESET_ALL)
            
            # Kick off processing for the uploaded PDFs immediately (non-blocking)
            if files_to_process:
                try:
                    threading.Thread(
                        target=self._background_process_invoices,
                        args=(len(files_to_process), files_to_process),
                        daemon=True
                    ).start()
                    print(Fore.CYAN + f"Started background processing for {len(files_to_process)} uploaded PDF(s)" + Style.RESET_ALL)
                except Exception as e:
                    print(Fore.YELLOW + f"⚠️ Failed to start immediate background processing: {e}" + Style.RESET_ALL)
        
        return processed_attachments

    def skip_unrelated_email(self, state: BaseGraph) -> BaseGraph:
        """Skip unrelated email and process attachments."""
        print(Fore.YELLOW + "Skipping unrelated email...\n" + Style.RESET_ALL)
        
        # Store current email in database
        current_email = state["current_email"]
        email_data = {
            'id': current_email.id,
            'threadId': current_email.threadId,
            'messageId': current_email.messageId,
            'references': current_email.references,
            'sender': current_email.sender,
            'subject': current_email.subject,
            'body': current_email.body
        }
        
        # Store email in database
        self.email_db.store_email(email_data)
        
        # Process attachments if any
        processed_attachments = self._process_attachments(current_email, "unrelated")
        all_processed = (state.get("processed_attachments", []) or []) + processed_attachments
        
        # Mark email as processed
        self.email_db.mark_email_processed(current_email.id)
        # Mark as read in Gmail to avoid reprocessing
        try:
            self.gmail_tools.mark_as_read(current_email.id)
        except Exception:
            pass
        
        # Remove the processed email from the list
        state["emails"].pop()
        
        return {
            "processed_attachments": all_processed
        }
    
    def process_invoice_emails(self, state: BaseGraph) -> BaseGraph:
        """Process invoice-related emails and upload attachments directly to Google Drive."""
        print(Fore.YELLOW + "Processing invoice-related emails...\n" + Style.RESET_ALL)
        
        # Store current email in database
        current_email = state["current_email"]
        email_data = {
            'id': current_email.id,
            'threadId': current_email.threadId,
            'messageId': current_email.messageId,
            'references': current_email.references,
            'sender': current_email.sender,
            'subject': current_email.subject,
            'body': current_email.body
        }
        
        # Store email in database
        self.email_db.store_email(email_data)
        
        # Process attachments if any
        processed_attachments = self._process_attachments(current_email, "invoice")
        all_processed = (state.get("processed_attachments", []) or []) + processed_attachments
        
        # Mark email as processed
        self.email_db.mark_email_processed(current_email.id)
        # Mark as read in Gmail to avoid reprocessing
        try:
            self.gmail_tools.mark_as_read(current_email.id)
        except Exception:
            pass
        
        # Remove the processed email from the list
        state["emails"].pop()
        
        return {
            "processed_attachments": all_processed
        }
    
    def check_invoice_related(self, state: BaseGraph) -> str:
        """Check if the current email is invoice-related."""
        current_email = state["current_email"]
        
        # Check if email is already marked as invoice-related in database
        email_data = {
            'id': current_email.id,
            'threadId': current_email.threadId,
            'messageId': current_email.messageId,
            'references': current_email.references,
            'sender': current_email.sender,
            'subject': current_email.subject,
            'body': current_email.body
        }
        
        # Store email and check if invoice-related
        self.email_db.store_email(email_data)
        
        # Get the stored email to check invoice status
        import sqlite3
        with sqlite3.connect(self.email_db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT is_invoice_related FROM emails WHERE id = ?', (current_email.id,))
            result = cursor.fetchone()
            
            if result and result[0]:
                print(Fore.GREEN + "Email is invoice-related!" + Style.RESET_ALL)
                return "invoice_related"
            else:
                print(Fore.BLUE + "Email is not invoice-related" + Style.RESET_ALL)
                return "not_invoice_related"
    
    def check_all_emails_processed(self, state: BaseGraph) -> str:
        """Check if all emails have been processed."""
        if len(state['emails']) == 0:
            print(Fore.GREEN + "All emails processed! Ending workflow." + Style.RESET_ALL)
            return "finished"
        else:
            print(Fore.YELLOW + f"Still have {len(state['emails'])} emails to process" + Style.RESET_ALL)
            return "continue"
    
    def upload_to_gdrive(self, state: BaseGraph) -> BaseGraph:
        """Provide summary of direct Google Drive uploads."""
        print(Fore.CYAN + "\n=== GOOGLE DRIVE UPLOAD SUMMARY ===" + Style.RESET_ALL)
        
        # Show the target folder
        if self.gdrive_folder_id:
            print(Fore.CYAN + f"Target Google Drive folder ID: {self.gdrive_folder_id}" + Style.RESET_ALL)
        else:
            print(Fore.YELLOW + "Warning: No specific Google Drive folder configured" + Style.RESET_ALL)
        
        # Collect all processed attachments from the session
        all_processed_attachments = []
        
        # Get attachments from current state
        current_attachments = state.get("processed_attachments", [])
        if current_attachments:
            all_processed_attachments.extend(current_attachments)
        
        # Also check if there are any attachments stored in the database for this session
        # (This would be useful if we want to show all attachments processed in the current run)
        
        if all_processed_attachments:
            print(Fore.GREEN + f"✓ Successfully uploaded {len(all_processed_attachments)} attachments directly to Google Drive" + Style.RESET_ALL)
            
            for attachment in all_processed_attachments:
                if attachment.get('gdrive_file_id'):
                    print(Fore.GREEN + f"  ✓ {attachment['filename']} (ID: {attachment['gdrive_file_id']})" + Style.RESET_ALL)
        else:
            print(Fore.YELLOW + "No attachments were processed in this session" + Style.RESET_ALL)
        
        return {
            "gdrive_upload_result": {
                "success": True,
                "total_files": len(all_processed_attachments),
                "message": f"Direct upload summary: {len(all_processed_attachments)} files uploaded"
            }
        }
    
    def _map_invoice_payload_for_agent(self, invoice_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Map RAG result to Discord agent schema."""
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
    
    def _add_business_days(self, start_dt: datetime, days: int) -> datetime:
        """Add a specified number of business days (Mon-Fri) to a datetime."""
        remaining = days
        current = start_dt
        while remaining > 0:
            current += timedelta(days=1)
            if current.weekday() < 5:  # 0=Mon .. 4=Fri
                remaining -= 1
        return current

    def _compose_missing_info_email(self, missing_fields: List[str], original_subject: str, is_reminder: bool = False) -> str:
        """Create a consolidated email body to request missing mandatory information."""
        header = "Reminder: Additional Information Required to Process Your Invoice" if is_reminder else "Additional Information Required to Process Your Invoice"
        intro = (
            "Thank you for your invoice. During validation we found that some mandatory details are missing."
            if not is_reminder else
            "This is a friendly reminder that we’re awaiting the missing details to continue processing your invoice."
        )
        bullet_list = "\n".join([f"- {item}" for item in (missing_fields or [])])
        footer = (
            "Processing for this specific invoice is paused until the above information is received. "
            "Other invoices (if any) are not affected. "
            "Please reply directly to this email with the requested details in a single response to avoid delays.\n\n"
            "Thank you."
        )
        return (
            f"{header}\n\n"
            f"Subject: {original_subject}\n\n"
            f"{intro}\n\n"
            f"Missing required information:\n{bullet_list if bullet_list else '- (No items listed)'}\n\n"
            f"{footer}"
        )

    def _start_followup_scheduler(self) -> None:
        """Start a background daemon thread that sends one reminder after 3 business days if no reply."""
        if getattr(self, "_followup_thread", None) and self._followup_thread.is_alive():
            return
        def _runner():
            try:
                self._followup_scheduler_loop()
            except Exception as e:
                print(Fore.YELLOW + f"⚠️ Follow-up scheduler stopped: {e}" + Style.RESET_ALL)
        self._followup_thread = threading.Thread(target=_runner, daemon=True)
        self._followup_thread.start()

    def _followup_scheduler_loop(self) -> None:
        """Periodically check due follow-ups, detect replies, and send a single reminder if needed."""
        poll_seconds = 600
        try:
            poll_seconds = int(os.getenv("FOLLOWUP_POLL_SECONDS", str(poll_seconds)))
        except Exception:
            pass
        my_email = (os.getenv("MY_EMAIL") or "").lower()
        while True:
            try:
                now_iso = datetime.now().isoformat()
                due = self.email_db.get_due_followups(now_iso)
                for f in due:
                    # First detect if requester has replied since initial notice
                    thread_id = f.get("thread_id")
                    initial_iso = f.get("initial_notice_sent_at") or ""
                    try:
                        initial_ts_ms = int(datetime.fromisoformat(initial_iso).timestamp() * 1000)
                    except Exception:
                        initial_ts_ms = None
                    replied = False
                    msgs = self.gmail_tools.get_thread_messages(thread_id) if thread_id else []
                    for m in msgs:
                        from_addr = (m.get("from") or "").lower()
                        internal_ms = m.get("internalDate")
                        if internal_ms and initial_ts_ms and internal_ms > initial_ts_ms:
                            # Treat as reply if not from our own mailbox
                            if my_email and my_email in from_addr:
                                continue
                            replied = True
                            break
                    if replied:
                        try:
                            self.email_db.mark_followup_resolved(f["id"])
                            print(Fore.GREEN + f"✓ Detected reply for follow-up {f['id']}, marked resolved" + Style.RESET_ALL)
                        except Exception as e:
                            print(Fore.YELLOW + f"⚠️ Failed marking follow-up resolved: {e}" + Style.RESET_ALL)
                        continue

                    # If no reply and reminder not sent, send one reminder
                    if not f.get("reminder_sent", 0):
                        try:
                            email_row = self.email_db.get_email_by_id(f["email_id"])
                            if email_row:
                                initial_email = Email(
                                    id=email_row["id"],
                                    threadId=email_row["thread_id"],
                                    messageId=email_row.get("message_id") or "",
                                    references=email_row.get("email_references") or "",
                                    sender=email_row["sender"],
                                    subject=email_row["subject"],
                                    body=email_row["body"],
                                    attachments=[]
                                )
                                try:
                                    import json as _json
                                    missing_fields = []
                                    raw = f.get("missing_fields")
                                    if raw:
                                        try:
                                            parsed = _json.loads(raw)
                                            if isinstance(parsed, list):
                                                missing_fields = parsed
                                            elif isinstance(parsed, dict):
                                                missing_fields = [f"{k}: {v}" for k, v in parsed.items()]
                                        except Exception:
                                            missing_fields = [str(raw)]
                                    msg = self._compose_missing_info_email(missing_fields, email_row["subject"], is_reminder=True)
                                except Exception:
                                    msg = self._compose_missing_info_email([], email_row["subject"], is_reminder=True)

                                self.gmail_tools.send_reply(initial_email, msg)
                                self.email_db.mark_followup_reminder_sent(f["id"], datetime.now().isoformat())
                                print(Fore.CYAN + f"Sent reminder for follow-up {f['id']}" + Style.RESET_ALL)
                        except Exception as e:
                            print(Fore.YELLOW + f"⚠️ Failed to send reminder for follow-up {f.get('id')}: {e}" + Style.RESET_ALL)
                time.sleep(max(60, poll_seconds))
            except Exception as loop_e:
                print(Fore.YELLOW + f"⚠️ Follow-up scheduler loop error: {loop_e}" + Style.RESET_ALL)
                time.sleep(120)

    def _handle_validation_failure(self, result: Dict[str, Any], file_meta: Dict[str, Any]) -> None:
        """Draft and send a single consolidated email to the requestor listing required info. Create follow-up record and schedule one reminder."""
        try:
            gdrive_file_id = (result.get("source_file") or {}).get("gdrive_id") or (file_meta or {}).get("id")
            if not gdrive_file_id:
                return
            # Avoid duplicate notices if already open
            existing = self.email_db.get_open_followup_by_file_id(gdrive_file_id)
            if existing:
                return

            # Find originating email from attachment mapping
            email_row = self.email_db.get_email_by_gdrive_file_id(gdrive_file_id)
            if not email_row:
                # Fallback: attempt to find originating email via Gmail search (by filename/vendor/invoice no.)
                src = (result.get("source_file") if isinstance(result, dict) else {}) or {}
                src_name = src.get("filename") or ""
                email_info = self._try_find_originating_email_via_gmail(src_name, result)
                if not email_info:
                    print(Fore.YELLOW + f"⚠️ Could not locate originating email for GDrive file {gdrive_file_id}" + Style.RESET_ALL)
                    return
                eid = email_info["id"]
                thread_id = email_info["threadId"]
                message_id = email_info.get("messageId") or ""
                references = email_info.get("references") or ""
                sender = email_info["sender"]
                subject = email_info["subject"]
                body_text = email_info["body"]
                # Ensure we have a DB record for follow-up linkage
                try:
                    self.email_db.store_email({
                        "id": eid,
                        "threadId": thread_id,
                        "messageId": message_id,
                        "references": references,
                        "sender": sender,
                        "subject": subject,
                        "body": body_text
                    })
                except Exception:
                    pass
            else:
                eid = email_row["id"]
                thread_id = email_row["thread_id"]
                message_id = email_row.get("message_id") or ""
                references = email_row.get("email_references") or ""
                sender = email_row["sender"]
                subject = email_row["subject"]
                body_text = email_row["body"]

            # Compose missing fields list from result
            missing_fields = []
            errors = result.get("errors") or []
            if isinstance(errors, list):
                missing_fields = errors
            else:
                missing_fields = [str(errors)]

            # Build Email object for reply
            initial_email = Email(
                id=eid,
                threadId=thread_id,
                messageId=message_id,
                references=references,
                sender=sender,
                subject=subject,
                body=body_text,
                attachments=[]
            )

            # Send consolidated request email
            body = self._compose_missing_info_email(missing_fields, subject, is_reminder=False)
            self.gmail_tools.send_reply(initial_email, body)
            sent_at = datetime.now()
            reminder_due = self._add_business_days(sent_at, 3)

            # Persist follow-up for one-time reminder scheduling
            self.email_db.create_followup(
                email_id=eid,
                thread_id=thread_id,
                gdrive_file_id=gdrive_file_id,
                missing_fields=missing_fields,
                initial_notice_sent_at=sent_at.isoformat(),
                reminder_due_at=reminder_due.isoformat()
            )
            print(Fore.CYAN + f"Created follow-up record for GDrive file {gdrive_file_id}; reminder due on {reminder_due.isoformat()}" + Style.RESET_ALL)
        except Exception as e:
            print(Fore.YELLOW + f"⚠️ Validation failure handler error: {e}" + Style.RESET_ALL)

    def _try_find_originating_email_via_gmail(self, src_name: str, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fallback strategy to locate the originating Gmail message when DB mapping is unavailable.
        Tries:
          1) filename: query for the invoice filename
          2) subject/vendor/invoice number heuristics based on extracted data
        Returns normalized email_info dict matching GmailTool.get_email_info_by_id output.
        """
        try:
            # 1) Search by filename if available
            if src_name:
                msgs = self.gmail_tools.search_messages(f'filename:"{src_name}"', max_results=5)
                for m in msgs:
                    info = self.gmail_tools.get_email_info_by_id(m.get("id"))
                    if info:
                        return info
            # 2) Search by extracted fields
            data = (result or {}).get("extracted_data") or {}
            candidates: List[str] = []
            inv_no = str(data.get("invoice_number") or "").strip()
            vendor = str(data.get("vendor_name") or "").strip()
            if inv_no:
                candidates.append(f'subject:"{inv_no}"')
            if vendor:
                candidates.append(f'subject:"{vendor}"')
            for q in candidates:
                msgs = self.gmail_tools.search_messages(q, max_results=5)
                for m in msgs:
                    info = self.gmail_tools.get_email_info_by_id(m.get("id"))
                    if info:
                        return info
        except Exception as e:
            print(Fore.YELLOW + f"⚠️ Gmail fallback search failed: {e}" + Style.RESET_ALL)
        return None

    def _background_process_invoices(self, max_files: int = 5, files_meta: Optional[List[Dict[str, Any]]] = None) -> None:
        """Process multiple invoices concurrently in background and submit to Discord."""
        try:
            if files_meta and len(files_meta) > 0:
                files = files_meta
                print(Fore.CYAN + f"Processing {len(files)} invoice file(s) from current session" + Style.RESET_ALL)
            else:
                files = self.invoice_processor.get_latest_invoice_files(self.gdrive_folder_id, max_files=max_files)
                if not files:
                    print(Fore.YELLOW + "⚠️ No invoice files found in Google Drive folder" + Style.RESET_ALL)
                    return
            
            # Ensure Discord agent is started once
            try:
                ensure_started()
            except Exception as e:
                print(Fore.YELLOW + f"⚠️ Could not start Discord agent: {e}" + Style.RESET_ALL)
            
            submitted_count = [0]  # track successful Discord submissions
            
            def worker(file_meta: Dict[str, Any]) -> Dict[str, Any]:
                # Skip if we've already attempted this file ID
                file_id = file_meta.get('id')
                if not file_id:
                    return {
                        "status": "skipped_invalid",
                        "error": "Missing file id",
                        "source_file": {"gdrive_id": None, "filename": file_meta.get("name", "unknown")}
                    }
                if file_id in self.processed_gdrive_ids:
                    return {
                        "status": "skipped_duplicate",
                        "source_file": {"gdrive_id": file_id, "filename": file_meta.get("name", "unknown")}
                    }
                # Mark as in-progress to avoid duplicate attempts
                self.processed_gdrive_ids.add(file_id)
                
                temp_path = self.invoice_processor.download_file(file_id, file_meta['name'])
                if not temp_path:
                    # Fallback: optionally submit minimal invoice to Discord to validate pipeline
                    if os.getenv("DISCORD_SUBMIT_ON_FAILURE", "false").lower() in ("1", "true", "yes", "on"):
                        try:
                            created = (file_meta.get('createdTime') or datetime.now().isoformat())[:10]
                            payload = {
                                "Timestamp": datetime.now().isoformat(),
                                "Vendor Name": "Unknown Vendor",
                                "Invoice Number": f"FALLBACK-{file_meta['id']}",
                                "Invoice Date": created,
                                "Total Amount": "0.00",
                                "Currency": "USD",
                                "Line Items Count": "0",
                                "Account Holder": "Unknown",
                                "Bank Address": "Unknown",
                                "Account Number/IBAN": "UNKNOWN",
                            }
                            fut = submit_invoice(payload)
                            submitted_count[0] += 1
                            print(Fore.CYAN + f"Submitted fallback invoice to Discord agent: {payload['Invoice Number']}" + Style.RESET_ALL)
                            try:
                                fut.add_done_callback(lambda f: print(Fore.GREEN + "✓ Fallback submit result: " + str(f.result()) + Style.RESET_ALL) if not f.exception() else print(Fore.RED + f"✗ Fallback submit failed: {f.exception()}" + Style.RESET_ALL))
                            except Exception:
                                pass
                        except Exception as se:
                            print(Fore.RED + f"✗ Failed to submit fallback invoice: {se}" + Style.RESET_ALL)
                    return {
                        "status": "download_failed",
                        "error": f"Failed to download file: {file_meta['name']}",
                        "source_file": {"gdrive_id": file_meta["id"], "filename": file_meta["name"]}
                    }
                try:
                    # Process invoice
                    result = self.invoice_processor.rag_agent.process_invoice(temp_path)
                    result['source_file'] = {
                        'gdrive_id': file_meta['id'],
                        'filename': file_meta['name'],
                        'created_time': file_meta.get('createdTime')
                    }
                except Exception as e:
                    result = {
                        "status": "processing_failed",
                        "error": f"Failed to process invoice: {str(e)}",
                        'source_file': {
                            'gdrive_id': file_meta['id'],
                            'filename': file_meta['name']
                        }
                    }
                    # Fallback: optionally submit minimal invoice to Discord to validate pipeline
                    if os.getenv("DISCORD_SUBMIT_ON_FAILURE", "false").lower() in ("1", "true", "yes", "on"):
                        try:
                            created = (file_meta.get('createdTime') or datetime.now().isoformat())[:10]
                            payload = {
                                "Timestamp": datetime.now().isoformat(),
                                "Vendor Name": "Unknown Vendor",
                                "Invoice Number": f"FALLBACK-{file_meta['id']}",
                                "Invoice Date": created,
                                "Total Amount": "0.00",
                                "Currency": "USD",
                                "Line Items Count": "1",
                                "Account Holder": "Unknown",
                                "Bank Address": "Unknown",
                                "Account Number/IBAN": "UNKNOWN",
                            }
                            fut = submit_invoice(payload)
                            submitted_count[0] += 1
                            print(Fore.CYAN + f"Submitted fallback invoice to Discord agent (processing_failed): {payload['Invoice Number']}" + Style.RESET_ALL)
                            try:
                                fut.add_done_callback(lambda f: print(Fore.GREEN + "✓ Fallback submit result: " + str(f.result()) + Style.RESET_ALL) if not f.exception() else print(Fore.RED + f"✗ Fallback submit failed: {f.exception()}" + Style.RESET_ALL))
                            except Exception:
                                pass
                        except Exception as se:
                            print(Fore.RED + f"✗ Failed to submit fallback invoice after processing error: {se}" + Style.RESET_ALL)
                finally:
                    # Cleanup temp file
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception:
                        pass
                
                # Handle validation failures (missing mandatory fields)
                if result.get("status") == "validation_failed":
                    try:
                        self._handle_validation_failure(result, file_meta)
                    except Exception as e:
                        print(Fore.YELLOW + f"⚠️ Failed to handle validation failure follow-up: {e}" + Style.RESET_ALL)

                # Submit for approval if processed
                if result.get("status") == "completed":
                    payload = self._map_invoice_payload_for_agent(result)
                    if payload and payload.get("Invoice Number"):
                        try:
                            fut = submit_invoice(payload)  # background agent persists state
                            submitted_count[0] += 1
                            # Log result/exception asynchronously for visibility
                            def _cb(f):
                                try:
                                    res = f.result()
                                    print(Fore.GREEN + f"✓ Discord submit result for {payload['Invoice Number']}: {res}" + Style.RESET_ALL)
                                except Exception as ex:
                                    print(Fore.RED + f"✗ Discord submit failed for {payload['Invoice Number']}: {ex}" + Style.RESET_ALL)
                            try:
                                fut.add_done_callback(_cb)
                            except Exception:
                                # Some futures may not support callbacks; best-effort
                                pass
                        except Exception as e:
                            print(Fore.YELLOW + f"⚠️ Failed to submit invoice to Discord agent: {e}" + Style.RESET_ALL)
                return result
            
            # Run workers concurrently
            max_workers = min(4, len(files))  # cap concurrency
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(worker, f) for f in files]
                for _ in as_completed(futures):
                    pass  # We don't block overall workflow; just let background finish
            
            if submitted_count[0] == 0:
                print(Fore.YELLOW + "⚠️ No invoices were submitted to Discord because none processed successfully. Check Google Drive download reliability (set GDRIVE_DOWNLOAD_CHUNK_KB=64, GDRIVE_DOWNLOAD_MAX_ATTEMPTS=8) and rerun." + Style.RESET_ALL)
                # Optional: submit a small debug invoice to validate Discord path end-to-end
                if os.getenv("DISCORD_DEBUG_SUBMIT", "false").lower() in ("1", "true", "yes", "on"):
                    try:
                        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                        payload = {
                            "Timestamp": datetime.now().isoformat(),
                            "Vendor Name": "Debug Vendor",
                            "Invoice Number": f"DEBUG-{ts}",
                            "Invoice Date": datetime.now().strftime("%Y-%m-%d"),
                            "Total Amount": "1.00",
                            "Currency": "USD",
                            "Line Items Count": "1",
                            "Account Holder": "Debug Holder",
                            "Bank Address": "123 Debug Street",
                            "Account Number/IBAN": "US00DEBUG0001",
                        }
                        fut = submit_invoice(payload)
                        print(Fore.CYAN + f"Submitted debug invoice to Discord agent: {payload['Invoice Number']}" + Style.RESET_ALL)
                        def _cb(f):
                            try:
                                res = f.result()
                                print(Fore.GREEN + f"✓ Debug submit result: {res}" + Style.RESET_ALL)
                            except Exception as ex:
                                print(Fore.RED + f"✗ Debug submit failed: {ex}" + Style.RESET_ALL)
                        try:
                            fut.add_done_callback(_cb)
                        except Exception:
                            pass
                    except Exception as e:
                        print(Fore.RED + f"✗ Failed to submit debug invoice: {e}" + Style.RESET_ALL)
            print(Fore.GREEN + "✓ Background invoice processing completed for batch" + Style.RESET_ALL)
        except Exception as e:
            print(Fore.YELLOW + f"⚠️ Background invoice processing error: {e}" + Style.RESET_ALL)
    
    def process_invoices_from_gdrive(self, state: BaseGraph) -> BaseGraph:
        """Kick off background processing for multiple invoices (non-blocking)."""
        print(Fore.CYAN + "\n=== STARTING BACKGROUND INVOICE PROCESSING (CONCURRENT) ===" + Style.RESET_ALL)
        
        if not self.invoice_processor:
            print(Fore.YELLOW + "⚠️ Invoice processor not available. Skipping invoice processing." + Style.RESET_ALL)
            return {
                "invoice_processing_result": {
                    "status": "skipped",
                    "message": "Invoice processor not available"
                }
            }
        
        try:
            # Prefer processing attachments uploaded in this workflow run (PDFs only)
            session_attachments = (state.get("processed_attachments", []) or [])
            attachments_pdf = []
            for att in session_attachments:
                if not att.get("gdrive_file_id"):
                    continue
                fname = str(att.get("filename") or att.get("original_filename") or "").lower()
                mime = (att.get("mime_type") or "").lower()
                if mime == "application/pdf" or fname.endswith(".pdf"):
                    attachments_pdf.append(att)
            files_meta = [
                {
                    "id": att["gdrive_file_id"],
                    "name": att.get("filename") or att.get("original_filename") or f"invoice_{i}.pdf"
                }
                for i, att in enumerate(attachments_pdf)
                if att["gdrive_file_id"] not in self.processed_gdrive_ids
            ]
            max_files = int(os.getenv("MAX_INVOICE_FILES", "5"))
            if files_meta:
                print(Fore.CYAN + f"Found {len(files_meta)} uploaded PDF attachment(s) in this session to process" + Style.RESET_ALL)
            
            # Start background thread, do not block workflow
            threading.Thread(
                target=self._background_process_invoices,
                args=(max_files, files_meta if files_meta else None),
                daemon=True
            ).start()
            
            return {
                "invoice_processing_result": {
                    "status": "started",
                    "message": f"Background invoice processing started ({'session attachments' if files_meta else f'up to {max_files} latest files'})"
                }
            }
        except Exception as e:
            print(Fore.RED + f"❌ Error starting background invoice processing: {e}" + Style.RESET_ALL)
            return {
                "invoice_processing_result": {
                    "status": "error",
                    "error": str(e)
                }
            }

    # Discord pipeline wrapper methods
    def create_discord_thread(self, state: BaseGraph) -> BaseGraph:
        return self.discord_nodes.create_discord_thread(state)

    def check_discord_approval(self, state: BaseGraph) -> BaseGraph:
        return self.discord_nodes.check_discord_approval(state)

    def check_payment_confirmation(self, state: BaseGraph) -> BaseGraph:
        return self.discord_nodes.check_payment_confirmation(state)
