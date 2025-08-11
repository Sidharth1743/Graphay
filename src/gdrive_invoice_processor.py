import os
import tempfile
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Google Drive API
from google.auth.transport.requests import Request, AuthorizedSession
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Invoice RAG Agent
from .invoice_rag_agent import InvoiceRAGAgent
from .database import EmailDatabase
from .tools.gmail_tool import GmailTool
from .state import Email

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class GDriveInvoiceProcessor:
    def __init__(self, openai_api_key: str, google_creds_path: str = "gdrive.json", 
                 token_file: str = "token1.json", sheet_name: str = "Invoice"):
        """
        Initialize the Google Drive Invoice Processor
        
        Args:
            openai_api_key: OpenAI API key for GPT-4
            google_creds_path: Path to Google Drive API credentials JSON file
            token_file: Path to store authentication token
            sheet_name: Name of the Google Sheet to store results
        """
        # Load environment variables
        load_dotenv()
        
        self.openai_api_key = openai_api_key
        self.credentials_file = google_creds_path
        self.token_file = token_file
        self.service = None
        self._authenticate()
        
        # Initialize the Invoice RAG Agent
        # Use g_sheets.json for Google Sheets authentication (service account)
        sheets_creds_path = "g_sheets.json"
        self.rag_agent = InvoiceRAGAgent(
            openai_api_key=openai_api_key,
            google_creds_path=sheets_creds_path,
            sheet_name=sheet_name
        )
        # Initialize database and Gmail tool for follow-up handling
        self.email_db = EmailDatabase()
        self.gmail_tools = GmailTool()
    
    def _authenticate(self):
        """Authenticate with Google Drive API"""
        creds = None
        
        # Load existing token
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        
        # If no valid credentials, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_file):
                    raise FileNotFoundError(f"Google Drive credentials file not found: {self.credentials_file}")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        
        self.creds = creds
        self.service = build('drive', 'v3', credentials=creds)
        print("✓ Google Drive authentication successful")
    
    def get_latest_invoice_files(self, folder_id: str = None, max_files: int = 5) -> List[Dict]:
        """
        Get the latest invoice files from Google Drive
        
        Args:
            folder_id: Google Drive folder ID (optional, uses GDRIVE_FOLDER_ID from .env if not provided)
            max_files: Maximum number of files to retrieve
        
        Returns:
            List of file metadata dictionaries
        """
        if not folder_id:
            folder_id = os.getenv('GDRIVE_FOLDER_ID')
            if not folder_id:
                raise ValueError("No folder ID provided and GDRIVE_FOLDER_ID not found in .env file")
        
        try:
            # Query for files in the specified folder
            query = f"'{folder_id}' in parents and trashed=false"
            
            # Get files sorted by creation time (newest first)
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name,mimeType,createdTime,size)',
                orderBy='createdTime desc',
                pageSize=max_files
            ).execute()
            
            files = results.get('files', [])
            
            # Filter for invoice-related files
            invoice_files = []
            for file in files:
                if self._is_invoice_file(file):
                    invoice_files.append(file)
            
            # Optional: filter by max size to reduce download failures on large files
            try:
                max_mb = float(os.getenv('MAX_INVOICE_SIZE_MB', '12'))  # default 12 MB
            except Exception:
                max_mb = 12.0
            max_bytes = int(max_mb * 1024 * 1024)
            filtered_files = []
            skipped_large = 0
            for file in invoice_files:
                try:
                    size_bytes = int(file.get('size', 0))
                except Exception:
                    size_bytes = 0
                if size_bytes and size_bytes > max_bytes:
                    skipped_large += 1
                    print(f"⚠️ Skipping large file (> {max_mb} MB): {file.get('name')} ({round(size_bytes/1024/1024,2)} MB)")
                    continue
                filtered_files.append(file)
            
            print(f"✓ Found {len(filtered_files)} invoice files in Google Drive" + (f" (skipped {skipped_large} large files)" if skipped_large else ""))
            for file in filtered_files:
                print(f"  📄 {file['name']} (ID: {file['id']})")
            
            return filtered_files
            
        except Exception as e:
            print(f"❌ Error retrieving files from Google Drive: {e}")
            return []
    
    def _is_invoice_file(self, file: Dict) -> bool:
        """Check if a file is likely an invoice based on name and type"""
        name = file.get('name', '').lower()
        mime_type = file.get('mimeType', '')
        
        # Check for invoice-related keywords in filename
        invoice_keywords = ['invoice', 'bill', 'receipt', 'statement', 'payment']
        if any(keyword in name for keyword in invoice_keywords):
            return True
        
        # Check for supported file types
        supported_types = [
            'application/pdf',
            'image/jpeg',
            'image/png',
            'image/gif',
            'image/bmp',
            'text/plain'
        ]
        
        return mime_type in supported_types
    
    def download_file(self, file_id: str, filename: str = None) -> Optional[str]:
        """
        Download a file from Google Drive to a temporary location
        
        Args:
            file_id: Google Drive file ID
            filename: Optional filename to use (defaults to Google Drive filename)
        
        Returns:
            Path to the downloaded file, or None if download failed
        """
        try:
            # Get file metadata
            file_metadata = self.service.files().get(
                fileId=file_id,
                fields='name,mimeType'
            ).execute()
            
            if not filename:
                filename = file_metadata.get('name', f'file_{file_id}')
            
            # Create temporary file
            temp_dir = tempfile.gettempdir()
            temp_path = os.path.join(temp_dir, filename)
            
            # Download the file with robust retries and smaller chunks
            request = self.service.files().get_media(fileId=file_id)
            
            chunk_kb = int(os.getenv('GDRIVE_DOWNLOAD_CHUNK_KB', '256'))  # default 256KB
            chunk_size = max(64 * 1024, chunk_kb * 1024)
            max_attempts = int(os.getenv('GDRIVE_DOWNLOAD_MAX_ATTEMPTS', '5'))
            
            with open(temp_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request, chunksize=chunk_size)
                done = False
                attempt = 0
                while not done:
                    try:
                        status, done = downloader.next_chunk(num_retries=3)
                        if status:
                            print(f"  📥 Download progress: {int(status.progress() * 100)}%")
                    except Exception as e:
                        attempt += 1
                        if attempt >= max_attempts:
                            raise
                        backoff = min(2 ** attempt, 10)
                        print(f"⚠️ Download chunk error: {e}. Retrying in {backoff}s (attempt {attempt}/{max_attempts})")
                        time.sleep(backoff)
            
            print(f"✅ Downloaded: {filename} to {temp_path}")
            return temp_path
            
        except Exception as e:
            print(f"❌ Error downloading file {file_id}: {e}")
            # Fallback: try AuthorizedSession direct download with safe destination
            try:
                safe_name = filename if 'filename' in locals() and filename else f'file_{file_id}'
                safe_dest = os.path.join(tempfile.gettempdir(), safe_name)
                fallback_path = self._download_via_authorized_session(file_id, safe_dest)
                if fallback_path:
                    print(f"✅ Downloaded via AuthorizedSession: {safe_name} to {fallback_path}")
                    return fallback_path
            except Exception as e2:
                print(f"❌ Fallback AuthorizedSession download failed: {e2}")
            return None
    
    def _download_via_authorized_session(self, file_id: str, dest_path: str) -> Optional[str]:
        """
        Fallback downloader using AuthorizedSession to avoid SSL issues with MediaIoBaseDownload.
        """
        if not getattr(self, "creds", None):
            return None
        session = AuthorizedSession(self.creds)
        url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        max_attempts = int(os.getenv('GDRIVE_DOWNLOAD_MAX_ATTEMPTS', '5'))
        chunk_size = max(64 * 1024, int(os.getenv('GDRIVE_DOWNLOAD_CHUNK_KB', '128')) * 1024)
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            try:
                with session.get(url, stream=True, timeout=30) as resp:
                    resp.raise_for_status()
                    with open(dest_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                return dest_path
            except Exception as e:
                if attempt >= max_attempts:
                    raise
                backoff = min(2 ** attempt, 10)
                print(f"⚠️ AuthorizedSession download error: {e}. Retrying in {backoff}s (attempt {attempt}/{max_attempts})")
                time.sleep(backoff)

        return None

    def process_latest_invoice(self, folder_id: str = None) -> Dict:
        """
        Process the latest invoice file from Google Drive
        
        Args:
            folder_id: Google Drive folder ID (optional)
        
        Returns:
            Processing result dictionary
        """
        print("🔍 Looking for latest invoice file in Google Drive...")
        
        # Get the latest invoice files
        files = self.get_latest_invoice_files(folder_id, max_files=1)
        
        if not files:
            return {
                "status": "no_files_found",
                "error": "No invoice files found in the specified Google Drive folder"
            }
        
        latest_file = files[0]
        print(f"📄 Processing latest file: {latest_file['name']}")
        
        # Download the file
        temp_path = self.download_file(latest_file['id'], latest_file['name'])
        
        if not temp_path:
            return {
                "status": "download_failed",
                "error": f"Failed to download file: {latest_file['name']}"
            }
        
        try:
            # Process the invoice using RAG agent
            result = self.rag_agent.process_invoice(temp_path)
            
            # Add file metadata to result
            result['source_file'] = {
                'gdrive_id': latest_file['id'],
                'filename': latest_file['name'],
                'created_time': latest_file.get('createdTime'),
                'temp_path': temp_path
            }

            # Handle validation failures: send consolidated email and schedule reminder
            if result.get("status") == "validation_failed":
                try:
                    self._handle_validation_failure_followup(result)
                except Exception as e:
                    print(f"⚠️ Failed to handle validation failure follow-up: {e}")
            
            return result
            
        except Exception as e:
            return {
                "status": "processing_failed",
                "error": f"Failed to process invoice: {str(e)}",
                'source_file': {
                    'gdrive_id': latest_file['id'],
                    'filename': latest_file['name'],
                    'temp_path': temp_path
                }
            }
    
    def process_invoice_file(self, file_id: str, filename: str) -> Dict:
        """
        Process a specific Google Drive file (by ID) as an invoice.
        Returns a result dict with status and extracted data on success.
        """
        print(f"📄 Processing specified file: {filename} (ID: {file_id})")
        # Download the file
        temp_path = self.download_file(file_id, filename)
        if not temp_path:
            return {
                "status": "download_failed",
                "error": f"Failed to download file: {filename}"
            }
        try:
            # Process the invoice using RAG agent
            result = self.rag_agent.process_invoice(temp_path)
            # Add file metadata to result
            result['source_file'] = {
                'gdrive_id': file_id,
                'filename': filename,
                'temp_path': temp_path
            }

            # Handle validation failures: send consolidated email and schedule reminder
            if result.get("status") == "validation_failed":
                try:
                    self._handle_validation_failure_followup(result)
                except Exception as e:
                    print(f"⚠️ Failed to handle validation failure follow-up: {e}")

            return result
        except Exception as e:
            return {
                "status": "processing_failed",
                "error": f"Failed to process invoice: {str(e)}",
                'source_file': {
                    'gdrive_id': file_id,
                    'filename': filename,
                    'temp_path': temp_path
                }
            }
        finally:
            # Cleanup temp file
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
    
    def process_multiple_invoices(self, folder_id: str = None, max_files: int = 5) -> List[Dict]:
        """
        Process multiple invoice files from Google Drive
        
        Args:
            folder_id: Google Drive folder ID (optional)
            max_files: Maximum number of files to process
        
        Returns:
            List of processing result dictionaries
        """
        print(f"🔍 Looking for up to {max_files} invoice files in Google Drive...")
        
        # Get invoice files
        files = self.get_latest_invoice_files(folder_id, max_files=max_files)
        
        if not files:
            return [{
                "status": "no_files_found",
                "error": "No invoice files found in the specified Google Drive folder"
            }]
        
        results = []
        
        for i, file in enumerate(files, 1):
            print(f"\n📄 Processing file {i}/{len(files)}: {file['name']}")
            
            # Download the file
            temp_path = self.download_file(file['id'], file['name'])
            
            if not temp_path:
                results.append({
                    "status": "download_failed",
                    "error": f"Failed to download file: {file['name']}",
                    'source_file': {
                        'gdrive_id': file['id'],
                        'filename': file['name']
                    }
                })
                continue
            
            try:
                # Process the invoice using RAG agent
                result = self.rag_agent.process_invoice(temp_path)
                
                # Add file metadata to result
                result['source_file'] = {
                    'gdrive_id': file['id'],
                    'filename': file['name'],
                    'created_time': file.get('createdTime'),
                    'temp_path': temp_path
                }

                # Handle validation failures: send consolidated email and schedule reminder
                if result.get("status") == "validation_failed":
                    try:
                        self._handle_validation_failure_followup(result)
                    except Exception as e:
                        print(f"⚠️ Failed to handle validation failure follow-up: {e}")
                
                results.append(result)
                
            except Exception as e:
                results.append({
                    "status": "processing_failed",
                    "error": f"Failed to process invoice: {str(e)}",
                    'source_file': {
                        'gdrive_id': file['id'],
                        'filename': file['name'],
                        'temp_path': temp_path
                    }
                })
        
        return results
    
    def cleanup_temp_files(self, results: List[Dict]):
        """Clean up temporary files after processing"""
        for result in results:
            if 'source_file' in result and 'temp_path' in result['source_file']:
                temp_path = result['source_file']['temp_path']
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        print(f"🗑️ Cleaned up: {temp_path}")
                except Exception as e:
                    print(f"⚠️ Failed to clean up {temp_path}: {e}")

    def _add_business_days(self, start_dt: datetime, days: int) -> datetime:
        """Add business days (Mon–Fri) to a datetime."""
        remaining = days
        current = start_dt
        while remaining > 0:
            current += timedelta(days=1)
            if current.weekday() < 5:
                remaining -= 1
        return current

    def _compose_missing_info_email(self, missing_fields: List[str], original_subject: str, is_reminder: bool = False) -> str:
        """Create a consolidated email listing required information."""
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

    def _handle_validation_failure_followup(self, result: Dict) -> None:
        """Send consolidated missing-info email and persist one-time reminder schedule."""
        try:
            src = result.get("source_file") or {}
            gdrive_file_id = src.get("gdrive_id")
            if not gdrive_file_id:
                return

            # Idempotency: skip if an unresolved follow-up already exists
            try:
                existing = self.email_db.get_open_followup_by_file_id(gdrive_file_id)
                if existing:
                    return
            except Exception:
                pass

            # Find originating email via attachments mapping
            email_row = self.email_db.get_email_by_gdrive_file_id(gdrive_file_id)
            if not email_row:
                # Fallback: attempt to find originating email via Gmail search (by filename/vendor/invoice no.)
                src = result.get("source_file") or {}
                src_name = src.get("filename") or ""
                email_info = self._try_find_originating_email_via_gmail(src_name, result)
                if not email_info:
                    print(f"⚠️ Could not locate originating email for GDrive file {gdrive_file_id}")
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

            # Prepare missing fields list
            errors = result.get("errors") or []
            if isinstance(errors, list):
                missing_fields = errors
            else:
                missing_fields = [str(errors)]

            # Build Email object for thread reply
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

            # Send consolidated email in the existing thread
            body = self._compose_missing_info_email(missing_fields, subject, is_reminder=False)
            self.gmail_tools.send_reply(initial_email, body)
            sent_at = datetime.now()
            reminder_due = self._add_business_days(sent_at, 3)

            # Persist follow-up for one-time reminder
            self.email_db.create_followup(
                email_id=eid,
                thread_id=thread_id,
                gdrive_file_id=gdrive_file_id,
                missing_fields=missing_fields,
                initial_notice_sent_at=sent_at.isoformat(),
                reminder_due_at=reminder_due.isoformat()
            )
            print(f"📬 Sent missing-info email; follow-up created for file {gdrive_file_id}, reminder due {reminder_due.isoformat()}")
        except Exception as e:
            print(f"⚠️ Validation failure follow-up error: {e}")

    def _try_find_originating_email_via_gmail(self, src_name: str, result: Dict) -> Optional[Dict]:
        """
        Fallback strategy to locate the originating Gmail message when DB mapping is unavailable.
        Tries:
          1) filename: query for the invoice filename
          2) subject/vendor/invoice number heuristics
        Returns normalized email_info dict compatible with Email model (id, threadId, messageId, references, sender, subject, body).
        """
        try:
            # 1) Search by filename if available
            if src_name:
                msgs = self.gmail_tools.search_messages(f'filename:"{src_name}"', max_results=5)
                for m in msgs:
                    info = self.gmail_tools.get_email_info_by_id(m.get("id"))
                    if info:
                        return info
            # 2) Search by fields in extracted data
            data = result.get("extracted_data") or {}
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
            print(f"⚠️ Gmail fallback search failed: {e}")
        return None
