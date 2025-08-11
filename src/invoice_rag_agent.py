import json
import base64
from typing import Dict, List, Optional, TypedDict
from datetime import datetime
import os
import time

# Core dependencies
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage
import gspread
from google.oauth2.service_account import Credentials

# PDF processing
import PyPDF2
import fitz  # PyMuPDF for better PDF text extraction

from .discord_notifier import DiscordNotifier

# State definition for the graph
class InvoiceState(TypedDict):
    document_path: str
    raw_text: Optional[str]
    extracted_data: Optional[Dict]
    validation_status: str
    validation_errors: List[str]
    sheet_status: str
    final_result: Optional[Dict]

class InvoiceRAGAgent:
    def __init__(self, openai_api_key: str, google_creds_path: str, sheet_name: str):
        """
        Initialize the Invoice RAG Agent
        
        Args:
            openai_api_key: OpenAI API key for GPT-4
            google_creds_path: Path to Google service account JSON file
            sheet_name: Name of the Google Sheet to store results
        """
        self.llm = ChatOpenAI(
            model="gpt-4o",
            api_key=openai_api_key,
            temperature=0
        )
        
        # Initialize Google Sheets client
        self.gc = self._init_google_sheets(google_creds_path)
        self.sheet_name = sheet_name
        # Prefer ID+worksheet name from environment for consistency with Discord flow
        self.spreadsheet_id = os.getenv("SPREADSHEET_ID")
        self.worksheet_name = os.getenv("WORKSHEET_NAME", sheet_name)
        
        # Discord notifier (optional, gated to avoid duplicate posting)
        self.use_discord_notifier = os.getenv("USE_DISCORD_NOTIFIER", "false").lower() in ("1", "true", "yes", "on")
        self.discord = DiscordNotifier() if self.use_discord_notifier else None
        
        # Create the LangGraph workflow
        self.graph = self._create_graph()
    
    def _init_google_sheets(self, creds_path: str):
        """Initialize Google Sheets client"""
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        return gspread.authorize(creds)
    
    def _create_graph(self) -> StateGraph:
        """Create the LangGraph workflow"""
        workflow = StateGraph(InvoiceState)
        
        # Add nodes
        workflow.add_node("extract_data", self.extract_data_node)
        workflow.add_node("validate_data", self.validate_data_node)
        workflow.add_node("store_in_sheets", self.store_in_sheets_node)
        
        # Define the flow
        workflow.set_entry_point("extract_data")
        workflow.add_edge("extract_data", "validate_data")
        workflow.add_conditional_edges(
            "validate_data",
            self._validation_router,
            {
                "valid": "store_in_sheets",
                "invalid": END
            }
        )
        workflow.add_edge("store_in_sheets", END)
        
        return workflow.compile()
    
    def extract_data_node(self, state: InvoiceState) -> InvoiceState:
        """Node to extract structured data from invoice using GPT-4"""
        print("📄 Extracting data from invoice...")
        
        try:
            file_path = state["document_path"]
            file_ext = file_path.lower().split('.')[-1]
            
            if file_ext in ['png', 'jpg', 'jpeg', 'gif', 'bmp']:
                # Handle image files
                with open(file_path, "rb") as f:
                    image_data = base64.b64encode(f.read()).decode()
                    message = HumanMessage(
                        content=[
                            {
                                "type": "text",
                                "text": self._get_extraction_prompt()
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{image_data}"
                                }
                            }
                        ]
                    )
            
            elif file_ext == 'pdf':
                # Handle PDF files (text + image-render fallback for image-based PDFs)
                text_content = self._extract_pdf_text(file_path)
                if not text_content.strip():
                    print("⚠️ No text extracted from PDF. PDF might be image-based.")
                # Keep a copy of raw text in state (even if minimal) for traceability
                state["raw_text"] = text_content or ""
                
                # Build multimodal content: always include prompt + a truncated text snippet if available
                text_snippet = (text_content or "")[:8000]
                content_parts = [{
                    "type": "text",
                    "text": f"{self._get_extraction_prompt()}\n\nDocument Content:\n{text_snippet or '(no selectable text extracted; rely on images)'}"
                }]
                
                # If text is insufficient, render first N pages to images and include in the message
                if not text_content.strip() or len(text_content) < 200:
                    try:
                        print("🖼️ Rendering first PDF pages to images for vision extraction...")
                        images_b64 = self._render_pdf_pages_to_base64(file_path, max_pages=2, zoom=2.0)
                        for b64 in images_b64:
                            content_parts.append({
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{b64}"}
                            })
                    except Exception as e:
                        print(f"⚠️ PDF image render fallback failed: {e}")
                
                message = HumanMessage(content=content_parts)
            
            else:
                # Handle text files
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    text_content = f.read()
                    state["raw_text"] = text_content
                    message = HumanMessage(
                        content=f"{self._get_extraction_prompt()}\n\nDocument Content:\n{text_content}"
                    )
            
            print(f"📝 Processing {file_ext.upper()} file...")
            response = self.llm.invoke([message])
            
            print(f"🤖 GPT-4 Response preview: {response.content[:200]}...")
            
            # Clean and parse the JSON response
            response_text = response.content.strip()
            
            # Try to find JSON in the response
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start != -1 and json_end > json_start:
                json_text = response_text[json_start:json_end]
                try:
                    extracted_data = json.loads(json_text)
                    state["extracted_data"] = extracted_data
                    print("✅ Data extraction completed successfully")
                    print(f"📊 Extracted vendor: {extracted_data.get('vendor_name', 'N/A')}")
                    print(f"📊 Invoice number: {extracted_data.get('invoice_number', 'N/A')}")
                except json.JSONDecodeError as e:
                    print(f"❌ Failed to parse JSON response: {e}")
                    print(f"🔍 JSON text attempted: {json_text[:100]}...")
                    state["extracted_data"] = None
            else:
                print("❌ No valid JSON found in response")
                print(f"🔍 Full response: {response_text}")
                state["extracted_data"] = None
                
        except Exception as e:
            print(f"❌ Error during data extraction: {e}")
            state["extracted_data"] = None
        
        return state
    
    def validate_data_node(self, state: InvoiceState) -> InvoiceState:
        """Node to validate extracted data for completeness"""
        print("🔍 Validating extracted data...")
        
        if not state["extracted_data"]:
            state["validation_status"] = "invalid"
            state["validation_errors"] = ["No data extracted"]
            return state
        
        required_fields = [
            "vendor_name",
            "invoice_number",
            "invoice_date",
            "line_items",
            "total_amount",
            "currency"
        ]
        
        errors = []
        data = state["extracted_data"]
        
        # Check required fields
        for field in required_fields:
            if field not in data or not data[field]:
                errors.append(f"Missing required field: {field}")
        
        # Validate line items structure
        if "line_items" in data and data["line_items"]:
            for i, item in enumerate(data["line_items"]):
                if not all(key in item for key in ["description", "quantity", "price"]):
                    errors.append(f"Incomplete line item {i+1}")
        
        # Validate payment details if present
        if "payment_details" in data and data["payment_details"]:
            payment_fields = ["account_holder", "bank_address", "account_number"]
            payment_data = data["payment_details"]
            missing_payment = [f for f in payment_fields if f not in payment_data or not payment_data[f]]
            if missing_payment:
                errors.append(f"Incomplete payment details: {', '.join(missing_payment)}")
        
        state["validation_errors"] = errors
        state["validation_status"] = "valid" if not errors else "invalid"
        
        if errors:
            print(f"❌ Validation failed: {len(errors)} errors found")
            for error in errors:
                print(f"  • {error}")
        else:
            print("✅ Validation passed successfully")
        
        return state
    
    def store_in_sheets_node(self, state: InvoiceState) -> InvoiceState:
        """Node to store validated data in Google Sheets and notify Discord"""
        print("📊 Storing data in Google Sheets...")

        # Canonical header used across the project (must match discord1.py)
        expected_headers = [
            "Timestamp", "Vendor Name", "Invoice Number", "Invoice Date",
            "Total Amount", "Currency", "Line Items Count", "Account Holder",
            "Bank Address", "Account Number/IBAN", "Discord Thread URL",
            "Status", "Approver", "Cost Center", "Rejection Reason",
            "Payment Status", "Transaction ID", "Paid Amount (ETH)", "Created At", "Updated At"
        ]

        try:
            # Resolve spreadsheet and worksheet consistently
            try:
                if getattr(self, "spreadsheet_id", None):
                    ss = self.gc.open_by_key(self.spreadsheet_id)
                    print(f"✅ Opened spreadsheet by ID: {self.spreadsheet_id}")
                else:
                    ss = self.gc.open(self.sheet_name)
                    print(f"✅ Opened spreadsheet by name: {self.sheet_name}")

                try:
                    ws = ss.worksheet(self.worksheet_name if getattr(self, "worksheet_name", None) else "Invoice")
                    print(f"✅ Using worksheet: {ws.title}")
                except gspread.WorksheetNotFound:
                    print(f"ℹ️ Worksheet '{self.worksheet_name}' not found. Creating new worksheet.")
                    ws = ss.add_worksheet(
                        title=(self.worksheet_name or "Invoice"),
                        rows=1000,
                        cols=len(expected_headers)
                    )
                    ws.update(f"A1:T1", [expected_headers])
                    print("🧭 Initialized headers in new worksheet")

            except gspread.SpreadsheetNotFound:
                print(f"❌ Spreadsheet not found (ID={getattr(self, 'spreadsheet_id', None)} name={self.sheet_name}).")
                return self._save_to_local_file(state)

            # Ensure headers are exactly as expected (no shifting columns)
            try:
                current_headers = ws.row_values(1)
                if current_headers != expected_headers:
                    print(f"🛠️ Normalizing header row. Current count={len(current_headers)} Expected={len(expected_headers)}")
                    ws.update(f"A1:T1", [expected_headers])
                else:
                    print("✅ Header row already normalized")
            except Exception as e:
                print(f"⚠️ Header check/update failed: {e}. Proceeding with best effort.")

            data = state.get("extracted_data") or {}
            now_iso = datetime.now().isoformat()

            # Build row exactly in header order (20 columns)
            row_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),                # Timestamp
                data.get("vendor_name", ""),                                 # Vendor Name
                data.get("invoice_number", ""),                              # Invoice Number
                data.get("invoice_date", ""),                                # Invoice Date
                data.get("total_amount", ""),                                # Total Amount
                data.get("currency", ""),                                    # Currency
                len(data.get("line_items", []) or []),                       # Line Items Count
                data.get("payment_details", {}).get("account_holder", ""),   # Account Holder
                data.get("payment_details", {}).get("bank_address", ""),     # Bank Address
                data.get("payment_details", {}).get("account_number", ""),   # Account Number/IBAN
                "",                                                          # Discord Thread URL (to be updated)
                "pending",                                                   # Status
                "",                                                          # Approver
                "",                                                          # Cost Center
                "",                                                          # Rejection Reason
                "pending",                                                   # Payment Status
                "",                                                          # Transaction ID
                "",                                                          # Paid Amount (ETH)
                now_iso,                                                     # Created At
                now_iso                                                      # Updated At
            ]

            # Append with retries for robustness
            append_ok = False
            last_exc = None
            for attempt in range(1, 4):
                try:
                    print(f"➡️ Appending row (attempt {attempt}) with {len(row_data)} columns")
                    ws.append_row(row_data, value_input_option="RAW")
                    append_ok = True
                    break
                except Exception as e:
                    last_exc = e
                    print(f"⚠️ append_row failed on attempt {attempt}: {e}")
                    time.sleep(min(1.5 * attempt, 5.0))
            if not append_ok:
                print("❌ append_row failed after retries, falling back to local file save")
                return self._save_to_local_file(state)

            # Compute last row for updates
            try:
                last_row = len(ws.get_all_values())
                print(f"📌 Last row index determined: {last_row}")
            except Exception as e:
                print(f"⚠️ Failed to determine last row: {e}")
                last_row = None

            # Post to Discord and capture thread URL (optional feature)
            thread_url = None
            if self.discord and getattr(self, "use_discord_notifier", False) and self.discord.is_configured():
                payload = {
                    "vendor_name": data.get("vendor_name", ""),
                    "invoice_number": data.get("invoice_number", ""),
                    "invoice_date": data.get("invoice_date", ""),
                    "total_amount": data.get("total_amount", ""),
                    "currency": data.get("currency", "")
                }
                try:
                    print("💬 Posting invoice to Discord...")
                    thread_url = self.discord.post_invoice_and_create_thread(payload)
                    print(f"✅ Discord thread created: {thread_url}")
                except Exception as e:
                    print(f"⚠️ Discord post failed: {e}")

            # Update sheet with Discord URL if available
            if thread_url and last_row:
                try:
                    discord_col = expected_headers.index("Discord Thread URL") + 1
                    ws.update_cell(last_row, discord_col, thread_url)
                    print(f"🔗 Updated Discord URL at row {last_row}, col {discord_col}")
                except Exception as e:
                    print(f"⚠️ Failed to update Discord URL in sheet: {e}")

            state["sheet_status"] = "success"
            state["final_result"] = {
                "status": "completed",
                "data": data,
                "sheet_url": f"https://docs.google.com/spreadsheets/d/{ws.spreadsheet.id}",
                "discord_thread_url": thread_url
            }

            print("✅ Data stored successfully in Google Sheets")

        except Exception as e:
            print(f"⚠️ Google Sheets/Discord step failed: {e}")
            print("💾 Falling back to local file storage...")
            return self._save_to_local_file(state)

        return state
    
    def _save_to_local_file(self, state: InvoiceState) -> InvoiceState:
        """Fallback: Save data to local CSV/JSON file"""
        try:
            data = state["extracted_data"]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save as JSON
            json_filename = f"invoice_data_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "extracted_data": data
                }, f, indent=2, ensure_ascii=False)
            
            # Also save as CSV for easy viewing
            csv_filename = f"invoice_data_{timestamp}.csv"
            import csv
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write headers
                writer.writerow([
                    "Timestamp", "Vendor Name", "Invoice Number", "Invoice Date",
                    "Total Amount", "Currency", "Line Items Count",
                    "Account Holder", "Bank Address", "Account Number/IBAN"
                ])
                
                # Write data
                writer.writerow([
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    data.get("vendor_name", ""),
                    data.get("invoice_number", ""),
                    data.get("invoice_date", ""),
                    data.get("total_amount", ""),
                    data.get("currency", ""),
                    len(data.get("line_items", [])),
                    data.get("payment_details", {}).get("account_holder", ""),
                    data.get("payment_details", {}).get("bank_address", ""),
                    data.get("payment_details", {}).get("account_number", "")
                ])
            
            state["sheet_status"] = "local_file_saved"
            state["final_result"] = {
                "status": "completed",
                "data": data,
                "local_files": [json_filename, csv_filename],
                "note": "Saved to local files due to Google Drive quota issue"
            }
            
            print(f"✅ Data saved to local files: {json_filename} and {csv_filename}")
            
        except Exception as e:
            print(f"❌ Failed to save to local file: {e}")
            state["sheet_status"] = "failed"
            state["final_result"] = {
                "status": "failed",
                "error": f"Both Google Sheets and local file storage failed: {str(e)}"
            }
        
        return state
    
    def _extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from PDF using multiple methods"""
        text = ""
        
        try:
            # Method 1: Try PyMuPDF (fitz) - better for complex PDFs
            doc = fitz.open(pdf_path)
            for page_num in range(doc.page_count):
                page = doc.load_page(page_num)
                text += page.get_text()
            doc.close()
            
            if text.strip():
                print(f"✅ Extracted text using PyMuPDF: {len(text)} characters")
                return text
                
        except Exception as e:
            print(f"⚠️ PyMuPDF extraction failed: {e}")
        
        try:
            # Method 2: Fallback to PyPDF2
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text += page.extract_text()
            
            if text.strip():
                print(f"✅ Extracted text using PyPDF2: {len(text)} characters")
                return text
                
        except Exception as e:
            print(f"⚠️ PyPDF2 extraction failed: {e}")
        
        print("⚠️ No text could be extracted from PDF - might be image-based")
        return text

    def _render_pdf_pages_to_base64(self, pdf_path: str, max_pages: int = 2, zoom: float = 2.0) -> List[str]:
        """
        Render the first few pages of a PDF to PNG images and return as base64 strings.
        Useful for image-based PDFs where text extraction fails.
        """
        images: List[str] = []
        try:
            doc = fitz.open(pdf_path)
            page_count = min(doc.page_count, max_pages)
            mat = fitz.Matrix(zoom, zoom)
            for i in range(page_count):
                page = doc.load_page(i)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                png_bytes = pix.tobytes("png")
                images.append(base64.b64encode(png_bytes).decode("utf-8"))
            doc.close()
            print(f"✅ Rendered {len(images)} PDF page(s) to images for vision model")
        except Exception as e:
            print(f"⚠️ Failed to render PDF pages to images: {e}")
        return images
    
    def _validation_router(self, state: InvoiceState) -> str:
        """Router to determine next step based on validation"""
        return "valid" if state["validation_status"] == "valid" else "invalid"
    
    def _get_extraction_prompt(self) -> str:
        """Get the prompt for data extraction"""
        return """
        You are a Document Intelligence Service. Extract structured data from this invoice image/document.
        
        Return the data in this exact JSON format:
        {
            "vendor_name": "Company name issuing the invoice",
            "invoice_number": "Invoice reference number",
            "invoice_date": "Date invoice was issued (YYYY-MM-DD format)",
            "line_items": [
                {
                    "description": "Item or service description",
                    "quantity": "Quantity ordered",
                    "price": "Unit price"
                }
            ],
            "total_amount": "Total amount to be paid",
            "currency": "Currency code (e.g., USD, EUR)",
            "payment_details": {
                "account_holder": "Bank account holder name",
                "bank_address": "Bank address",
                "account_number": "Account number or IBAN",
                "routing_code": "Routing or IFSC code",
                "bic_swift_code": "BIC or SWIFT code",
                "wallet_address": "Crypto wallet address if applicable"
            }
        }
        
        Extract all available information. If a field is not present in the document, use an empty string "".
        Ensure the response is valid JSON only, no additional text or explanations.
        """
    
    def process_invoice(self, document_path: str) -> Dict:
        """
        Process a single invoice through the complete pipeline
        
        Args:
            document_path: Path to the invoice document (image, PDF, or text file)
        
        Returns:
            Dictionary with processing results
        """
        print(f"🚀 Starting invoice processing for: {document_path}")
        
        # Check if file exists
        if not os.path.exists(document_path):
            return {
                "status": "failed",
                "error": f"File not found: {document_path}"
            }
        
        initial_state = InvoiceState(
            document_path=document_path,
            raw_text=None,
            extracted_data=None,
            validation_status="pending",
            validation_errors=[],
            sheet_status="pending",
            final_result=None
        )
        
        try:
            # Run the graph
            final_state = self.graph.invoke(initial_state)
            
            print("🎉 Invoice processing completed!")
            
            # Ensure we have a final result
            if final_state.get("final_result") is None:
                if final_state.get("validation_status") == "invalid":
                    return {
                        "status": "validation_failed",
                        "errors": final_state.get("validation_errors", []),
                        "extracted_data": final_state.get("extracted_data")
                    }
                else:
                    return {
                        "status": "extraction_failed",
                        "error": "No data could be extracted from the document"
                    }
            
            return final_state["final_result"]
            
        except Exception as e:
            print(f"❌ Processing pipeline failed: {e}")
            return {
                "status": "pipeline_failed",
                "error": str(e)
            }
