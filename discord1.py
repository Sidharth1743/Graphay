import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging
import os
from dotenv import load_dotenv

import discord
from discord.ext import tasks
import gspread
from google.oauth2.service_account import Credentials
from langgraph.graph import StateGraph, START, END
from langchain_openai import ChatOpenAI
import sqlite3

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# State Management using TypedDict for LangGraph compatibility
from typing_extensions import TypedDict

class InvoiceState(TypedDict):
    invoice_data: Dict[str, Any]
    discord_thread_id: Optional[str]
    discord_message_id: Optional[str]
    approval_status: str  # "pending", "approved", "rejected"
    cost_center: Optional[str]
    approver: Optional[str]
    rejection_reason: Optional[str]
    payment_status: str  # "pending", "completed"
    transaction_id: Optional[str]
    paid_amount_eth: Optional[float]
    reminder_count: int
    last_reminder: Optional[str]
    spreadsheet_row: Optional[int]
    sla_message_id: Optional[str]
    approval_sla_hours: int

@dataclass
class Config:
    # Discord Configuration
    discord_token: str
    discord_channel_id: int
    approving_team_role_id: int
    
    # OpenAI Configuration
    openai_api_key: str
    
    # Google Sheets Configuration
    google_sheets_credentials_file: str
    google_drive_credentials_file: str
    spreadsheet_id: str
    worksheet_name: str = "Invoice"
    
    # Timing Configuration
    reminder_interval_hours: int = 24
    max_reminders: int = 5
    approval_sla_hours: int = 24
    # Fallback by channel name when ID is not accessible
    fallback_channel_name: Optional[str] = None

from src.eth.etherscan_client import check_transaction_success, get_transaction_amount_eth

class InvoiceApprovalAgent:
    def __init__(self, config: Config):
        self.config = config
        self.llm = ChatOpenAI(
            model="gpt-4",
            api_key=config.openai_api_key,
            temperature=0.1
        )
        
        # Initialize Discord client
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        self.discord_client = discord.Client(intents=intents)
        self.fallback_channel_name = getattr(config, "fallback_channel_name", None)
        self.shutting_down = False
        
        # Initialize Google Sheets
        self.init_google_sheets()
        
        # Initialize local database for state persistence
        self.init_database()
        
        # Create LangGraph workflow
        self.workflow = self.create_workflow()
        
        # Setup Discord event handlers
        self.setup_discord_handlers()
        
        # Reminder task will be started on Discord on_ready to ensure a running loop
    
    def init_google_sheets(self):
        """Initialize Google Sheets client with separate credentials"""
        try:
            # Use Google Sheets credentials
            scope = ["https://www.googleapis.com/auth/spreadsheets"]
            sheets_creds = Credentials.from_service_account_file(
                self.config.google_sheets_credentials_file, 
                scopes=scope
            )
            
            self.gc = gspread.authorize(sheets_creds)
            self.sheet = self.gc.open_by_key(self.config.spreadsheet_id)
            
            # Create worksheet if it doesn't exist
            try:
                self.worksheet = self.sheet.worksheet(self.config.worksheet_name)
            except gspread.WorksheetNotFound:
                self.worksheet = self.sheet.add_worksheet(
                    title=self.config.worksheet_name,
                    rows=1000,
                    cols=20
                )
            # Normalize header row to expected schema
            expected_headers = [
                "Timestamp", "Vendor Name", "Invoice Number", "Invoice Date",
                "Total Amount", "Currency", "Line Items Count", "Account Holder",
                "Bank Address", "Account Number/IBAN", "Discord Thread URL",
                "Status", "Approver", "Cost Center", "Rejection Reason",
                "Payment Status", "Transaction ID", "Paid Amount (ETH)", "Created At", "Updated At"
            ]
            try:
                current_headers = self.worksheet.row_values(1)
                if current_headers != expected_headers:
                    self.worksheet.update("A1:T1", [expected_headers])
                    logger.info("Initialized/normalized header row in worksheet")
            except Exception as e:
                logger.warning(f"Failed to normalize header row: {e}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets: {e}")
            raise
    
    def init_database(self):
        """Initialize SQLite database for state persistence"""
        self.conn = sqlite3.connect('invoice_states.db', check_same_thread=False, timeout=30)
        cursor = self.conn.cursor()
        try:
            cursor.execute('PRAGMA journal_mode=WAL;')
        except Exception:
            pass
        try:
            cursor.execute('PRAGMA busy_timeout=5000;')
        except Exception:
            pass
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoice_states (
                invoice_number TEXT PRIMARY KEY,
                state_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Deterministic mapping: Discord thread -> invoice number
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS thread_map (
                thread_id TEXT PRIMARY KEY,
                invoice_number TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
    
    def save_state(self, invoice_number: str, state: InvoiceState):
        """Save invoice state to database"""
        retries = 5
        delay = 0.1
        for attempt in range(retries):
            try:
                cursor = self.conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO invoice_states (invoice_number, state_json, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (invoice_number, json.dumps(state, default=str)))
                self.conn.commit()
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() or "busy" in str(e).lower():
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
    
    def load_state(self, invoice_number: str) -> Optional[InvoiceState]:
        """Load invoice state from database"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT state_json FROM invoice_states WHERE invoice_number = ?', (invoice_number,))
        result = cursor.fetchone()
        if result:
            return json.loads(result[0])
        return None
    
    def create_workflow(self) -> StateGraph:
        """Create the LangGraph workflow"""
        workflow = StateGraph(InvoiceState)
        
        # Add nodes
        workflow.add_node("process_invoice", self.process_invoice_node)
        workflow.add_node("post_to_discord", self.post_to_discord_node)
        workflow.add_node("update_spreadsheet", self.update_spreadsheet_node)
        workflow.add_node("finalize_invoice", self.finalize_invoice_node)
        
        # Define edges
        workflow.add_edge(START, "process_invoice")
        workflow.add_edge("process_invoice", "post_to_discord")
        workflow.add_edge("post_to_discord", "update_spreadsheet")
        workflow.add_edge("update_spreadsheet", "finalize_invoice")
        workflow.add_edge("finalize_invoice", END)
        
        return workflow.compile()
    
    def process_invoice_node(self, state: InvoiceState) -> InvoiceState:
        """Process and validate incoming invoice data"""
        logger.info(f"Processing invoice: {state['invoice_data'].get('Invoice Number')}")
        
        # Validate invoice data
        required_fields = [
            "Timestamp", "Vendor Name", "Invoice Number", "Invoice Date",
            "Total Amount", "Currency", "Line Items Count", "Account Holder",
            "Bank Address", "Account Number/IBAN"
        ]
        
        missing_fields = [field for field in required_fields if field not in state['invoice_data']]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Initialize state
        new_state = state.copy()
        new_state.update({
            'approval_status': 'pending',
            'payment_status': 'pending',
            'reminder_count': 0,
            'last_reminder': None
        })
        
        return new_state
    
    async def post_to_discord_node(self, state: InvoiceState) -> InvoiceState:
        """Post invoice details to Discord for approval"""
        invoice_data = state['invoice_data']
        
        # Ensure Discord client is ready before accessing channels
        await self.discord_client.wait_until_ready()
        
        # Create embed with invoice details
        embed = discord.Embed(
            title="🧾 Invoice Approval Request",
            description=f"**Vendor:** {invoice_data['Vendor Name']}",
            color=0x3498db
        )
        
        embed.add_field(
            name="📋 Invoice Details",
            value=f"**Number:** {invoice_data['Invoice Number']}\n"
                  f"**Date:** {invoice_data['Invoice Date']}\n"
                  f"**Amount:** {invoice_data['Total Amount']} {invoice_data['Currency']}",
            inline=False
        )
        
        embed.add_field(
            name="🏦 Payment Details",
            value=f"**Account Holder:** {invoice_data['Account Holder']}\n"
                  f"**Bank:** {invoice_data['Bank Address']}\n"
                  f"**Account:** {invoice_data['Account Number/IBAN']}",
            inline=False
        )
        
        embed.add_field(
            name="📊 Additional Info",
            value=f"**Line Items:** {invoice_data['Line Items Count']}\n"
                  f"**Submitted:** {invoice_data['Timestamp']}",
            inline=False
        )
        
        embed.add_field(
            name="⚡ Action Required",
            value="Please respond with:\n"
                  "• `APPROVE <cost_center>` to approve\n"
                  "• `REJECT <reason>` to reject\n\n"
                  f"<@&{self.config.approving_team_role_id}>",
            inline=False
        )
        
        # Post to Discord (robust resolution with fetch_channel and type handling)
        channel = self.discord_client.get_channel(self.config.discord_channel_id)
        if channel is None:
            try:
                channel = await self.discord_client.fetch_channel(self.config.discord_channel_id)
            except Exception:
                channel = None

        if channel is None:
            # Fallback by name if configured
            if self.fallback_channel_name:
                # Try text channels across all guilds
                for guild in self.discord_client.guilds:
                    for ch in getattr(guild, "text_channels", []):
                        if ch.name.lower() == self.fallback_channel_name.lower():
                            channel = ch
                            break
                    if channel:
                        break
                # Try forum channels if still not found
                if channel is None:
                    for guild in self.discord_client.guilds:
                        for ch in getattr(guild, "channels", []):
                            try:
                                from discord import ForumChannel
                                if isinstance(ch, ForumChannel) and ch.name.lower() == self.fallback_channel_name.lower():
                                    channel = ch
                                    break
                            except Exception:
                                continue
                        if channel:
                            break
            if channel is None:
                raise ValueError(f"Cannot access Discord channel {self.config.discord_channel_id} and no fallback channel named '{self.fallback_channel_name}' was found")

        logger.info(f"Posting to Discord channel: id={getattr(channel, 'id', None)} type={type(channel)} name={getattr(channel, 'name', None)}")
        thread = None
        message = None

        try:
            # Text channel: send embed and create a thread off the message
            if isinstance(channel, discord.TextChannel):
                message = await channel.send(embed=embed)
                thread = await message.create_thread(
                    name=f"Invoice {invoice_data['Invoice Number']} - {invoice_data['Vendor Name']}"
                )
            # Existing thread: just post into it and reuse
            elif isinstance(channel, discord.Thread):
                message = await channel.send(embed=embed)
                thread = channel
            # Forum channel: create a forum thread with the embed as the first message
            elif hasattr(discord, "ForumChannel") and isinstance(channel, discord.ForumChannel):
                created = await channel.create_thread(
                    name=f"Invoice {invoice_data['Invoice Number']} - {invoice_data['Vendor Name']}",
                    content=None,
                    embed=embed
                )
                # Some versions return (thread, message)
                if isinstance(created, tuple) and len(created) >= 1:
                    thread = created[0]
                else:
                    thread = created
            else:
                # Fallback: send in the channel and treat the channel as the discussion context
                message = await channel.send(embed=embed)
                thread = message.channel
        except Exception as e:
            raise ValueError(f"Cannot access Discord channel {self.config.discord_channel_id} ({type(channel)}): {e}")
        
        new_state = state.copy()
        new_state['discord_thread_id'] = str(thread.id)
        # If no message object (e.g., forum thread created without initial message), fall back to thread id
        new_state['discord_message_id'] = str(message.id) if message is not None else str(thread.id)
        logger.info(f"Posted invoice to Discord: thread_id={new_state['discord_thread_id']} message_id={new_state['discord_message_id']}")
        # Post initial SLA countdown message
        updated_state = await self.post_or_update_sla_message(new_state)
        if updated_state is not None:
            new_state = updated_state
        
        # Save state
        self.save_state(invoice_data['Invoice Number'], new_state)
        # Persist thread -> invoice mapping for reliable event handling
        try:
            retries = 5
            delay = 0.1
            for attempt in range(retries):
                try:
                    c = self.conn.cursor()
                    c.execute(
                        'INSERT OR REPLACE INTO thread_map (thread_id, invoice_number, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                        (new_state['discord_thread_id'], invoice_data['Invoice Number'])
                    )
                    self.conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() or "busy" in str(e).lower():
                        time.sleep(delay)
                        delay *= 2
                        continue
                    raise
        except Exception as e:
            logger.warning(f"Failed to write thread_map: {e}")
        
        return new_state
    
    def update_spreadsheet_node(self, state: InvoiceState) -> InvoiceState:
        """Update Google Spreadsheet with invoice details"""
        invoice_data = state['invoice_data']
        
        # Create Discord thread URL (approximate - you'll need actual guild ID)
        thread_url = f"https://discord.com/channels/@me/{state['discord_thread_id']}"
        
        # Ensure header row is normalized to expected schema to prevent column drift
        expected_headers = [
            "Timestamp", "Vendor Name", "Invoice Number", "Invoice Date",
            "Total Amount", "Currency", "Line Items Count", "Account Holder",
            "Bank Address", "Account Number/IBAN", "Discord Thread URL",
            "Status", "Approver", "Cost Center", "Rejection Reason",
            "Payment Status", "Transaction ID", "Paid Amount (ETH)", "Created At", "Updated At"
        ]
        try:
            current_headers = self.worksheet.row_values(1)
            if current_headers != expected_headers:
                self.worksheet.update("A1:T1", [expected_headers])
                logger.info("Normalized header row in existing worksheet")
        except Exception as e:
            logger.warning(f"Header normalization failed: {e}")
        
        row_data = [
            invoice_data['Timestamp'],
            invoice_data['Vendor Name'],
            invoice_data['Invoice Number'],
            invoice_data['Invoice Date'],
            invoice_data['Total Amount'],
            invoice_data['Currency'],
            invoice_data['Line Items Count'],
            invoice_data['Account Holder'],
            invoice_data['Bank Address'],
            invoice_data['Account Number/IBAN'],
            thread_url,
            state['approval_status'],
            state.get('approver', ''),
            state.get('cost_center', ''),
            state.get('rejection_reason', ''),
            state['payment_status'],
            state.get('transaction_id', ''),
            state.get('paid_amount_eth', ''),
            datetime.now().isoformat(),
            datetime.now().isoformat()
        ]
        
        # Append with retries for robustness
        append_ok = False
        for attempt in range(1, 4):
            try:
                logger.info(f"Appending spreadsheet row (attempt {attempt})")
                self.worksheet.append_row(row_data, value_input_option="RAW")
                append_ok = True
                break
            except Exception as e:
                logger.warning(f"append_row failed on attempt {attempt}: {e}")
                time.sleep(min(1.5 * attempt, 5.0))
        if not append_ok:
            logger.error("append_row failed after retries in update_spreadsheet_node")
            raise
        
        # Get the row number for future updates
        all_values = self.worksheet.get_all_values()
        new_state = state.copy()
        new_state['spreadsheet_row'] = len(all_values)
        logger.info(f"Spreadsheet row appended at index {new_state['spreadsheet_row']}")
        
        self.save_state(invoice_data['Invoice Number'], new_state)
        return new_state
    
    def finalize_invoice_node(self, state: InvoiceState) -> InvoiceState:
        """Finalize the invoice process"""
        logger.info(f"Invoice {state['invoice_data']['Invoice Number']} process completed")
        return state
    
    def update_spreadsheet_row(self, state: InvoiceState):
        """Update specific row in spreadsheet"""
        if state.get('spreadsheet_row'):
            row = state['spreadsheet_row']
            
            # Determine the overall status
            if state['payment_status'] == 'completed' and state.get('transaction_id'):
                overall_status = 'completed'
            elif state['payment_status'] == 'failed':
                overall_status = 'failed'
            elif state['approval_status'] == 'approved':
                overall_status = 'approved'
            elif state['approval_status'] == 'rejected':
                overall_status = 'rejected'
            else:
                overall_status = state['approval_status']
            
            # Update specific columns
            updates = [
                (row, 12, overall_status),  # Status - now shows 'completed' when payment is done
                (row, 13, state.get('approver', '')),  # Approver
                (row, 14, state.get('cost_center', '')),  # Cost Center
                (row, 15, state.get('rejection_reason', '')),  # Rejection Reason
                (row, 16, state['payment_status']),  # Payment Status
                (row, 17, state.get('transaction_id', '')),  # Transaction ID
                (row, 18, state.get('paid_amount_eth', '')),  # Paid Amount (ETH)
                (row, 20, datetime.now().isoformat())  # Updated At
            ]
            
            for row_num, col_num, value in updates:
                self.worksheet.update_cell(row_num, col_num, value)
    
    def setup_discord_handlers(self):
        """Setup Discord event handlers"""
        
        @self.discord_client.event
        async def on_ready():
            logger.info(f'{self.discord_client.user} has connected to Discord!')
            # Start reminder task here to ensure a running event loop exists
            try:
                if not self.reminder_task.is_running():
                    self.reminder_task.start()
            except Exception as e:
                logger.warning(f"Failed to start reminder task on_ready: {e}")
        
        @self.discord_client.event
        async def on_message(message):
            if message.author.bot:
                return
            if getattr(self, "shutting_down", False) or self.discord_client.is_closed():
                return
            
            # Check if message is in a thread we're monitoring
            if isinstance(message.channel, discord.Thread):
                try:
                    await self.handle_thread_message(message)
                except Exception as e:
                    logger.error(f"Error in handle_thread_message: {e}")
    
    async def safe_send(self, channel, content: Optional[str] = None, embed: Optional[discord.Embed] = None):
        """Safely send a message, skipping if the client/event loop is shutting down."""
        try:
            if getattr(self, "shutting_down", False) or self.discord_client.is_closed():
                return None
            return await channel.send(content=content, embed=embed)
        except RuntimeError as e:
            # Common during shutdown: "cannot schedule new futures after shutdown"
            if "after shutdown" in str(e).lower():
                logger.info("Send skipped during shutdown")
                return None
            raise

    async def handle_thread_message(self, message: discord.Message):
        """Handle messages in monitored threads"""
        if getattr(self, "shutting_down", False) or self.discord_client.is_closed():
            return
        thread_id = str(message.channel.id)
        logger.debug(f"Handling message in thread {thread_id} by user {getattr(message.author, 'id', 'unknown')}")
        
        # Find invoice state for this thread using deterministic mapping first
        cursor = self.conn.cursor()
        invoice_number = None
        try:
            cursor.execute('SELECT invoice_number FROM thread_map WHERE thread_id = ?', (thread_id,))
            row = cursor.fetchone()
            if row:
                invoice_number = row[0]
        except Exception as e:
            logger.debug(f"thread_map lookup failed: {e}")
        
        state = None
        if invoice_number:
            loaded = self.load_state(invoice_number)
            if not loaded:
                return
            state = loaded
        else:
            # Fallback to LIKE scan (legacy)
            try:
                cursor.execute('''
                    SELECT invoice_number, state_json FROM invoice_states 
                    WHERE state_json LIKE ?
                ''', (f'%"discord_thread_id": "{thread_id}"%',))
                result = cursor.fetchone()
                if not result:
                    logger.debug(f"No state found for thread {thread_id}")
                    return
                invoice_number, state_json = result
                state = json.loads(state_json)
            except Exception as e:
                logger.debug(f"Fallback state lookup failed for thread {thread_id}: {e}")
                return
        
        # Check if user has approving role (robust: fetch member if roles not cached)
        has_role = False
        try:
            roles = getattr(message.author, "roles", []) or []
            has_role = any(getattr(role, "id", None) == self.config.approving_team_role_id for role in roles)
            if not has_role and getattr(message, "guild", None) is not None:
                try:
                    full_member = await message.guild.fetch_member(message.author.id)
                    roles = getattr(full_member, "roles", []) or []
                    has_role = any(getattr(role, "id", None) == self.config.approving_team_role_id for role in roles)
                except Exception:
                    pass
        except Exception:
            has_role = False
        if not has_role:
            logger.debug(f"Ignoring message in thread {thread_id} from user {getattr(message.author, 'id', 'unknown')} without required role {self.config.approving_team_role_id}")
            return
        
        raw_content = message.content.strip()
        parsed = await self.parse_message_with_llm(raw_content, state)
        intent = (parsed.get('intent') or 'none').lower().strip()

        if intent == 'status':
            await self.show_invoice_status(message, state)
            return

        # If already decided, acknowledge attempts to re-approve/reject
        if state['approval_status'] != 'pending' and intent in ('approve', 'reject'):
            await self.safe_send(
                message.channel,
                content=f"ℹ️ Invoice is already {state['approval_status'].upper()} by {state.get('approver','unknown')}."
            )
            return

        if state['approval_status'] == 'pending':
            if intent == 'approve':
                if parsed.get('cost_center'):
                    message.content = f"APPROVE {parsed['cost_center']}"
                else:
                    message.content = "APPROVE"
                await self.process_approval_message(message, state, 'approved')
                return
            if intent == 'reject':
                reason_text = parsed.get('reason') or ''
                message.content = f"REJECT {reason_text}".strip()
                await self.process_approval_message(message, state, 'rejected')
                return

        # Handle payment confirmation regardless of approval status
        if state['payment_status'] == 'pending':
            content_upper = raw_content.upper()
            looks_like_payment = (
                intent == 'payment'
                or any(k in content_upper for k in ['TX', 'TRANSACTION', 'REF', 'PAYMENT', 'COMPLETED'])
                or bool(re.search(r'0x[a-fA-F0-9]{64}', raw_content, re.IGNORECASE))
            )
            if looks_like_payment:
                if state.get('transaction_id') and state['payment_status'] == 'completed':
                    await self.safe_send(message.channel,
                        content="ℹ️ **Invoice already completed!**\n"
                                f"Transaction ID: **{state['transaction_id']}**\n"
                                f"Status: **COMPLETED**"
                    )
                    return
                transaction_id = parsed.get('transaction_id')
                if not transaction_id:
                    tx_patterns = [
                        r'(0x[a-fA-F0-9]{64})',
                        r'TX[ID]?[:\s]+([A-Z0-9]{6,})',
                        r'TRANSACTION[ID]?[:\s]+([A-Z0-9]{6,})',
                        r'REF[ERENCE]?:[:\s]+([A-Z0-9]{6,})',
                        r'PAYMENT[:\s]+([A-Z0-9]{6,})',
                        r'COMPLETED[:\s]+([A-Z0-9]{6,})',
                        r'([A-Z0-9]{8,64})'
                    ]
                    for pattern in tx_patterns:
                        match = re.search(pattern, raw_content, re.IGNORECASE)
                        if match:
                            transaction_id = match.group(1)
                            break
                if transaction_id:
                    # If transaction_id looks like an Ethereum tx hash, verify on Etherscan (Sepolia)
                    if re.fullmatch(r"0x[a-fA-F0-9]{64}", transaction_id, re.IGNORECASE):
                        loop = asyncio.get_running_loop()
                        tx_norm = transaction_id.lower()
                        success = await loop.run_in_executor(None, lambda: check_transaction_success(tx_norm))
                        amount_eth = await loop.run_in_executor(None, lambda: get_transaction_amount_eth(tx_norm))
                        state['transaction_id'] = tx_norm
                        state['paid_amount_eth'] = amount_eth
                        state['payment_status'] = 'completed' if success else 'failed'
                        # Persist and update Google Sheets
                        self.save_state(invoice_number, state)
                        self.update_spreadsheet_row(state)
                        status_text = "successful" if success else "failed"
                        color = 0x27ae60 if success else 0xe74c3c
                        icon = "✅" if success else "❌"
                        embed = discord.Embed(
                            title=f"{icon} Payment {status_text.capitalize()}",
                            description=f"Transaction ID **{transaction_id}** for invoice **{invoice_number}**",
                            color=color
                        )
                        embed.add_field(name="Invoice Number", value=invoice_number, inline=True)
                        embed.add_field(name="Transaction ID", value=transaction_id, inline=False)
                        embed.add_field(name="Amount (ETH)", value=str(amount_eth), inline=True)
                        embed.add_field(name="Status", value=status_text.upper(), inline=True)
                        embed.set_footer(text=f"Updated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        await self.safe_send(message.channel, embed=embed)
                        return
                    # Fallback: treat as non-Ethereum reference and mark completed
                    state['transaction_id'] = transaction_id
                    state['payment_status'] = 'completed'
                    # Immediately persist and update Google Sheets
                    self.save_state(invoice_number, state)
                    self.update_spreadsheet_row(state)
                    logger.info(f"Transaction ID {transaction_id} recorded for invoice {invoice_number}")
                    embed = discord.Embed(
                        title="✅ Payment Confirmed",
                        description=f"Transaction ID **{transaction_id}** recorded for invoice **{invoice_number}**\n\nStatus updated to: **COMPLETED**",
                        color=0x27ae60
                    )
                    embed.add_field(name="Invoice Number", value=invoice_number, inline=True)
                    embed.add_field(name="Transaction ID", value=transaction_id, inline=True)
                    embed.add_field(name="Status", value="COMPLETED", inline=True)
                    embed.set_footer(text=f"Updated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    await self.safe_send(message.channel, embed=embed)
                    return
                if intent == 'payment':
                    await self.safe_send(message.channel,
                        content="💡 **To complete payment, please provide the transaction ID in one of these formats:**\n"
                                "• `TX: ABC123456789`\n"
                                "• `TRANSACTION: ABC123456789`\n"
                                "• `REF: ABC123456789`\n"
                                "• `PAYMENT: ABC123456789`\n"
                                "• `ABC123456789` (just the ID)"
                    )
    
    async def show_invoice_status(self, message: discord.Message, state: InvoiceState):
        """Show current status of the invoice"""
        invoice_number = state['invoice_data']['Invoice Number']
        
        # Determine overall status
        if state['payment_status'] == 'completed' and state.get('transaction_id'):
            overall_status = 'completed'
            status_color = 0x27ae60  # Green
        elif state['approval_status'] == 'approved':
            overall_status = 'approved'
            status_color = 0x3498db  # Blue
        elif state['approval_status'] == 'rejected':
            overall_status = 'rejected'
            status_color = 0xe74c3c  # Red
        else:
            overall_status = state['approval_status']
            status_color = 0xf39c12  # Orange
        
        embed = discord.Embed(
            title=f"📋 Invoice Status: {invoice_number}",
            color=status_color
        )
        
        embed.add_field(name="Overall Status", value=overall_status.upper(), inline=True)
        embed.add_field(name="Approval Status", value=state['approval_status'].upper(), inline=True)
        embed.add_field(name="Payment Status", value=state['payment_status'].upper(), inline=True)
        if state['approval_status'] == 'pending':
            time_remaining_text, _ = self._compute_time_remaining(state)
            embed.add_field(name="Time Remaining", value=time_remaining_text, inline=True)
        
        if state.get('approver'):
            embed.add_field(name="Approver", value=state['approver'], inline=True)
        if state.get('cost_center'):
            embed.add_field(name="Cost Center", value=state['cost_center'], inline=True)
        if state.get('transaction_id'):
            embed.add_field(name="Transaction ID", value=state['transaction_id'], inline=True)
        if state.get('rejection_reason'):
            embed.add_field(name="Rejection Reason", value=state['rejection_reason'], inline=False)
        
        embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        await self.safe_send(message.channel, embed=embed)
    
    async def process_approval_message(self, message: discord.Message, state: InvoiceState, decision: str):
        """Process approval or rejection message"""
        content = message.content.strip()
        invoice_number = state['invoice_data']['Invoice Number']
        
        state['approval_status'] = decision
        state['approver'] = f"{message.author.display_name} ({message.author.name})"
        
        if decision == 'approved':
            # Extract cost center
            parts = content.split(' ', 1)
            if len(parts) > 1:
                state['cost_center'] = parts[1].strip()
                
                # Send confirmation
                embed = discord.Embed(
                    title="✅ Invoice Approved",
                    description=f"Invoice {invoice_number} approved for cost center: {state['cost_center']}",
                    color=0x27ae60
                )
                await self.safe_send(message.channel, embed=embed)
            else:
                # Ask for cost center
                await self.safe_send(message.channel,
                    content="⚠️ Please provide the cost center:\n"
                            f"`APPROVE <cost_center>`"
                )
                return
        
        elif decision == 'rejected':
            # Extract rejection reason
            parts = content.split(' ', 1)
            if len(parts) > 1:
                state['rejection_reason'] = parts[1].strip()
            else:
                state['rejection_reason'] = 'No reason provided'
            
            # Send confirmation
            embed = discord.Embed(
                title="❌ Invoice Rejected",
                description=f"Invoice {invoice_number} rejected: {state['rejection_reason']}",
                color=0xe74c3c
            )
            await self.safe_send(message.channel, embed=embed)
        
        # Save state and update spreadsheet
        self.save_state(invoice_number, state)
        self.update_spreadsheet_row(state)
    
    @tasks.loop(minutes=2)
    async def reminder_task(self):
        """Background task to check for pending approvals needing reminders"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT invoice_number, state_json FROM invoice_states
                WHERE state_json LIKE '%"approval_status": "pending"%'
            ''')
            
            for invoice_number, state_json in cursor.fetchall():
                state = json.loads(state_json)
                # Update SLA message periodically
                updated_state = await self.post_or_update_sla_message(state)
                if updated_state is not None:
                    state = updated_state
                    self.save_state(invoice_number, state)
                
                # Check if reminder is due
                last_reminder = state.get('last_reminder')
                if last_reminder:
                    last_reminder = datetime.fromisoformat(last_reminder)
                    hours_since = (datetime.now() - last_reminder).total_seconds() / 3600
                    if hours_since >= self.config.reminder_interval_hours:
                        await self.send_reminder(state)
                        state['reminder_count'] += 1
                        state['last_reminder'] = datetime.now().isoformat()
                        self.save_state(invoice_number, state)
                elif state['reminder_count'] == 0:
                    # First reminder after 24 hours
                    created_time = datetime.fromisoformat(state['invoice_data']['Timestamp'])
                    hours_since = (datetime.now() - created_time).total_seconds() / 3600
                    if hours_since >= self.config.reminder_interval_hours:
                        await self.send_reminder(state)
                        state['reminder_count'] = 1
                        state['last_reminder'] = datetime.now().isoformat()
                        self.save_state(invoice_number, state)
                        
        except Exception as e:
            logger.error(f"Error in reminder task: {e}")
    
    async def send_reminder(self, state: InvoiceState):
        """Send reminder to Discord thread"""
        thread = self.discord_client.get_channel(int(state['discord_thread_id']))
        if thread:
            reminder_embed = discord.Embed(
                title="⏰ Approval Reminder",
                description=f"Invoice {state['invoice_data']['Invoice Number']} is still pending approval.",
                color=0xf39c12
            )
            time_remaining_text, _ = self._compute_time_remaining(state)
            reminder_embed.add_field(
                name="Time Elapsed",
                value=f"Reminder #{state['reminder_count'] + 1}",
                inline=True
            )
            reminder_embed.add_field(
                name="Time Remaining",
                value=time_remaining_text,
                inline=True
            )
            reminder_embed.add_field(
                name="Action Required",
                value=f"<@&{self.config.approving_team_role_id}> Please review and respond.",
                inline=False
            )
            
            await self.safe_send(thread, embed=reminder_embed)

    async def parse_message_with_llm(self, content: str, state: InvoiceState) -> Dict[str, Any]:
        """Parse freeform messages using the LLM into structured fields."""
        system_prompt = (
            "You are an assistant that extracts structured actions from approver messages in a Discord thread about an invoice.\n"
            "Decide the user's intent among: approve, reject, status, payment, none.\n"
            "- approve: The message approves the invoice, optionally specifying a cost center (examples: CC-123, 1001, COSTCENTER: 789).\n"
            "- reject: The message rejects and may include a reason.\n"
            "- status: The user asks for current status.\n"
            "- payment: The user provides a transaction/reference/payment ID. If an Ethereum transaction hash is present (format: 0x followed by 64 hex characters), set intent=payment and extract ONLY that hash as transaction_id.\n"
            "Rules:\n"
            "1) When an Ethereum tx hash appears anywhere in the message, transaction_id MUST be that 0x... string (lowercased).\n"
            "2) Ignore other text, numbers, or references when an Ethereum tx hash is present.\n"
            "3) If multiple hashes appear, choose the first.\n"
            "Output strict JSON with keys: intent, cost_center, reason, transaction_id. Use null when not present."
        )
        user_prompt = (
            f"Invoice number: {state['invoice_data'].get('Invoice Number')}\n"
            f"Message: {content}"
        )
        try:
            response = await self.llm.ainvoke(f"{system_prompt}\n\n{user_prompt}")
            text = response.content if hasattr(response, 'content') else str(response)
            json_match = re.search(r"\{[\s\S]*\}", text)
            if json_match:
                parsed = json.loads(json_match.group(0))
            else:
                parsed = json.loads(text)
            for key in ['cost_center', 'reason', 'transaction_id']:
                if parsed.get(key) in ('', 'none', 'null', None):
                    parsed[key] = None
            if not parsed.get('intent'):
                parsed['intent'] = 'none'
            return parsed
        except Exception as e:
            logger.warning(f"LLM parsing failed, falling back to heuristics: {e}")
            content_lower = content.lower()
            intent = 'none'
            if 'status' in content_lower:
                intent = 'status'
            elif 'approve' in content_lower or 'approved' in content_lower:
                intent = 'approve'
            elif 'reject' in content_lower or 'rejected' in content_lower:
                intent = 'reject'
            cc_match = re.search(r"(cc[-\s:]?\s*\w+|cost\s*center[:\s]*\w+|\b\d{3,6}\b)", content, re.IGNORECASE)
            cost_center = cc_match.group(0) if cc_match else None
            reason_match = re.search(r"(?:because|due to|reason[:\s])\s+(.+)$", content, re.IGNORECASE)
            reason = reason_match.group(1).strip() if reason_match else None
            return {
                'intent': intent,
                'cost_center': cost_center,
                'reason': reason,
                'transaction_id': None
            }

    def _compute_time_remaining(self, state: InvoiceState) -> Tuple[str, float]:
        """Return (human_text, seconds_remaining) for approval SLA."""
        created_time = datetime.fromisoformat(state['invoice_data']['Timestamp'])
        sla_hours = state.get('approval_sla_hours') or self.config.approval_sla_hours
        deadline = created_time + timedelta(hours=sla_hours)
        seconds_remaining = max(0.0, (deadline - datetime.now()).total_seconds())
        hours = int(seconds_remaining // 3600)
        minutes = int((seconds_remaining % 3600) // 60)
        time_text = f"{hours}h {minutes}m" if seconds_remaining > 0 else "Expired"
        return time_text, seconds_remaining

    async def post_or_update_sla_message(self, state: InvoiceState) -> Optional[InvoiceState]:
        """Post or update an SLA countdown message while awaiting approval."""
        if state['approval_status'] != 'pending':
            return None
        thread_id = state.get('discord_thread_id')
        if not thread_id:
            return None
        thread = self.discord_client.get_channel(int(thread_id))
        if not thread:
            return None
        time_text, _ = self._compute_time_remaining(state)
        embed = discord.Embed(
            title="🕒 Approval SLA",
            description=f"Time remaining to approve invoice {state['invoice_data']['Invoice Number']}:",
            color=0x95a5a6
        )
        embed.add_field(name="Remaining", value=time_text, inline=True)
        embed.add_field(name="SLA (hours)", value=str(state.get('approval_sla_hours', self.config.approval_sla_hours)), inline=True)
        embed.set_footer(text=f"Updated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        message_id = state.get('sla_message_id')
        try:
            if message_id:
                try:
                    msg = await thread.fetch_message(int(message_id))
                    await msg.edit(embed=embed)
                except Exception:
                    msg = await self.safe_send(thread, embed=embed)
                    state['sla_message_id'] = str(msg.id)
            else:
                msg = await self.safe_send(thread, embed=embed)
                state['sla_message_id'] = str(msg.id)
        except Exception as e:
            logger.warning(f"Failed to post/update SLA message: {e}")
            return None
        return state
    
    async def process_invoice(self, invoice_data: Dict[str, Any]) -> str:
        """Main entry point to process a new invoice"""
        initial_state: InvoiceState = {
            'invoice_data': invoice_data,
            'discord_thread_id': None,
            'discord_message_id': None,
            'approval_status': 'pending',
            'cost_center': None,
            'approver': None,
            'rejection_reason': None,
            'payment_status': 'pending',
            'transaction_id': None,
            'paid_amount_eth': None,
            'reminder_count': 0,
            'last_reminder': None,
            'spreadsheet_row': None,
            'sla_message_id': None,
            'approval_sla_hours': self.config.approval_sla_hours
        }
        
        invoice_number = invoice_data['Invoice Number']
        
        try:
            result = await self.workflow.ainvoke(initial_state)
            
            logger.info(f"Invoice {invoice_number} processing initiated successfully")
            return f"Invoice {invoice_number} submitted for approval"
            
        except Exception as e:
            logger.error(f"Error processing invoice {invoice_number}: {e}")
            raise
    
    async def start(self):
        """Start the agent"""
        await self.discord_client.start(self.config.discord_token)
    
    async def stop(self):
        """Stop the agent"""
        self.shutting_down = True
        self.reminder_task.cancel()
        await self.discord_client.close()
        self.conn.close()

# Usage Example
async def main():
    # Configuration
    config = Config(
        discord_token=os.getenv("DISCORD_TOKEN"),
        discord_channel_id=int(os.getenv("DISCORD_CHANNEL_ID")),
        approving_team_role_id=int(os.getenv("APPROVING_TEAM_ROLE_ID")),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        google_sheets_credentials_file=os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE"),
        google_drive_credentials_file=os.getenv("GOOGLE_DRIVE_CREDENTIALS_FILE"),
        spreadsheet_id=os.getenv("SPREADSHEET_ID"),
        worksheet_name=os.getenv("WORKSHEET_NAME", "Invoice"),
        reminder_interval_hours=24,
        max_reminders=5
    )
    
    # Initialize agent
    agent = InvoiceApprovalAgent(config)
    
    # Example invoice data
    sample_invoice = {
        "Timestamp": datetime.now().isoformat(),
        "Vendor Name": "Acme Corp",
        "Invoice Number": "INV-2025-001",
        "Invoice Date": "2025-08-05",
        "Total Amount": "1500.00",
        "Currency": "USD",
        "Line Items Count": "3",
        "Account Holder": "Acme Corporation",
        "Bank Address": "123 Main St, New York, NY 10001",
        "Account Number/IBAN": "US12345678901234567890"
    }
    
    try:
        # Start the Discord client in the background
        discord_task = asyncio.create_task(agent.start())
        
        # Wait a moment for Discord to connect
        await asyncio.sleep(3)
        
        # Process an invoice
        result = await agent.process_invoice(sample_invoice)
        print(result)
        
        # Keep the agent running
        await discord_task
        
    except KeyboardInterrupt:
        print("Stopping agent...")
        await agent.stop()
    except Exception as e:
        print(f"Error: {e}")
        await agent.stop()

if __name__ == "__main__":
    asyncio.run(main())
