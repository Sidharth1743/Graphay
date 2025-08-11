import sqlite3
import os
import json
from datetime import datetime
from typing import List, Dict, Optional
import re

class EmailDatabase:
    def __init__(self, db_path: str = "db/email_database.db"):
        """Initialize the email database."""
        self.db_path = db_path
        self._ensure_db_directory()
        self._create_tables()
    
    def _ensure_db_directory(self):
        """Ensure the database directory exists."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _create_tables(self):
        """Create the necessary database tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Emails table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS emails (
                    id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    message_id TEXT,
                    email_references TEXT,
                    sender TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body TEXT NOT NULL,
                    received_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_invoice_related BOOLEAN DEFAULT FALSE,
                    invoice_keywords TEXT,
                    has_attachments BOOLEAN DEFAULT FALSE,
                    processed BOOLEAN DEFAULT FALSE
                )
            ''')
            
            # Attachments table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS attachments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    original_filename TEXT,
                    mime_type TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size INTEGER,
                    category TEXT,
                    file_hash TEXT,
                    saved_date TEXT,
                    gdrive_file_id TEXT,
                    gdrive_folder_id TEXT,
                    downloaded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (email_id) REFERENCES emails (id)
                )
            ''')
            
            # Invoice metadata table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS invoice_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id TEXT NOT NULL,
                    invoice_number TEXT,
                    amount REAL,
                    currency TEXT,
                    due_date DATE,
                    vendor TEXT,
                    extracted_data TEXT,
                    FOREIGN KEY (email_id) REFERENCES emails (id)
                )
            ''')
            
            # Followups table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS invoice_followups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    gdrive_file_id TEXT NOT NULL,
                    missing_fields TEXT,
                    initial_notice_sent_at TIMESTAMP NOT NULL,
                    reminder_due_at TIMESTAMP NOT NULL,
                    reminder_sent BOOLEAN DEFAULT FALSE,
                    reminder_sent_at TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (email_id) REFERENCES emails (id)
                )
            ''')
            
            # Backfill/migrate missing columns on existing databases (safe idempotent ALTERs)
            try:
                cursor.execute("PRAGMA table_info(attachments)")
                att_cols = [row[1] for row in cursor.fetchall()]
                # Ensure columns used by code exist
                if "saved_date" not in att_cols:
                    cursor.execute("ALTER TABLE attachments ADD COLUMN saved_date TEXT")
                if "gdrive_file_id" not in att_cols:
                    cursor.execute("ALTER TABLE attachments ADD COLUMN gdrive_file_id TEXT")
                if "gdrive_folder_id" not in att_cols:
                    cursor.execute("ALTER TABLE attachments ADD COLUMN gdrive_folder_id TEXT")
            except Exception as mig_e:
                print(f"Warning: attachments table migration check failed: {mig_e}")
            
            conn.commit()
    
    def store_email(self, email_data: Dict) -> bool:
        """Store an email in the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if invoice-related
                is_invoice_related, keywords = self._detect_invoice_keywords(
                    email_data.get('subject', '') + ' ' + email_data.get('body', '')
                )
                
                cursor.execute('''
                    INSERT OR REPLACE INTO emails 
                    (id, thread_id, message_id, email_references, sender, subject, body, is_invoice_related, invoice_keywords)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    email_data['id'],
                    email_data['threadId'],
                    email_data.get('messageId', ''),
                    email_data.get('references', ''),
                    email_data['sender'],
                    email_data['subject'],
                    email_data['body'],
                    is_invoice_related,
                    json.dumps(keywords) if keywords else None
                ))
                
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing email: {e}")
            return False
    
    def _detect_invoice_keywords(self, text: str) -> tuple[bool, List[str]]:
        """Detect invoice-related keywords in text."""
        invoice_keywords = [
            'invoice', 'bill', 'payment', 'receipt', 'statement', 'charge',
            'amount due', 'balance', 'outstanding', 'overdue', 'payment due',
            'total amount', 'subtotal', 'tax', 'shipping', 'handling',
            'invoice number', 'bill number', 'account number', 'reference number',
            'due date', 'payment terms', 'net 30', 'net 60', 'net 90',
            'credit card', 'bank transfer', 'wire transfer', 'check',
            'vendor', 'supplier', 'merchant', 'service provider',
            'subscription', 'recurring', 'monthly', 'quarterly', 'annual',
            'pdf', 'attachment', 'document', 'file'
        ]
        
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in invoice_keywords:
            if keyword in text_lower:
                found_keywords.append(keyword)
        
        # Also check for common invoice patterns
        patterns = [
            r'\$[\d,]+\.?\d*',  # Dollar amounts
            r'invoice\s*#?\s*\d+',  # Invoice numbers
            r'bill\s*#?\s*\d+',  # Bill numbers
            r'due\s+date[:\s]*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # Due dates
            r'payment\s+due[:\s]*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # Payment due dates
        ]
        
        for pattern in patterns:
            if re.search(pattern, text_lower):
                found_keywords.append('pattern_match')
        
        return len(found_keywords) > 0, found_keywords
    
    def get_invoice_related_emails(self) -> List[Dict]:
        """Get all invoice-related emails."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM emails 
                    WHERE is_invoice_related = TRUE 
                    ORDER BY received_date DESC
                ''')
                
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving invoice emails: {e}")
            return []
    
    def store_attachment(self, email_id: str, attachment_data: Dict) -> bool:
        """Store attachment information in the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO attachments 
                    (email_id, filename, original_filename, mime_type, file_path, file_size, 
                     category, file_hash, saved_date, gdrive_file_id, gdrive_folder_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    email_id,
                    attachment_data['filename'],
                    attachment_data.get('original_filename', ''),
                    attachment_data['mime_type'],
                    attachment_data['file_path'],
                    attachment_data.get('file_size', 0),
                    attachment_data.get('category', ''),
                    attachment_data.get('file_hash', ''),
                    attachment_data.get('saved_date', ''),
                    attachment_data.get('gdrive_file_id', ''),
                    attachment_data.get('gdrive_folder_id', '')
                ))
                
                # Update email to indicate it has attachments
                cursor.execute('''
                    UPDATE emails 
                    SET has_attachments = TRUE 
                    WHERE id = ?
                ''', (email_id,))
                
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing attachment: {e}")
            return False
    
    def store_invoice_metadata(self, email_id: str, metadata: Dict) -> bool:
        """Store invoice metadata in the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO invoice_metadata 
                    (email_id, invoice_number, amount, currency, due_date, vendor, extracted_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    email_id,
                    metadata.get('invoice_number'),
                    metadata.get('amount'),
                    metadata.get('currency'),
                    metadata.get('due_date'),
                    metadata.get('vendor'),
                    json.dumps(metadata.get('extracted_data', {}))
                ))
                
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing invoice metadata: {e}")
            return False
    
    def mark_email_processed(self, email_id: str) -> bool:
        """Mark an email as processed."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE emails 
                    SET processed = TRUE 
                    WHERE id = ?
                ''', (email_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error marking email as processed: {e}")
            return False
    
    def get_unprocessed_invoice_emails(self) -> List[Dict]:
        """Get unprocessed invoice-related emails."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM emails 
                    WHERE is_invoice_related = TRUE AND processed = FALSE
                    ORDER BY received_date DESC
                ''')
                
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving unprocessed invoice emails: {e}")
            return []
    
    def get_email_by_gdrive_file_id(self, gdrive_file_id: str) -> Optional[Dict]:
        """Get the originating email for a given Google Drive file ID via attachments table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT e.*
                    FROM attachments a
                    JOIN emails e ON a.email_id = e.id
                    WHERE a.gdrive_file_id = ?
                    ORDER BY a.id DESC
                    LIMIT 1
                ''', (gdrive_file_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
        except Exception as e:
            print(f"Error retrieving email by gdrive_file_id: {e}")
            return None

    def get_email_by_id(self, email_id: str) -> Optional[Dict]:
        """Retrieve an email row by its ID."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM emails WHERE id = ? LIMIT 1', (email_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
        except Exception as e:
            print(f"Error retrieving email by id: {e}")
            return None

    def create_followup(self, email_id: str, thread_id: str, gdrive_file_id: str, missing_fields: Dict, initial_notice_sent_at: str, reminder_due_at: str) -> Optional[int]:
        """Create a follow-up record for an invoice with missing information."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO invoice_followups
                    (email_id, thread_id, gdrive_file_id, missing_fields, initial_notice_sent_at, reminder_due_at, reminder_sent, resolved)
                    VALUES (?, ?, ?, ?, ?, ?, FALSE, FALSE)
                ''', (
                    email_id,
                    thread_id,
                    gdrive_file_id,
                    json.dumps(missing_fields) if isinstance(missing_fields, (list, dict)) else str(missing_fields),
                    initial_notice_sent_at,
                    reminder_due_at
                ))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            print(f"Error creating followup: {e}")
            return None

    def get_due_followups(self, now_iso: str) -> List[Dict]:
        """Return follow-ups that are due for reminder and not yet resolved/sent."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, email_id, thread_id, gdrive_file_id, missing_fields, initial_notice_sent_at, reminder_due_at, reminder_sent, resolved
                    FROM invoice_followups
                    WHERE resolved = FALSE
                      AND reminder_sent = FALSE
                      AND reminder_due_at <= ?
                ''', (now_iso,))
                rows = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, r)) for r in rows]
        except Exception as e:
            print(f"Error retrieving due followups: {e}")
            return []

    def mark_followup_reminder_sent(self, followup_id: int, sent_at_iso: str) -> bool:
        """Mark a follow-up as reminder sent."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE invoice_followups
                    SET reminder_sent = TRUE,
                        reminder_sent_at = ?
                    WHERE id = ?
                ''', (sent_at_iso, followup_id))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error marking followup reminder sent: {e}")
            return False

    def mark_followup_resolved(self, followup_id: int) -> bool:
        """Mark a follow-up as resolved (a reply was detected)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE invoice_followups
                    SET resolved = TRUE
                    WHERE id = ?
                ''', (followup_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error marking followup resolved: {e}")
            return False

    def get_open_followup_by_file_id(self, gdrive_file_id: str) -> Optional[Dict]:
        """Return an unresolved follow-up for a given Google Drive file ID, if any."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, email_id, thread_id, gdrive_file_id, missing_fields, initial_notice_sent_at, reminder_due_at, reminder_sent, resolved
                    FROM invoice_followups
                    WHERE gdrive_file_id = ?
                      AND resolved = FALSE
                    ORDER BY id DESC
                    LIMIT 1
                ''', (gdrive_file_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
        except Exception as e:
            print(f"Error retrieving open followup by file id: {e}")
            return None
