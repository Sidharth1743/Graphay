import os
import re
import uuid
import base64
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

class GmailTool:
    def __init__(self):
        self.service = self._get_gmail_service()
        
    def fetch_unanswered_emails(self, max_results=50):
        """
        Fetches all emails included in unanswered threads.

        @param max_results: Maximum number of recent emails to fetch
        @return: List of dictionaries, each representing a thread with its emails
        """
        try:
            recent_emails = self.fetch_recent_emails(max_results)
            if not recent_emails:
                return []

            drafts = self.fetch_draft_replies()
            threads_with_drafts = {draft['threadId'] for draft in drafts}

            seen_threads = set()
            unanswered_emails = []
            for email in recent_emails:
                thread_id = email['threadId']
                if thread_id not in seen_threads and thread_id not in threads_with_drafts:
                    seen_threads.add(thread_id)
                    email_info = self._get_email_info(email['id'])
                    if self._should_skip_email(email_info):
                        continue
                    unanswered_emails.append(email_info)
            return unanswered_emails

        except Exception as e:
            print(f"An error occurred: {e}")
            return []

    def fetch_recent_emails(self, max_results=50):
        try:
            now = datetime.now()
            delay = now - timedelta(hours=8)

            after_timestamp = int(delay.timestamp())
            before_timestamp = int(now.timestamp())

            query = f"after:{after_timestamp} before:{before_timestamp}"
            results = self.service.users().messages().list(
                userId="me", q=query, maxResults=max_results
            ).execute()
            messages = results.get("messages", [])
            
            return messages
        
        except Exception as error:
            print(f"An error occurred while fetching emails: {error}")
            return []

    def fetch_unread_emails(self, max_results=10):
        """
        Fetch top N unread emails (newest first) and return expanded email info.
        """
        try:
            # Gmail query: unread only
            query = "is:unread"
            results = self.service.users().messages().list(
                userId="me", q=query, maxResults=max_results
            ).execute()
            messages = results.get("messages", [])

            emails = []
            for msg in messages:
                email_info = self._get_email_info(msg["id"])
                if self._should_skip_email(email_info):
                    continue
                emails.append(email_info)
            return emails

        except Exception as error:
            print(f"An error occurred while fetching unread emails: {error}")
            return []

    def fetch_draft_replies(self):
        """
        Fetches all draft email replies from Gmail.
        """
        try:
            drafts = self.service.users().drafts().list(userId="me").execute()
            draft_list = drafts.get("drafts", [])
            return [
                {
                    "draft_id": draft["id"],
                    "threadId": draft["message"]["threadId"],
                    "id": draft["message"]["id"],
                }
                for draft in draft_list
            ]

        except Exception as error:
            print(f"An error occurred while fetching drafts: {error}")
            return []

    def create_draft_reply(self, initial_email, reply_text):
        try:
            message = self._create_reply_message(initial_email, reply_text)

            draft = self.service.users().drafts().create(
                userId="me", body={"message": message}
            ).execute()

            return draft
        except Exception as error:
            print(f"An error occurred while creating draft: {error}")
            return None

    def send_reply(self, initial_email, reply_text):
        try:
            message = self._create_reply_message(initial_email, reply_text, send=True)

            sent_message = self.service.users().messages().send(
                userId="me", body=message
            ).execute()
            
            return sent_message

        except Exception as error:
            print(f"An error occurred while sending reply: {error}")
            return None

    def mark_as_read(self, message_id: str) -> bool:
        """
        Mark a Gmail message as read by removing the UNREAD label.
        """
        try:
            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"removeLabelIds": ["UNREAD"]}
            ).execute()
            return True
        except Exception as error:
            print(f"An error occurred while marking message as read: {error}")
            return False
        
    def _create_reply_message(self, email, reply_text, send=False):
        message = self._create_html_email_message(
            recipient=email.sender,
            subject=email.subject,
            reply_text=reply_text
        )

        if email.messageId:
            message["In-Reply-To"] = email.messageId
            message["References"] = f"{email.references} {email.messageId}".strip()
            
            if send:
                message["Message-ID"] = f"<{uuid.uuid4()}@gmail.com>"
                
        body = {
            "raw": base64.urlsafe_b64encode(message.as_bytes()).decode(),
            "threadId": email.threadId
        }

        return body

    def _get_gmail_service(self):
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        
        return build('gmail', 'v1', credentials=creds)

    def _should_skip_email(self, email_info):
        my_email = os.getenv('MY_EMAIL', '')
        sender = email_info.get('sender') if isinstance(email_info, dict) else ''
        return bool(my_email) and (my_email in sender)

    def _get_email_info(self, msg_id):
        message = self.service.users().messages().get(
            userId="me", id=msg_id, format="full"
        ).execute()

        payload = message.get('payload', {})
        headers = {header["name"].lower(): header["value"] for header in payload.get("headers", [])}

        # Extract attachments
        attachments = self._extract_attachments(payload, msg_id)

        return {
            "id": msg_id,
            "threadId": message.get("threadId"),
            "messageId": headers.get("message-id"),
            "references": headers.get("references", ""),
            "sender": headers.get("from", "Unknown"),
            "subject": headers.get("subject", "No Subject"),
            "body": self._get_email_body(payload),
            "attachments": attachments
        }

    def _get_email_body(self, payload):
        """
        Extract the email body, prioritizing text/plain over text/html.
        Handles multipart messages, avoids duplicating content, and strips HTML if necessary.
        """
        def decode_data(data):
            """Decode base64-encoded data."""
            return base64.urlsafe_b64decode(data).decode('utf-8').strip() if data else ""

        def extract_body(parts):
            """Recursively extract text content from parts."""
            for part in parts:
                mime_type = part.get('mimeType', '')
                data = part['body'].get('data', '')
                if mime_type == 'text/plain':
                    return decode_data(data)
                if mime_type == 'text/html':
                    html_content = decode_data(data)
                    return self._extract_main_content_from_html(html_content)
                if 'parts' in part:
                    result = extract_body(part['parts'])
                    if result:
                        return result
            return ""

        if 'parts' in payload:
            body = extract_body(payload['parts'])
        else:
            data = payload['body'].get('data', '')
            body = decode_data(data)
            if payload.get('mimeType') == 'text/html':
                body = self._extract_main_content_from_html(body)

        return self._clean_body_text(body)

    def _extract_main_content_from_html(self, html_content):
        """
        Extract main visible content from HTML.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup(['script', 'style', 'head', 'meta', 'title']):
            tag.decompose()
        return soup.get_text(separator='\n', strip=True)

    def _clean_body_text(self, text):
        """
        Clean up the email body text by removing extra spaces and newlines.
        """
        return re.sub(r'\s+', ' ', text.replace('\r', '').replace('\n', '')).strip()
    
    def _extract_attachments(self, payload, message_id=None):
        """
        Extract attachment information from email payload.
        """
        attachments = []
        
        def extract_from_parts(parts):
            for part in parts:
                # Check if this part is an attachment
                if part.get('filename'):
                    # This is an attachment, try to get the data
                    if part.get('body', {}).get('data'):
                        # Data is directly available
                        attachment = {
                            'filename': part['filename'],
                            'mimeType': part.get('mimeType', 'application/octet-stream'),
                            'data': part['body']['data'],
                            'size': part['body'].get('size', 0)
                        }
                        attachments.append(attachment)
                    elif part.get('body', {}).get('attachmentId') and message_id:
                        # Data needs to be fetched separately using attachmentId
                        try:
                            attachment_data = self.service.users().messages().attachments().get(
                                userId='me',
                                messageId=message_id,
                                id=part['body']['attachmentId']
                            ).execute()
                            
                            if attachment_data.get('data'):
                                attachment = {
                                    'filename': part['filename'],
                                    'mimeType': part.get('mimeType', 'application/octet-stream'),
                                    'data': attachment_data['data'],
                                    'size': part['body'].get('size', 0)
                                }
                                attachments.append(attachment)
                        except Exception as e:
                            print(f"Error fetching attachment data: {e}")
                
                # Recursively check nested parts
                if 'parts' in part:
                    extract_from_parts(part['parts'])
        
        if 'parts' in payload:
            extract_from_parts(payload['parts'])
        elif payload.get('filename'):
            # Single attachment case
            if payload.get('body', {}).get('data'):
                attachment = {
                    'filename': payload['filename'],
                    'mimeType': payload.get('mimeType', 'application/octet-stream'),
                    'data': payload['body']['data'],
                    'size': payload['body'].get('size', 0)
                }
                attachments.append(attachment)
            elif payload.get('body', {}).get('attachmentId') and message_id:
                try:
                    attachment_data = self.service.users().messages().attachments().get(
                        userId='me',
                        messageId=message_id,
                        id=payload['body']['attachmentId']
                    ).execute()
                    
                    if attachment_data.get('data'):
                        attachment = {
                            'filename': payload['filename'],
                            'mimeType': payload.get('mimeType', 'application/octet-stream'),
                            'data': attachment_data['data'],
                            'size': payload['body'].get('size', 0)
                        }
                        attachments.append(attachment)
                except Exception as e:
                    print(f"Error fetching attachment data: {e}")
        
        return attachments
    
    def _create_html_email_message(self, recipient, subject, reply_text):
        """
        Creates a simple HTML email message with proper formatting and plaintext fallback.
        """
        message = MIMEMultipart("alternative")
        message["to"] = recipient
        message["subject"] = f"Re: {subject}" if not subject.startswith("Re: ") else subject

        html_text = reply_text.replace("\n", "<br>").replace("\\n", "<br>")
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body>{html_text}</body>
        </html>
        """

        html_part = MIMEText(html_content, "html")
        message.attach(html_part)

        return message

    def get_thread_messages(self, thread_id: str):
        """
        Fetch all messages in a Gmail thread (lightweight metadata), used to detect replies.
        Returns a list of dicts: id, threadId, from, to, subject, date, internalDate (epoch ms).
        """
        try:
            thread = self.service.users().threads().get(userId="me", id=thread_id, format="full").execute()
            msgs = []
            for msg in thread.get("messages", []):
                payload = msg.get("payload", {})
                headers = {h["name"].lower(): h["value"] for h in payload.get("headers", [])}
                try:
                    internal_date = int(msg.get("internalDate")) if msg.get("internalDate") else None
                except Exception:
                    internal_date = None
                msgs.append({
                    "id": msg.get("id"),
                    "threadId": msg.get("threadId"),
                    "from": headers.get("from", ""),
                    "to": headers.get("to", ""),
                    "subject": headers.get("subject", ""),
                    "date": headers.get("date", ""),
                    "internalDate": internal_date,
                })
            return msgs
        except Exception as e:
            print(f"An error occurred while fetching thread messages: {e}")
            return []

    def search_messages(self, query: str, max_results: int = 10):
        """
        Search Gmail messages using the Gmail query syntax.
        Returns a list of dicts: {"id": message_id, "threadId": threadId}
        Example queries:
          - filename:"Invoice-Miss.pdf"
          - subject:"Invoice" newer_than:7d
        """
        try:
            results = self.service.users().messages().list(
                userId="me", q=query, maxResults=max_results
            ).execute()
            return results.get("messages", []) or []
        except Exception as e:
            print(f"An error occurred while searching messages: {e}")
            return []

    def get_email_info_by_id(self, msg_id: str):
        """
        Public wrapper to fetch expanded email info (including headers/body/attachments)
        for a specific Gmail message ID. Reuses internal extraction logic.
        """
        try:
            return self._get_email_info(msg_id)
        except Exception as e:
            print(f"An error occurred while getting email info by id: {e}")
            return None
