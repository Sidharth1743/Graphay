#!/usr/bin/env python3
"""
Google Drive Uploader for Email Agent PDFs
"""

import os
import json
import base64
import io
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

class GDriveUploader:
    def __init__(self, credentials_file: str = "credentials.json", token_file: str = "token1.json"):
        """
        Initialize the Google Drive uploader.
        
        Args:
            credentials_file: Path to Google Drive API credentials JSON file
            token_file: Path to store authentication token
        """
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.service = None
        self._authenticate()
    
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
                    print(f"Warning: {self.credentials_file} not found. Google Drive upload will be disabled.")
                    return
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        
        self.service = build('drive', 'v3', credentials=creds)
        print("✓ Google Drive authentication successful")
    
    def create_gdrive_folder(self, folder_name: str, parent_folder_id: str = None) -> Optional[str]:
        """Create a folder in Google Drive and return its ID"""
        if not self.service:
            print("Google Drive service not available")
            return None
            
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id,name'
            ).execute()
            
            folder_id = folder.get('id')
            folder_name = folder.get('name')
            print(f"✓ Created Google Drive folder '{folder_name}' with ID: {folder_id}")
            return folder_id
            
        except Exception as e:
            print(f"✗ Error creating folder: {str(e)}")
            return None
    
    def find_existing_folder(self, folder_name: str, parent_folder_id: str = None) -> Optional[str]:
        """Find an existing folder in Google Drive"""
        if not self.service:
            return None
            
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            else:
                query += " and 'root' in parents"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name)'
            ).execute()
            
            files = results.get('files', [])
            if files:
                folder_id = files[0]['id']
                print(f"✓ Found existing folder '{folder_name}' with ID: {folder_id}")
                return folder_id
            
            return None
            
        except Exception as e:
            print(f"✗ Error finding folder: {str(e)}")
            return None
    
    def get_or_create_folder(self, folder_name: str, parent_folder_id: str = None) -> Optional[str]:
        """Get existing folder or create new one"""
        # First try to find existing folder
        folder_id = self.find_existing_folder(folder_name, parent_folder_id)
        
        if not folder_id:
            # Create new folder if not found
            folder_id = self.create_gdrive_folder(folder_name, parent_folder_id)
        
        return folder_id
    
    def upload_file(self, file_path: str, folder_id: str = None, custom_name: str = None) -> Dict:
        """Upload a file to Google Drive folder"""
        if not self.service:
            return {
                'success': False,
                'error': 'Google Drive service not available',
                'message': 'Google Drive authentication failed'
            }
            
        try:
            if not os.path.exists(file_path):
                return {
                    'success': False,
                    'error': 'File not found',
                    'message': f"File {file_path} does not exist"
                }
            
            # Use custom name if provided, otherwise use original filename
            file_name = custom_name or os.path.basename(file_path)
            
            # Check if file already exists in the folder
            existing_file_id = self._find_existing_file(file_name, folder_id)
            if existing_file_id:
                return {
                    'success': True,
                    'file_id': existing_file_id,
                    'file_name': file_name,
                    'message': f"File {file_name} already exists in Google Drive",
                    'already_exists': True
                }
            
            # File metadata
            file_metadata = {
                'name': file_name
            }
            
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            # Determine MIME type
            mime_type = self._get_mime_type(file_path)
            
            # Media upload
            media = MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=True
            )
            
            # Upload file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size'
            ).execute()
            
            file_size = file.get('size', 0)
            size_mb = int(file_size) / (1024 * 1024) if file_size else 0
            
            return {
                'success': True,
                'file_id': file.get('id'),
                'file_name': file.get('name'),
                'file_size_mb': size_mb,
                'message': f"Successfully uploaded {file_name} ({size_mb:.2f} MB)"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f"Failed to upload {os.path.basename(file_path)}: {str(e)}"
            }
    
    def _find_existing_file(self, file_name: str, folder_id: str = None) -> Optional[str]:
        """Find if a file already exists in the folder"""
        try:
            query = f"name='{file_name}' and trashed=false"
            if folder_id:
                query += f" and '{folder_id}' in parents"
            else:
                query += " and 'root' in parents"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name)'
            ).execute()
            
            files = results.get('files', [])
            if files:
                return files[0]['id']
            
            return None
            
        except Exception:
            return None
    
    def _get_mime_type(self, file_path: str) -> str:
        """Get MIME type based on file extension"""
        ext = Path(file_path).suffix.lower()
        mime_types = {
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xls': 'application/vnd.ms-excel',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.txt': 'text/plain',
            '.csv': 'text/csv'
        }
        return mime_types.get(ext, 'application/octet-stream')
    
    def upload_folder_contents(self, local_folder_path: str, gdrive_folder_name: str = None, 
                              parent_folder_id: str = None) -> Dict:
        """
        Upload all files from a local folder to Google Drive
        
        Args:
            local_folder_path: Path to local folder
            gdrive_folder_name: Name for Google Drive folder (defaults to local folder name)
            parent_folder_id: Parent Google Drive folder ID (optional)
        """
        if not self.service:
            return {
                'success': False,
                'error': 'Google Drive service not available',
                'uploaded_files': [],
                'errors': []
            }
        
        try:
            local_folder = Path(local_folder_path)
            if not local_folder.exists():
                return {
                    'success': False,
                    'error': f'Local folder {local_folder_path} does not exist',
                    'uploaded_files': [],
                    'errors': []
                }
            
            # Use local folder name if no custom name provided
            if not gdrive_folder_name:
                gdrive_folder_name = local_folder.name
            
            # Get or create Google Drive folder
            gdrive_folder_id = self.get_or_create_folder(gdrive_folder_name, parent_folder_id)
            if not gdrive_folder_id:
                return {
                    'success': False,
                    'error': 'Failed to create or find Google Drive folder',
                    'uploaded_files': [],
                    'errors': []
                }
            
            # Find all files in the local folder
            uploaded_files = []
            errors = []
            
            for file_path in local_folder.rglob("*"):
                if file_path.is_file():
                    print(f"Uploading {file_path.name}...")
                    result = self.upload_file(str(file_path), gdrive_folder_id)
                    
                    if result['success']:
                        uploaded_files.append(result)
                        print(f"✓ {result['message']}")
                    else:
                        errors.append(result['message'])
                        print(f"✗ {result['message']}")
            
            return {
                'success': True,
                'gdrive_folder_id': gdrive_folder_id,
                'gdrive_folder_name': gdrive_folder_name,
                'uploaded_files': uploaded_files,
                'errors': errors,
                'total_files': len(uploaded_files),
                'total_errors': len(errors)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'uploaded_files': [],
                'errors': [str(e)]
            }
    
    def upload_email_attachments(self, files_base_path: str = "files", 
                                gdrive_folder_name: str = "Email Attachments",
                                parent_folder_id: str = None) -> Dict:
        """
        Upload all email attachments from the files folder to Google Drive
        
        Args:
            files_base_path: Path to the files folder
            gdrive_folder_name: Name for the Google Drive folder
            parent_folder_id: Parent Google Drive folder ID (optional)
        """
        print(f"\n=== UPLOADING EMAIL ATTACHMENTS TO GOOGLE DRIVE ===")
        print(f"Local folder: {files_base_path}")
        print(f"Google Drive folder: {gdrive_folder_name}")
        print("-" * 60)
        
        result = self.upload_folder_contents(files_base_path, gdrive_folder_name, parent_folder_id)
        
        # Print summary
        print("\n" + "="*60)
        print("UPLOAD SUMMARY")
        print("="*60)
        
        if result['success']:
            print(f"✓ Successfully uploaded to Google Drive folder: {result['gdrive_folder_name']}")
            print(f"✓ Total files uploaded: {result['total_files']}")
            
            if result['uploaded_files']:
                print("\nUploaded files:")
                for file_result in result['uploaded_files']:
                    if file_result.get('already_exists'):
                        print(f"  ⚠ {file_result['file_name']} (already existed)")
                    else:
                        size_info = f" ({file_result.get('file_size_mb', 0):.2f} MB)" if file_result.get('file_size_mb') else ""
                        print(f"  ✓ {file_result['file_name']}{size_info}")
            
            if result['errors']:
                print(f"\nErrors ({result['total_errors']}):")
                for error in result['errors']:
                    print(f"  ✗ {error}")
        else:
            print(f"✗ Upload failed: {result['error']}")
            if result['errors']:
                print("\nErrors:")
                for error in result['errors']:
                    print(f"  ✗ {error}")
        
        return result 

    def upload_attachment_directly(self, attachment_data: Dict, email_id: str, 
                                  folder_id: str = None, custom_name: str = None) -> Dict:
        """
        Upload attachment data directly to Google Drive without saving to local filesystem
        
        Args:
            attachment_data: Dictionary containing attachment data with 'data' and 'filename' keys
            email_id: Email ID for reference
            folder_id: Google Drive folder ID (optional)
            custom_name: Custom filename (optional)
        """
        if not self.service:
            return {
                'success': False,
                'error': 'Google Drive service not available',
                'message': 'Google Drive authentication failed'
            }
            
        try:
            # Decode the attachment data
            if 'data' not in attachment_data:
                return {
                    'success': False,
                    'error': 'No data found in attachment',
                    'message': f"No data found in attachment for email {email_id}"
                }
            
            file_data = base64.urlsafe_b64decode(attachment_data['data'])
            
            # Get filename and MIME type
            filename = custom_name or attachment_data.get('filename', f'attachment_{email_id}')
            mime_type = attachment_data.get('mimeType', 'application/octet-stream')
            
            # Create a file-like object from the data
            file_stream = io.BytesIO(file_data)
            
            # File metadata
            file_metadata = {
                'name': filename
            }
            
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            # Create media upload object
            media = MediaIoBaseUpload(
                file_stream,
                mimetype=mime_type,
                resumable=True
            )
            
            # Upload file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size'
            ).execute()
            
            file_size = file.get('size', 0)
            size_mb = int(file_size) / (1024 * 1024) if file_size else 0
            
            return {
                'success': True,
                'file_id': file.get('id'),
                'file_name': file.get('name'),
                'file_size_mb': size_mb,
                'message': f"Successfully uploaded {filename} ({size_mb:.2f} MB) directly to Google Drive"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f"Failed to upload {filename}: {str(e)}"
            } 