from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field
from typing import TypedDict, List, Annotated, Dict, Optional

class Email(BaseModel):
    id:str
    threadId :str
    messageId :str
    references : str
    sender :str
    subject: str
    body : str
    attachments: List[Dict] = Field(default_factory=list)

class DiscordState(TypedDict, total=False):
    thread_id: Optional[str]
    message_id: Optional[str]
    approval_status: str
    cost_center: Optional[str]
    approver: Optional[str]
    rejection_reason: Optional[str]
    payment_status: str
    transaction_id: Optional[str]
    reminder_count: int
    last_reminder: Optional[str]
    sla_message_id: Optional[str]
    approval_sla_hours: int
    created_at: Optional[str]

class BaseGraph(TypedDict):
    emails : List[Email]
    current_email : Email
    email_category: str
    generated_email : str
    rag_queries: List[str]
    retrieved_documents: str
    writer_messages : Annotated[List, add_messages]
    sendable: bool
    trials : int
    invoice_emails: List[Dict]
    processed_attachments: List[Dict]
    gdrive_upload_result: Dict
    invoice_processing_result: Dict
    discord_state: DiscordState
