from langgraph.graph import END, StateGraph
from .state import BaseGraph
from .nodes import Nodes
from colorama import Fore, Style

class Workflow():
    def __init__(self):
        # initiate graph state & nodes
        workflow = StateGraph(BaseGraph)
        nodes = Nodes()

        # define all graph nodes
        workflow.add_node("load_inbox_emails", nodes.load_new_emails)
        workflow.add_node("is_email_inbox_empty", nodes.is_email_inbox_empty)
        workflow.add_node("categorize_email", nodes.categorize_email)
        workflow.add_node("process_invoice_emails", nodes.process_invoice_emails)
        workflow.add_node("route_email_based_on_category", nodes.route_email_based_on_category)
        workflow.add_node("construct_rag_queries", nodes.construct_rag_queries)
        workflow.add_node("retrieve_from_rag", nodes.retrieve_from_rag)
        workflow.add_node("email_writer", nodes.write_draft_email)
        workflow.add_node("email_proofreader", nodes.verify_generated_email)
        workflow.add_node("send_email", nodes.create_draft_response)
        workflow.add_node("skip_unrelated_email", nodes.skip_unrelated_email)
        workflow.add_node("upload_to_gdrive", nodes.upload_to_gdrive)
        workflow.add_node("process_invoices_from_gdrive", nodes.process_invoices_from_gdrive)

        # load inbox emails
        workflow.set_entry_point("load_inbox_emails")

        # check if there are emails to process
        workflow.add_edge("load_inbox_emails", "is_email_inbox_empty")
        workflow.add_conditional_edges(
            "is_email_inbox_empty",
            nodes.check_new_emails,
            {
                "process": "categorize_email",
                "empty": END
            }
        )

        # check if email is invoice-related
        workflow.add_conditional_edges(
            "categorize_email",
            nodes.check_invoice_related,
            {
                "invoice_related": "process_invoice_emails",
                "not_invoice_related": "route_email_based_on_category"
            }
        )
        
        # route email based on category (for non-invoice emails)
        workflow.add_conditional_edges(
            "route_email_based_on_category",
            nodes.route_decision,
            {
                "product related": "construct_rag_queries",
                "not product related": "email_writer", # Feedback or Complaint
                "unrelated": "skip_unrelated_email"
            }
        )

        # pass constructed queries to RAG chain to retrieve information
        workflow.add_edge("construct_rag_queries", "retrieve_from_rag")
        # give information to writer agent to create draft email
        workflow.add_edge("retrieve_from_rag", "email_writer")
        # proofread the generated draft email
        workflow.add_edge("email_writer", "email_proofreader")
        # check if email is sendable or not, if not rewrite the email
        workflow.add_conditional_edges(
            "email_proofreader",
            nodes.must_rewrite,
            {
                "send": "send_email",
                "rewrite": "email_writer",
                "stop": "categorize_email"
            }
        )

        # check if all emails are processed after each email
        workflow.add_conditional_edges(
            "send_email",
            nodes.check_all_emails_processed,
            {
                "finished": "upload_to_gdrive",
                "continue": "is_email_inbox_empty"
            }
        )
        
        workflow.add_conditional_edges(
            "skip_unrelated_email",
            nodes.check_all_emails_processed,
            {
                "finished": "upload_to_gdrive",
                "continue": "is_email_inbox_empty"
            }
        )
        
        workflow.add_conditional_edges(
            "process_invoice_emails",
            nodes.check_all_emails_processed,
            {
                "finished": "upload_to_gdrive",
                "continue": "is_email_inbox_empty"
            }
        )
        
        # Add Discord nodes for approval flow
        workflow.add_node("create_discord_thread", nodes.create_discord_thread)
        workflow.add_node("check_discord_approval", nodes.check_discord_approval)
        workflow.add_node("check_payment_confirmation", nodes.check_payment_confirmation)

        # Process invoices and conditionally create Discord threads
        workflow.add_edge("upload_to_gdrive", "process_invoices_from_gdrive")
        
        # Only proceed to Discord flow if invoice processing completed
        workflow.add_conditional_edges(
            "process_invoices_from_gdrive",
            lambda state: state.get("invoice_processing_result", {}).get("status", "skipped"),
            {
                "completed": "create_discord_thread",
                "started": END,
                "skipped": END,
                "error": END,
                "no_files_found": END,
                "validation_failed": END
            }
        )
        
        # Add conditional edges for approval flow
        workflow.add_conditional_edges(
            "create_discord_thread",
            lambda state: state.get("discord_state", {}).get("approval_status", "pending"),
            {
                "approved": "check_payment_confirmation",
                "rejected": END,
                "pending": END
            }
        )
        
        # Loop back to check approval status
        workflow.add_conditional_edges(
            "check_discord_approval",
            lambda state: state.get("discord_state", {}).get("approval_status", "pending"),
            {
                "pending": END,
                "approved": "check_payment_confirmation",
                "rejected": END
            }
        )
        
        # Add conditional edges for payment confirmation
        workflow.add_conditional_edges(
            "check_payment_confirmation",
            lambda state: state.get("discord_state", {}).get("payment_status", "pending"),
            {
                "completed": END,
                "pending": END
            }
        )
        
        # Log graph configuration
        print(Fore.CYAN + "\nWorkflow Configuration:" + Style.RESET_ALL)
        print("- Invoice Processing → Discord Thread Creation")
        print("- Discord Thread → Approval Monitoring")
        print("- Approved → Payment Confirmation")
        print("- Payment Confirmed → End")
        
        # Compile
        self.app = workflow.compile()
