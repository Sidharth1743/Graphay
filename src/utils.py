from colorama import Fore, Style
from datetime import datetime

def log_state(state, message, color=Fore.YELLOW):
    """Log state information with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{timestamp}] {message}")
    
    # Log relevant state information
    if state.get("invoice_processing_result"):
        print(f"{color}Invoice Processing Result: {state['invoice_processing_result']}")
    
    if state.get("discord_state"):
        print(f"{color}Discord State: ")
        print(f"  Thread ID: {state['discord_state'].get('thread_id')}")
        print(f"  Approval Status: {state['discord_state'].get('approval_status')}")
        print(f"  Payment Status: {state['discord_state'].get('payment_status')}")
    
    print(Style.RESET_ALL)
