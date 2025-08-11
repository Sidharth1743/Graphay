import os
import time
import requests
from typing import Dict, Any

# Etherscan Sepolia API key is expected in environment
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")


def _etherscan_get(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Internal helper to call Etherscan Sepolia API with simple retry/backoff.
    Preserves existing logic and response handling.
    """
    url = "https://api-sepolia.etherscan.io/api"
    params = {**params, "apikey": ETHERSCAN_API_KEY}
    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            # brief backoff to handle transient network/rate-limit
            time.sleep(0.5 * (attempt + 1))
    raise last_err if last_err else RuntimeError("Etherscan request failed")


def check_transaction_success(tx_hash: str) -> bool:
    """
    Return True if the transaction succeeded on Sepolia (status == '1'), else False.
    Logic preserved from original implementation.
    """
    try:
        data = _etherscan_get({
            "module": "transaction",
            "action": "gettxreceiptstatus",
            "txhash": tx_hash
        })
        return (data or {}).get("result", {}).get("status") == "1"
    except Exception:
        return False


def get_transaction_amount_eth(tx_hash: str) -> float:
    """
    Return the transaction value in ETH using proxy.eth_getTransactionByHash (wei -> ETH).
    Logic preserved from original implementation.
    """
    try:
        data = _etherscan_get({
            "module": "proxy",
            "action": "eth_getTransactionByHash",
            "txhash": tx_hash
        })
        value_hex = (data or {}).get("result", {}).get("value", "0x0")
        return int(value_hex, 16) / 10**18
    except Exception:
        return 0.0
