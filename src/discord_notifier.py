import os
import requests
from datetime import datetime
from typing import Dict, Optional

DISCORD_API_BASE = "https://discord.com/api/v10"

class DiscordNotifier:
    def __init__(self,
                 bot_token: Optional[str] = None,
                 channel_id: Optional[str] = None,
                 approving_role_id: Optional[str] = None):
        self.bot_token = bot_token or os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
        self.channel_id = channel_id or os.getenv("DISCORD_CHANNEL_ID")
        self.approving_role_id = approving_role_id or os.getenv("APPROVING_TEAM_ROLE_ID")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bot {self.bot_token}", "Content-Type": "application/json"}

    def is_configured(self) -> bool:
        return bool(self.bot_token and self.channel_id and self.approving_role_id)

    def _get_channel(self, channel_id: str) -> Optional[Dict]:
        try:
            url = f"{DISCORD_API_BASE}/channels/{channel_id}"
            resp = requests.get(url, headers=self._headers())
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def _resolve_guild_id(self, thread_or_channel_id: str) -> Optional[str]:
        ch = self._get_channel(thread_or_channel_id)
        if not ch:
            return None
        return str(ch.get("guild_id")) if ch.get("guild_id") else None

    def post_invoice_and_create_thread(self, invoice: Dict) -> Optional[str]:
        if not self.is_configured():
            return None

        try:
            vendor = invoice.get('vendor_name', '')
            inv_no = invoice.get('invoice_number', '')
            inv_date = invoice.get('invoice_date', '')
            amount = invoice.get('total_amount', '')
            currency = invoice.get('currency', '')
            line_items = invoice.get('line_items') or []
            submitted = invoice.get('submitted_at') or invoice.get('timestamp') or ''
            payment = invoice.get('payment_details') or {}

            role_mention = f"<@&{self.approving_role_id}>"

            embed = {
                "title": "🧾 Invoice Approval Request",
                "description": f"Vendor: {vendor}",
                "color": 3447003,
                "fields": [
                    {
                        "name": "📋 Invoice Details",
                        "value": f"**Number:** {inv_no}\n**Date:** {inv_date}\n**Amount:** {amount} {currency}",
                        "inline": False
                    },
                    {
                        "name": "🏦 Payment Details",
                        "value": f"**Account Holder:** {payment.get('account_holder','')}\n"
                                 f"**Bank:** {payment.get('bank_address','')}\n"
                                 f"**Account:** {payment.get('account_number','')}",
                        "inline": False
                    },
                    {
                        "name": "📊 Additional Info",
                        "value": f"**Line Items:** {len(line_items)}\n**Submitted:** {submitted}",
                        "inline": False
                    },
                    {
                        "name": "⚡ Action Required",
                        "value": "Please respond with:\n• `APPROVE <cost_center>` to approve\n• `REJECT <reason>` to reject\n\n"
                                 f"{role_mention}",
                        "inline": False
                    }
                ]
            }

            # Create the message with embed
            url = f"{DISCORD_API_BASE}/channels/{self.channel_id}/messages"
            resp = requests.post(url, headers=self._headers(), json={"embeds": [embed]})
            resp.raise_for_status()
            message = resp.json()
            message_id = message["id"]

            # Create a thread from the message
            thread_name = f"Invoice {inv_no} - {vendor}".strip()
            url = f"{DISCORD_API_BASE}/channels/{self.channel_id}/messages/{message_id}/threads"
            resp = requests.post(url, headers=self._headers(), json={"name": thread_name})
            resp.raise_for_status()
            thread = resp.json()
            thread_id = thread["id"]

            guild_id = self._resolve_guild_id(thread_id) or self._resolve_guild_id(self.channel_id)
            if not guild_id:
                return None

            thread_url = f"https://discord.com/channels/{guild_id}/{thread_id}"
            return thread_url

        except Exception:
            return None
