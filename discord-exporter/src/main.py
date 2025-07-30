import discord
import asyncio
import pandas as pd
from datetime import datetime, timezone
import os
import re
from typing import List, Dict, Optional
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import gspread
import pickle
from pathlib import Path

# Configuration
class Config:
    BOT_TOKEN_ENV = 'DISCORD_BOT_TOKEN'  # Must be set in environment
    GUILD_ID: Optional[int] = None       # Discord guild ID or None for all
    CHANNEL_IDS: List[int] = []          # Specific channel IDs or empty for all

    # Date range (UTC)
    START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
    END_DATE   = datetime(2025, 12, 31, tzinfo=timezone.utc)

    # Export settings
    OUTPUT_DIR = "discord_exports"
    INCLUDE_SYSTEM_MESSAGES = False
    MAX_CONTENT_PREVIEW = 100

    # Google OAuth2 client settings
    USE_GOOGLE_SHEETS = True
    OAUTH_CLIENT_SECRET_FILE = 'oauth_client_secret.json'
    OAUTH_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]

    # Performance settings
    CHANNEL_TIMEOUT_MINUTES = 5
    PROGRESS_INTERVAL = 50

class DiscordExporter:
    def __init__(self):
        token = os.getenv(Config.BOT_TOKEN_ENV)
        if not token:
            raise RuntimeError(f"Environment variable {Config.BOT_TOKEN_ENV} not set")
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        self.client = discord.Client(intents=intents)
        self.bot_token = token

        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        self.sheets_client = None
        if Config.USE_GOOGLE_SHEETS:
            self.init_google_sheets()

    def init_google_sheets(self):
        token_path = Path('token.pickle')
        creds = None
        if token_path.exists():
            with open(token_path, 'rb') as f:
                creds = pickle.load(f)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    Config.OAUTH_CLIENT_SECRET_FILE,
                    scopes=Config.OAUTH_SCOPES
                )
                creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
            with open(token_path, 'wb') as f:
                pickle.dump(creds, f)
        try:
            self.sheets_client = gspread.authorize(creds)
            print("✅ Google Sheets authentication successful")
        except Exception as e:
            print(f"❌ Google Sheets auth error: {e}")
            Config.USE_GOOGLE_SHEETS = False

    async def setup_handlers(self):
        @self.client.event
        async def on_ready():
            print(f"✅ Logged in as {self.client.user}")
            await self.export_messages()
            await self.client.close()

    def clean_text(self, text: Optional[str]) -> str:
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text).strip()
        fixes = {
            'â€™':"'", 'â€œ':'"', 'â€“':'-', 'â€¢':'•',
            'Ã¼':'ü', 'Ã±':'ñ', 'Ä±':'ı', 'ÄŸ':'ğ', 'ÅŸ':'ş', 'Ã§':'ç', 'Ã¶':'ö'
        }
        for old, new in fixes.items():
            text = text.replace(old, new)
        return text

    def get_channels(self) -> List[discord.TextChannel]:
        channels: List[discord.TextChannel] = []
        if Config.CHANNEL_IDS:
            for cid in Config.CHANNEL_IDS:
                ch = self.client.get_channel(cid)
                if isinstance(ch, discord.TextChannel):
                    channels.append(ch)
        else:
            for guild in self.client.guilds:
                if Config.GUILD_ID and guild.id != Config.GUILD_ID:
                    continue
                for ch in guild.text_channels:
                    perms = ch.permissions_for(guild.me)
                    if perms.view_channel and perms.read_message_history:
                        channels.append(ch)
        print(f"🎯 Found {len(channels)} channels to export")
        return channels

    def format_reactions(self, msg: discord.Message) -> Dict:
        if not msg.reactions:
            return {'total':0, 'details':'', 'unique':0}
        entries = [f"{r.emoji}({r.count})" for r in msg.reactions]
        return {
            'total': sum(r.count for r in msg.reactions),
            'details': ' | '.join(entries),
            'unique': len(msg.reactions)
        }

    def preview(self, content: str) -> str:
        txt = self.clean_text(content)
        return txt if len(txt) <= Config.MAX_CONTENT_PREVIEW else txt[:Config.MAX_CONTENT_PREVIEW]+'...'

    def extract_urls(self, content: str) -> List[str]:
        return re.findall(r'https?://[^\s]+', content)

    async def export_channel(self, channel: discord.TextChannel) -> List[Dict]:
        print(f"📥 Exporting #{channel.name}")
        data = []
        start = asyncio.get_event_loop().time()
        count = 0
        async for m in channel.history(limit=None,
                                        after=Config.START_DATE,
                                        before=Config.END_DATE,
                                        oldest_first=True):
            now = asyncio.get_event_loop().time()
            if now - start > Config.CHANNEL_TIMEOUT_MINUTES * 60:
                print(f"⏰ Timeout for #{channel.name}")
                break
            if not Config.INCLUDE_SYSTEM_MESSAGES and m.type != discord.MessageType.default:
                continue
            react = self.format_reactions(m)
            urls = self.extract_urls(m.content)
            data.append({
                'timestamp': m.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'channel': channel.name,
                'author': str(m.author),
                'preview': self.preview(m.content),
                'length': len(m.content),
                'words': len(m.content.split()),
                'reactions': react['total'],
                'reaction_details': react['details'],
                'attachments': len(m.attachments),
                'urls': len(urls),
                'message_id': str(m.id)
            })
            count += 1
            if count % Config.PROGRESS_INTERVAL == 0:
                print(f"📊 Processed {count} messages")
            start = now
        print(f"✅ {len(data)} messages from #{channel.name}")
        return data

    def upload_sheets(self, df: pd.DataFrame) -> Optional[str]:
        try:
            now = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            title = f"Discord Export {now}"
            print(f"📤 Creating Google Sheet: {title}")
            sheet = self.sheets_client.create(title)
            ws = sheet.worksheets()
            for extra in ws[1:]: sheet.del_worksheet(extra)
            main = ws[0]; main.update_title('All Messages')
            for tab in ['Channel Summary','Daily Activity','Author Activity']:
                sheet.add_worksheet(title=tab, rows=1000, cols=20)
            main.clear()
            cols = ['timestamp','channel','author','preview','reactions','reaction_details','attachments']
            core = df[cols]
            main.update('A1', [core.columns.tolist()] + core.values.tolist())
            url = f"https://docs.google.com/spreadsheets/d/{sheet.id}"
            print(f"✅ Sheet URL: {url}")
            return url
        except Exception as e:
            print(f"❌ Sheet upload error: {e}")
            return None

    def save_local(self, df: pd.DataFrame):
        print("💾 Saving locally...")
        path = f"{Config.OUTPUT_DIR}/export_combined.xlsx"
        df.to_excel(path, index=False)
        print(f"Saved: {path}")

    async def export_messages(self):
        chans = self.get_channels()
        all_msgs = []
        for ch in chans:
            all_msgs.extend(await self.export_channel(ch))
        df_all = pd.DataFrame(all_msgs)
        if Config.USE_GOOGLE_SHEETS and self.sheets_client:
            url = self.upload_sheets(df_all)
            if not url:
                self.save_local(df_all)
        else:
            self.save_local(df_all)

    async def run(self):
        await self.setup_handlers()
        try:
            await self.client.start(self.bot_token)
        finally:
            if not self.client.is_closed():
                await self.client.close()

async def main():
    exporter = DiscordExporter()
    await exporter.run()

if __name__ == '__main__':
    asyncio.run(main())
