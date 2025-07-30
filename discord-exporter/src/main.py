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
    BOT_TOKEN = 'MTM3OTg3MjMwOTI5MjE3NTM4MA.GHcrXM.raUBDX-WRQ2OpDXAFnbhbq0FfH3ogbIX5mLkBE'
    GUILD_ID: Optional[int] = None  # Discord guild ID or None for all
    CHANNEL_IDS: List[int] = []     # Specific channel IDs or empty for all

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

    # Channels to skip
    SKIP_CHANNELS = ["🌟・welcome", "welcome", "🌟・ welcome"]

    # Performance settings
    CHANNEL_TIMEOUT_MINUTES = 5
    PROGRESS_INTERVAL = 50

class DiscordExporter:
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        self.client = discord.Client(intents=intents)
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        self.sheets_client = None
        if Config.USE_GOOGLE_SHEETS:
            self.init_google_sheets()

    def init_google_sheets(self):
        """Authenticate to Google Sheets via OAuth2 and persist tokens."""
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
            'â€™':"'", 'â€œ':'"', 'â€': '"', 'â€¢':'•',
            'â€“':'-', 'Ã¼':'ü', 'Ã±':'ñ', 'Ä±':'ı', 'ÄŸ':'ğ',
            'ÅŸ':'ş', 'Ã§':'ç', 'Ã¶':'ö'
        }
        for old, new in fixes.items():
            text = text.replace(old, new)
        return text

    def should_skip_channel(self, name: str) -> bool:
        key = name.lower().strip()
        return any(pat.lower().strip() == key for pat in Config.SKIP_CHANNELS)

    def get_channels(self) -> List[discord.TextChannel]:
        channels = []
        skipped = []
        if Config.CHANNEL_IDS:
            for cid in Config.CHANNEL_IDS:
                ch = self.client.get_channel(cid)
                if isinstance(ch, discord.TextChannel) and not self.should_skip_channel(ch.name):
                    channels.append(ch)
                else:
                    skipped.append(str(cid))
        else:
            for guild in self.client.guilds:
                if Config.GUILD_ID and guild.id != Config.GUILD_ID:
                    continue
                for ch in guild.text_channels:
                    if self.should_skip_channel(ch.name):
                        skipped.append(ch.name)
                        continue
                    perms = ch.permissions_for(guild.me)
                    if perms.view_channel and perms.read_message_history:
                        channels.append(ch)
        if skipped:
            print(f"⏭️ Skipped channels: {', '.join(skipped)}")
        return channels

    def format_reactions(self, msg: discord.Message) -> Dict:
        if not msg.reactions:
            return { 'total':0, 'details':'', 'unique':0 }
        entries = [f"{str(r.emoji)}({r.count})" for r in msg.reactions]
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
        async for m in channel.history(limit=None, after=Config.START_DATE,
                                        before=Config.END_DATE, oldest_first=True):
            now = asyncio.get_event_loop().time()
            if now - start > Config.CHANNEL_TIMEOUT_MINUTES*60:
                print(f"⏰ Timeout for #{channel.name}")
                break
            if not Config.INCLUDE_SYSTEM_MESSAGES and m.type != discord.MessageType.default:
                continue
            react = self.format_reactions(m)
            urls = self.extract_urls(m.content)
            data.append({
                'timestamp': m.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'channel': channel.name,
                'guild': channel.guild.name,
                'author': str(m.author),
                'preview': self.preview(m.content),
                'full': self.clean_text(m.content),
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
                print(f"📊 Processed {count} messages in #{channel.name}")
            start = now
        print(f"✅ {len(data)} messages exported from #{channel.name}")
        return data

    def upload_sheets(self, df: pd.DataFrame, summary: List[Dict]) -> Optional[str]:
        try:
            now = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            title = f"Discord Export {now}"
            print(f"📤 Creating sheet: {title}")
            sheet = self.sheets_client.create(title)
            # Prepare sheets
            titles = ['All Messages', 'Channel Summary', 'Daily Activity', 'Author Activity']
            ws = sheet.worksheets()
            # Delete extras
            for extra in ws[1:]: sheet.del_worksheet(extra)
            main = ws[0]; main.update_title('All Messages')
            for t in titles[1:]: sheet.add_worksheet(title=t, rows=1000, cols=20)
            # All Messages
            main.clear()
            cols = ['timestamp','channel','author','preview','reactions','reaction_details','attachments']
            data = df[cols]
            main.update('A1', [data.columns.tolist()] + data.values.tolist())
            # Channel Summary
            df_sum = pd.DataFrame(summary)
            sum_ws = sheet.worksheet('Channel Summary')
            sum_ws.update('A1', [df_sum.columns.tolist()] + df_sum.values.tolist())
            # Daily Activity
            daily = df.groupby(df['timestamp'].str[:10]).agg(
                messages=('message_id','count'),
                reactions=('reactions','sum'),
                unique_authors=('author','nunique')
            ).reset_index().rename(columns={'timestamp':'date'})
            daily_ws = sheet.worksheet('Daily Activity')
            daily_ws.update('A1', [daily.columns.tolist()] + daily.values.tolist())
            # Author Activity
            auth = df.groupby('author').agg(
                messages=('message_id','count'),
                reactions=('reactions','sum'),
                avg_length=('length','mean')
            ).reset_index()
            auth_ws = sheet.worksheet('Author Activity')
            auth_ws.update('A1', [auth.columns.tolist()] + auth.values.tolist())
            url = f"https://docs.google.com/spreadsheets/d/{sheet.id}"
            print(f"✅ Sheet created: {url}")
            return url
        except Exception as e:
            print(f"❌ Sheet upload error: {e}")
            return None

    def save_local(self, df: pd.DataFrame, summary: List[Dict]):
        print("💾 Saving locally...")
        excel = f"{Config.OUTPUT_DIR}/export_combined.xlsx"
        df.to_excel(excel, index=False)
        print(f"Saved: {excel}")

    async def export_messages(self):
        channels = self.get_channels()
        if not channels:
            print("❌ No channels to export.")
            return
        print(f"🎯 Exporting {len(channels)} channels...")
        all_msgs, summary = [], []
        for ch in channels:
            data = await self.export_channel(ch)
            all_msgs.extend(data)
            if data:
                dfc = pd.DataFrame(data)
                summary.append({
                    'channel': ch.name,
                    'total_messages': len(data),
                    'unique_authors': dfc['author'].nunique(),
                    'total_reactions': dfc['reactions'].sum(),
                    'avg_length': dfc['length'].mean()
                })
        df_all = pd.DataFrame(all_msgs)
        if Config.USE_GOOGLE_SHEETS and self.sheets_client:
            url = self.upload_sheets(df_all, summary)
            if not url:
                self.save_local(df_all, summary)
        else:
            self.save_local(df_all, summary)

    async def run(self):
        await self.setup_handlers()
        try:
            await self.client.start(Config.BOT_TOKEN)
        finally:
            if not self.client.is_closed():
                await self.client.close()

async def main():
    exporter = DiscordExporter()
    await exporter.run()

if __name__ == '__main__':
    asyncio.run(main())