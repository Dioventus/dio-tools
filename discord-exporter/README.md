# Discord Exporter

Exports Discord messages & reactions into Google Sheets or local Excel/CSV.

## Setup

1. Copy template and add your credentials:
   \`\`\`bash
   cp oauth_client_secret.json.example oauth_client_secret.json
   # edit oauth_client_secret.json with your client_id & client_secret
   \`\`\`

2. Install dependencies:
   \`\`\`bash
   pip install -r requirements.txt
   \`\`\`

3. Edit \`src/main.py\` and set:
   \`\`\`python
   Config.BOT_TOKEN = "YOUR_DISCORD_BOT_TOKEN"
   \`\`\`

## Run

\`\`\`bash
python src/main.py
\`\`\`

First run will open your browser for Google OAuth; subsequent runs use \`token.pickle\`.

