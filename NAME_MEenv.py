# Required settings
VOD_ROOT = "/path/to/vod/storage" # Path to store VODs
LOG_ROOT = "/path/to/log/storage" # Path to store logs
RELOAD_INTERVAL = 300  # seconds with minimum of 45
PROBE_INTERVAL = 120   # seconds with minimum of 45
PLATFORM_COOLDOWN = 60  # seconds with minimum of 20

# Optional settings for Discord notifications and profile pictures
DISCORD_WEBHOOK_URL = ""  # Discord webhook URL for notifications (optional)

# If you want profile pictures for YouTube and/or Twitch you must set the API keys
# Twitch requires both client ID and client secret to be set
YOUTUBE_API_KEY = ""      # YouTube API key for profile pictures (optional)
TWITCH_CLIENT_ID = ""     # Twitch client ID for API access (required for Twitch profile pictures)
TWITCH_CLIENT_SECRET = "" # Twitch client secret for API access (required for Twitch profile pictures)
