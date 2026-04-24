import os
from dotenv import load_dotenv

load_dotenv()

 
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

 
PERPLEXITY_API_KEY = os.environ["PERPLEXITY_API_KEY"]
PERPLEXITY_MODEL = "sonar-pro"
PERPLEXITY_API_URL = "https://api.perplexity.ai/chat/completions"

 
INVESTABLE_PCT = 0.98          # 98% of fund goes to investment

TOP_TIER_PCT = 0.10            # Top 10% of stocks = top tier
TOP_TIER_MIN = 22                # Minimum top-tier count
TOP_TIER_ALLOC = 0.70          # 70% of criterion slice to top tier
REST_ALLOC = 0.30              # 30% to the rest


BOTTOM_FISH_PCT = 0.10         # 10% of L1-L4 proceeds per bottom fish
BOTTOM_FISH_MAX = 10             # Maximum bottom fishing buys

REBALANCE_INTERVAL_DAYS = 15    # Rebalance cycle

 
POLL_INTERVAL_SECONDS = 60       # How often to check for new directives
