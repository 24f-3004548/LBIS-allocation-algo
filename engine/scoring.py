import logging
import httpx
from config import PERPLEXITY_API_KEY, PERPLEXITY_API_URL, PERPLEXITY_MODEL
from db.client import upsert_scoring
from utils.retry import with_retry

log = logging.getLogger(__name__)

SCORING_PROMPT = """You are a forensic investment analyst with deep expertise in fundamental analysis, management evaluation, and historical stock research. You have access to all publicly available information up to the analysis date — including annual reports, quarterly reports, management transcripts, press conferences, investor presentations, and video recordings. Where video content is available, you must also assess management body language and confidence levels. You must not use any information beyond the specified historical date. Zero forward-looking bias is permitted.
ANALYSIS DATE: {analysis_date}
COMPANY: {company_name}
STOCK PRICE ON ANALYSIS DATE: {stock_price}
---
SECTION 1 — COMPANY OVERVIEW
In 3–5 lines, describe what {company_name} does — its core business, primary products/services, key customers, and revenue model — as understood from all data available up to {analysis_date}.
---
SECTION 2 — THEMATIC & STRUCTURAL CONTEXT (answer each with YES/NO + 1-line reasoning)
A. Does the company operate in a SUNRISE SECTOR (high-growth, future-facing industry) as of {analysis_date}?
B. Does the company have a STRONG, RECOGNIZABLE BRAND with pricing power?
C. Are there any CAPACITY EXPANSION plans announced or recently completed as of {analysis_date}?
D. Are there any MERGER, ACQUISITION, or strategic alliance news as of {analysis_date}?
E. Are there DIRECT OR INDIRECT GOVERNMENT BUDGET ALLOCATIONS benefiting this company or sector?
---
SECTION 3 — INSTITUTIONAL & SMART MONEY OWNERSHIP (as of {analysis_date})
Report the following as available from public disclosures, SEBI filings, and shareholding pattern data:
- Mutual Fund Holdings (% and key fund names)
- DII (Domestic Institutional Investors) aggregate holding %
- FII (Foreign Institutional Investors) aggregate holding %
- HNI / High-Net-Worth Individual notable positions (if any public data exists)
- Promoter Holding %
- Any notable changes in institutional ownership in the preceding 4 quarters
---
SECTION 4 — DETAILED SCORING (15 PARAMETERS)
Score {company_name} on each of the following 15 parameters out of 100 each, strictly based on data available up to {analysis_date}. For each parameter, provide: the score, and 3–5 lines of reasoning referencing specific data points (reports, statements, filings, or observable management behavior from videos/transcripts where applicable).
1. PROMOTER HOLDING — Quality, stability, and confidence signaled by promoter stake and changes
2. MANAGEMENT EXPERIENCE (in years) — Depth, longevity, and domain expertise of the leadership team
3. MARKET OPPORTUNITY — Size, growth rate, and headroom of addressable market as of analysis date
4. GOVERNMENT BUDGET ALLOCATION — Direct/indirect policy or budget tailwinds for the company/sector
5. MANAGEMENT ASPIRATION — Ambition, articulated vision, and growth targets stated by management
6. INTEGRITY OF MANAGEMENT — Track record of честность: related-party transactions, audit qualifications, governance flags
7. PRODUCT INNOVATION — New product launches, R&D spend, patents, or technology differentiation
8. TECHNOLOGY ADOPTION — Use of modern manufacturing, automation, or digital tools relative to peers
9. EXPORT OPPORTUNITY EXECUTION — Actual export revenues, growth, global partnerships, and execution track record
10. POLITICAL CONNECTIONS — Proximity to policy levers without compromising governance (neutral assessment)
11. TIMELY COMPLETION OF ORDERS — On-time delivery record, customer satisfaction signals from filings/transcripts
12. TIMELY EXECUTION OF PROJECTS (CAPEX) — History of on-budget, on-schedule capital project delivery
13. HIGH MARGIN OR MONOPOLY PRODUCTS — Presence of premium, proprietary, or near-monopoly product lines with margin evidence
14. DEBTOR DAYS — Receivables efficiency: debtor days trend over the last 3–5 years available
15. CURRENT FINANCIAL CONDITION — Balance sheet strength: D/E ratio, interest coverage, cash flow, working capital health
---
SECTION 5 — VALUATION & FORWARD ESTIMATE (strictly as of {analysis_date})
Based only on data available up to {analysis_date} and assuming bullish market state:
- Current P/E ratio of {company_name} at price {stock_price}
- Historical P/E range (last 3–5 years if available)
- Earnings Growth Rate (trailing and estimated based on visible order book / capacity / guidance available at that date)
- Probability and reasoning for P/E EXPANSION in the next 3 years from {analysis_date}
- TARGET PRICE in 3 years from {analysis_date}: derive using earnings growth + P/E re-rating scenario. Show your working.
---
SECTION 6 — MANAGEMENT BODY LANGUAGE & TONE ASSESSMENT
Where video recordings, press conferences, or investor day presentations are available up to {analysis_date}:
- Assess the confidence, consistency, and transparency of management communication
- Note any observable hesitation, evasiveness, or contradictions between stated guidance and body language
- Rate overall management communication quality: HIGH / MEDIUM / LOW with reasoning
---
SECTION 7 — EXCEL EXPORT ROW (MANDATORY — OUTPUT THIS EXACTLY)
Output a single line of pipe-separated values (no headers) in this exact order, suitable for Excel Text-to-Columns splitting on "|":
{company_name} | {analysis_date} | {stock_price} | [Score 1] | [Score 2] | [Score 3] | [Score 4] | [Score 5] | [Score 6] | [Score 7] | [Score 8] | [Score 9] | [Score 10] | [Score 11] | [Score 12] | [Score 13] | [Score 14] | [Score 15] | [3Y Target Price] | [Sunrise Sector Y/N] | [Strong Brand Y/N] | [Capacity Expansion Y/N] | [M&A News Y/N] | [Govt Tailwind Y/N] | [Promoter Holding %] | [FII %] | [DII %] | [MF %]
---
STRICT RULES:
- Use ONLY information publicly available on or before {analysis_date}
- DO NOT apply hindsight or reference any event, result, or data point after {analysis_date}
- Cite specific reports, filings, or disclosures where possible
- If data is unavailable for a parameter, assign a conservative score with explanation.
- ONLY SEND SECTION 7 AS RAW OUTPUT, no other text.
"""

def _compute_green_count(total_score, flags):
    count = 0
    always_green_keys = ["ma_news", "sunrise_sector",
                         "capacity_expansion", "strong_brand"]
    for key in always_green_keys:
        if flags.get(key, "N").upper() == "Y":
            count += 1

    conditional = {
        "market_opportunity": flags.get("score_3"),
        "high_margin": flags.get("score_13"),
        "govt_allocation": flags.get("score_4"),
        "promoter_holdings": flags.get("score_1"),
        "mgmt_aspiration": flags.get("score_5"),
    }
    for _, param_score in conditional.items():
        if float(param_score or 0) > 90:
            count += 1

    return min(count, 9)

def _parse_section7(response_text):
    lines = response_text.strip().splitlines()
    pipe_line = None

    for line in reversed(lines):
        if line.count("|") >= 20 and "---" not in line:
            pipe_line = line.strip()
            break

    if not pipe_line:
        raise ValueError("Section 7 export row not found in model response.")

    parts = [p.strip() for p in pipe_line.strip("|").split("|")]

    if len(parts) < 28:
        parts.extend(["0"] * (28 - len(parts)))

    def _f(val):
        clean = val.replace("%", "").replace(",", "").replace(
            "9", "").replace("$", "").replace("Rs.", "").strip()
        if not clean or clean.upper() in ("N", "N/A", "NA", "-", "NONE", "NULL", "ND"):
            return None
        import re
        if "/" in clean:
            try:
                num, denom = clean.split("/")
                return float(num) / float(denom) * 100
            except Exception:
                pass
        if "-" in clean:
            try:
                parts = clean.split("-")
                nums = [float(p) for p in parts if p.strip().replace('.', '', 1).isdigit()]
                if nums:
                    return sum(nums) / len(nums)
            except Exception:
                pass
        text_map = {"high": 90, "medium": 50, "low": 20}
        if clean.lower() in text_map:
            return text_map[clean.lower()]
        try:
            return float(clean)
        except ValueError:
            return None

    def _yn(val):
        return "Y" if val.strip().upper().startswith("Y") else "N"

    result = {
        "company_name": parts[0],
        "analysis_date": parts[1],
        "stock_price": _f(parts[2]),
        "score_1": _f(parts[3]),
        "score_2": _f(parts[4]),
        "score_3": _f(parts[5]),
        "score_4": _f(parts[6]),
        "score_5": _f(parts[7]),
        "score_6": _f(parts[8]),
        "score_7": _f(parts[9]),
        "score_8": _f(parts[10]),
        "score_9": _f(parts[11]),
        "score_10": _f(parts[12]),
        "score_11": _f(parts[13]),
        "score_12": _f(parts[14]),
        "score_13": _f(parts[15]),
        "score_14": _f(parts[16]),
        "score_15": _f(parts[17]),
        "target_price_3y": _f(parts[18]),
        "sunrise_sector": _yn(parts[19]),
        "strong_brand": _yn(parts[20]),
        "capacity_expansion": _yn(parts[21]),
        "ma_news": _yn(parts[22]),
        "govt_tailwind": _yn(parts[23]),
        "promoter_pct": _f(parts[24]),
        "fii_pct": _f(parts[25]),
        "dii_pct": _f(parts[26]),
        "mf_pct": _f(parts[27]),
    }
    import logging
    score_keys = [f"score_{i}" for i in range(1, 16)]
    if all(result[k] is None for k in score_keys):
        logging.warning("All parsed scores are None for company: %s", result.get("company_name"))
    return result

from utils.retry import with_retry

@with_retry(max_attempts=3, backoff_base=2.0, exceptions=(Exception,), label="Perplexity+Parse")
def call_perplexity_and_parse(prompt):
    response = _call_perplexity(prompt)
    parsed = _parse_section7(response)
    return response, parsed

@with_retry(max_attempts=3, backoff_base=2.0, exceptions=(httpx.HTTPError, ValueError), label="Perplexity")
def _call_perplexity(prompt):
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": PERPLEXITY_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4000,
        "temperature": 0.2,
    }
    with httpx.Client(timeout=120) as client:
        resp = client.post(PERPLEXITY_API_URL, json=payload, headers=headers)
        resp.raise_for_status()

    return resp.json()["choices"][0]["message"]["content"]

def _save_llm_response(unit_id, name, date, response):
    import os
    from datetime import datetime
    os.makedirs("logs/llm_responses", exist_ok=True)
    filename = f"logs/llm_responses/{unit_id}_{name}_{date}.md".replace(" ", "_")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# LLM Response for {name} ({unit_id})\n")
        f.write(f"Date: {date}\n")
        f.write(f"Generated At: {datetime.now().isoformat()}\n\n")
        f.write(response)
    log.info(f"Stored LLM response: {filename}")

def run_scoring(unit_id, name, analysis_date, stock_price):
    prompt = SCORING_PROMPT.format(
        company_name=name,
        analysis_date=analysis_date,
        stock_price=stock_price,
    )

    raw_response, parsed = call_perplexity_and_parse(prompt)
    
    _save_llm_response(unit_id, name, analysis_date, raw_response)
    
    total_score = sum(parsed[f"score_{i}"] or 0.0 for i in range(1, 16))
    target_3y = parsed.get("target_price_3y")
    max_return = (target_3y - stock_price) / \
        stock_price if (target_3y is not None and stock_price) else None

    green_count = _compute_green_count(total_score, parsed)
    upsert_scoring(unit_id, {
        "green_count": green_count,
        "score": total_score,
        "max_return": max_return,
    })

    log.info(f"Scored {unit_id}: {total_score:.1f}")
    return parsed
