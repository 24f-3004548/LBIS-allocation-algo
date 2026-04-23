import logging
import httpx
from config import PERPLEXITY_API_KEY, PERPLEXITY_API_URL, PERPLEXITY_MODEL
from db.client import upsert_scoring, update_unit
from utils.retry import with_retry

log = logging.getLogger(__name__)

SCORING_PROMPT = """
You are a forensic investment analyst with deep expertise in fundamental analysis,
management evaluation, and historical stock research. You must aggressively use your
web search capabilities and internal knowledge base to find publicly available
information up to the analysis date — including financials, news, and transcripts.
If specific documents (like videos or full annual reports) are not surfaced in your
search, you must synthesize the best possible estimates from the information you DO find.
You must not use any information beyond the specified historical date. Zero forward-looking
bias is permitted.

ANALYSIS DATE: {analysis_date}
COMPANY: {company_name}
STOCK PRICE ON ANALYSIS DATE: {stock_price}

---

SECTION 1 — COMPANY OVERVIEW
In 3–5 lines, describe what {company_name} does — its core business, primary
products/services, key customers, and revenue model — as understood from all data
available up to {analysis_date}.

---

SECTION 2 — THEMATIC & STRUCTURAL CONTEXT (answer each with YES/NO + 1-line reasoning)

A. Does the company operate in a SUNRISE SECTOR (high-growth, future-facing industry)?
B. Does the company have a STRONG, RECOGNIZABLE BRAND with pricing power?
C. Are there any CAPACITY EXPANSION plans announced or recently completed?
D. Are there any MERGER, ACQUISITION, or strategic alliance news?
E. Are there DIRECT OR INDIRECT GOVERNMENT BUDGET ALLOCATIONS benefiting this company or sector?

---

SECTION 3 — INSTITUTIONAL & SMART MONEY OWNERSHIP
- Mutual Fund Holdings %
- DII aggregate holding %
- FII aggregate holding %
- HNI / High-Net-Worth Individual notable positions
- Promoter Holding %
- Notable changes in institutional ownership in the preceding 4 quarters

---

SECTION 4 — DETAILED SCORING (15 PARAMETERS)
Score on each parameter out of 100. For each: give the score and 3–5 lines of
reasoning with specific data points.

1. PROMOTER HOLDING
2. MANAGEMENT EXPERIENCE (in years)
3. MARKET OPPORTUNITY
4. GOVERNMENT BUDGET ALLOCATION
5. MANAGEMENT ASPIRATION
6. INTEGRITY OF MANAGEMENT
7. PRODUCT INNOVATION
8. TECHNOLOGY ADOPTION
9. EXPORT OPPORTUNITY EXECUTION
10. POLITICAL CONNECTIONS
11. TIMELY COMPLETION OF ORDERS
12. TIMELY EXECUTION OF PROJECTS (CAPEX)
13. HIGH MARGIN OR MONOPOLY PRODUCTS
14. DEBTOR DAYS
15. CURRENT FINANCIAL CONDITION

---

SECTION 5 — VALUATION & FORWARD ESTIMATE
- Current P/E at price {stock_price}
- Historical P/E range (last 3–5 years)
- Earnings Growth Rate
- Probability and reasoning for P/E EXPANSION in next 3 years
- TARGET PRICE in 3 years: show your working

---

SECTION 6 — MANAGEMENT BODY LANGUAGE & TONE ASSESSMENT
Rate overall management communication quality: HIGH / MEDIUM / LOW with reasoning.

---

SECTION 7 — EXCEL EXPORT ROW (MANDATORY)
Output a single pipe-separated line in EXACTLY this order, no headers, no extra text
on that line:

{company_name}|{analysis_date}|{stock_price}|[Score1]|[Score2]|[Score3]|[Score4]|[Score5]|[Score6]|[Score7]|[Score8]|[Score9]|[Score10]|[Score11]|[Score12]|[Score13]|[Score14]|[Score15]|[3YTargetPrice]|[SunriseSector Y/N]|[StrongBrand Y/N]|[CapacityExpansion Y/N]|[MandA Y/N]|[GovtTailwind Y/N]|[PromoterHolding%]|[FII%]|[DII%]|[MF%]

STRICT RULES:
- Use ONLY information publicly available on or before {analysis_date}
- DO NOT apply hindsight or reference any event after {analysis_date}
- Cite specific reports, filings, or disclosures where possible
- If data is unavailable for a parameter, state so and assign a conservative score (e.g. 0)
- CRITICAL: You MUST ALWAYS output the SECTION 7 EXCEL EXPORT ROW at the very end of your response, no matter what. If you feel you lack data, output the row with conservative estimated scores or N/A. Refusing to output Section 7 will crash the system.
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
            "₹", "").replace("$", "").replace("Rs.", "").strip()
        if not clean or clean.upper() in ("N", "N/A", "NA", "-", "NONE", "NULL", "ND"):
            return None
        try:
            return float(clean)
        except ValueError:
            return None

    def _yn(val):
        return "Y" if val.strip().upper().startswith("Y") else "N"

    return {
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

def run_scoring(unit_id, isin, name, analysis_date, stock_price):
    prompt = SCORING_PROMPT.format(
        company_name=name,
        analysis_date=analysis_date,
        stock_price=stock_price,
    )

    raw_response = _call_perplexity(prompt)
    parsed = _parse_section7(raw_response)
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
