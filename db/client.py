from supabase import create_client
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY

db = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_unit(unit_id):
    res = db.table("positions").select(
        "*").eq("unit_id", unit_id).single().execute()
    return res.data


def update_unit(unit_id, fields):
    from datetime import datetime, timezone
    fields["last_update"] = datetime.now(timezone.utc).isoformat()
    res = (
        db.table("positions")
        .update(fields)
        .eq("unit_id", unit_id)
        .execute()
    )
    return res.data[0]


def get_all_units_with_scoring():
    res = (
        db.table("positions")
        .select("*, scoring(*)")
        .or_("status.in.(active,free),status.is.null")
        .execute()
    )
    return res.data



def get_unprocessed_directives():
    res = (
        db.table("stock_directives")
        .select("*")
        .is_("processed_at", "null")
        .order("directive_id", desc=False)
        .execute()
    )
    return res.data


def mark_directive_processed(directive_id):
    from datetime import datetime, timezone
    db.table("stock_directives").update(
        {"processed_at": datetime.now(timezone.utc).isoformat()}
    ).eq("directive_id", directive_id).execute()

def insert_directive(
    unit_id,
    isin,
    name,
    directive,
    current_price,
    date=None,
):
    payload = {
        "unit_id": unit_id,
        "isin": isin,
        "name": name,
        "directive": directive,
        "current_price": current_price,
        "processed_at": None,
    }
    if date:
        payload["date"] = date
    res = db.table("stock_directives").insert(payload).execute()
    return res.data[0]



def write_ledger(
    unit_id,
    directive_id,
    delta_shares,
    delta_investment,
    date=None,
):
    from datetime import date as _date

    payload = {
        "unit_id": unit_id,
        "directive_id": directive_id,
        "delta_shares": delta_shares,
        "delta_investment": delta_investment,
    }
    if date:
        payload["date"] = date
    else:
        payload["date"] = _date.today().isoformat()

    res = db.table("ledger").insert(payload).execute()
    return res.data[0]



def upsert_scoring(unit_id, fields):
    payload = {"unit_id": unit_id, **fields}
    res = (
        db.table("scoring")
        .upsert(payload, on_conflict="unit_id")
        .execute()
    )
    return res.data[0]

def delete_scoring(unit_id):
    db.table("scoring").delete().eq("unit_id", unit_id).execute()




def get_portfolio_state():
    res = db.table("portfolio_state").select(
        "*").order("state_id", desc=True).limit(1).execute()
    return res.data[0] if res.data else None

def upsert_portfolio_state(fields):
    state = get_portfolio_state()
    if state:
        res = (
            db.table("portfolio_state")
            .update(fields)
            .eq("state_id", state["state_id"])
            .execute()
        )
    else:
        res = db.table("portfolio_state").insert(fields).execute()
    return res.data[0]
