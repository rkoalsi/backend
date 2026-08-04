"""
Daily motivation + celebration engine for the salesperson homepage.

Two separate things live here:

* `build_motivation()` — one short, positive line shown every day under the
  greeting. Candidates are generated from the rep's own numbers; whichever
  qualifies with the highest priority wins, and a date-seeded shuffle rotates
  between the close contenders so the same sentence doesn't sit there for three
  weeks. If nothing data-driven qualifies (new joiner, first day of the month)
  an evergreen line is picked from the pool below.

* `build_celebration()` — a rare one-off moment (beat last month, personal
  best, crossed a round number). Unlike the daily line these are *events*: each
  has a stable key, the frontend shows it once with confetti, then POSTs the
  key back so it's never shown again.

Tone rules, deliberate and worth keeping:
  - Never comparative-negative. A rep who is down on last month gets a
    forward-looking line, never the drop itself, and never a peer comparison.
  - No surveillance phrasing ("we noticed you were idle on Tuesday").
  - One sentence, and it has to still read well at 360px wide.
"""

import datetime as dt
import hashlib
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Evergreen pool — the fallback tier when no data-driven rule qualifies.
# ---------------------------------------------------------------------------

EVERGREEN_MESSAGES: List[str] = [
    "Every order starts with a conversation. Go start one. 💬",
    "The best salespeople aren't the loudest — they're the ones who follow up. 📞",
    "One good call today beats ten planned for tomorrow.",
    "Your customers remember how you made things easy for them. 🐾",
    "Small wins, stacked daily, become a big month.",
    "Nobody ever regretted checking in on a quiet customer.",
    "A 'no' today is often just a 'not yet'. Keep the door open.",
    "Know the products, and the pitch takes care of itself. 📚",
    "The follow-up is where most of the business actually lives.",
    "Somebody's shop is running low on something right now. Find out who. 🔎",
    "Be the rep they call first. That's the whole job.",
    "Consistency beats intensity. Show up again today.",
    "Every shop you walk into is a chance to learn something. 🚶",
    "Solve the problem, and the order follows.",
    "Your pipeline is only as warm as your last conversation.",
    "Listening closes more deals than talking. 👂",
    "Today's groundwork is next month's number.",
    "The customer who felt heard comes back. Every time.",
    "Progress over perfection — just move one deal forward. ➡️",
    "A quick call beats a perfect email nobody opens.",
    "Reliability is a feature. Be the one who always replies. ⚡",
    "The hardest order is the first one of the day. Get it done early.",
    "You know this range better than anyone. Use that. 🧠",
    "Momentum is built, not found. Start now.",
    "Check on the customer who hasn't ordered in a while. 🤝",
    "Good reps sell products. Great reps solve problems.",
    "Every 'let me check' is a reason to call back tomorrow.",
    "Make it easy to say yes. That's the craft.",
    "The market rewards the person who shows up consistently. 🎯",
    "Tails wag when the shelves are stocked. Go stock some shelves. 🐕",
]


# ---------------------------------------------------------------------------
# Seeding — stable for a whole day, different per person.
# ---------------------------------------------------------------------------


def _daily_seed(user_id: str, today: dt.date, salt: str = "") -> int:
    """Deterministic per (user, day) so the line doesn't reshuffle on refresh."""
    raw = f"{user_id}|{today.isoformat()}|{salt}".encode("utf-8")
    return int(hashlib.sha256(raw).hexdigest()[:12], 16)


def _inr(value: float) -> str:
    """₹ with Indian digit grouping, no decimals — matches the rest of the UI."""
    n = int(round(value or 0))
    if n >= 10000000:
        return f"₹{n / 10000000:.2f}Cr".replace(".00Cr", "Cr")
    if n >= 100000:
        return f"₹{n / 100000:.2f}L".replace(".00L", "L")
    s = str(n)
    if len(s) <= 3:
        return f"₹{s}"
    head, tail = s[:-3], s[-3:]
    parts = []
    while len(head) > 2:
        parts.insert(0, head[-2:])
        head = head[:-2]
    if head:
        parts.insert(0, head)
    return "₹" + ",".join(parts + [tail])


def _next_round(value: float, steps: List[int]) -> Optional[int]:
    """Smallest boundary in `steps` still above `value`."""
    for step in steps:
        if value < step:
            return step
    return None


# ---------------------------------------------------------------------------
# The daily line
# ---------------------------------------------------------------------------


def build_motivation(
    user_id: str,
    first_name: str,
    stats: Dict[str, Any],
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    """
    Pick today's line.

    `stats` is the bundle assembled in routes/orders.py:
        this_month / last_month  -> {"total_count", "total_value"}
        monthly_history          -> [{"key","count","value"}] oldest→newest,
                                    EXCLUDING the current month
        customers_this_month     -> int
        best_customers_month     -> int   (best previous month's distinct count)
        streak_days              -> int   (consecutive recent days with an order)

    Returns {"text", "tone", "emoji", "rule"} — `rule` is for debugging only.
    """
    now = now or dt.datetime.now()
    today = now.date()
    day_of_month = today.day

    this_month = stats.get("this_month") or {}
    last_month = stats.get("last_month") or {}
    count = this_month.get("total_count") or 0
    value = float(this_month.get("total_value") or 0)
    last_count = last_month.get("total_count") or 0
    last_value = float(last_month.get("total_value") or 0)

    history = stats.get("monthly_history") or []
    best_prev_value = max((float(m.get("value") or 0) for m in history), default=0.0)
    customers = stats.get("customers_this_month") or 0
    best_customers = stats.get("best_customers_month") or 0
    streak = stats.get("streak_days") or 0

    # Days left in the month, used by several lines.
    if now.month == 12:
        next_month_start = dt.datetime(now.year + 1, 1, 1)
    else:
        next_month_start = dt.datetime(now.year, now.month + 1, 1)
    days_left = max(0, (next_month_start.date() - today).days)
    month_name = today.strftime("%B")

    candidates: List[Dict[str, Any]] = []

    def add(priority: int, rule: str, text: str, tone: str = "positive", emoji: str = "✨"):
        candidates.append(
            {"priority": priority, "rule": rule, "text": text, "tone": tone, "emoji": emoji}
        )

    # --- Personal best -----------------------------------------------------
    if history and count >= 3 and value > best_prev_value > 0:
        add(
            100,
            "personal_best",
            f"This is already your biggest month on record. Nobody's catching you now.",
            "celebrate",
            "🏆",
        )

    # --- Milestone proximity ----------------------------------------------
    if count >= 6:
        target = _next_round(count, [10, 25, 50, 75, 100, 150, 200, 300])
        if target and (target - count) <= 4:
            gap = target - count
            add(
                90,
                "milestone_orders",
                f"{gap} more {'order' if gap == 1 else 'orders'} and you cross {target} this month.",
                "push",
                "🎯",
            )

    if value > 0:
        target_v = _next_round(value, [100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000])
        if target_v and value >= target_v * 0.85:
            add(
                88,
                "milestone_value",
                f"{_inr(target_v - value)} away from {_inr(target_v)} this month. So close.",
                "push",
                "📈",
            )

    # --- Streak ------------------------------------------------------------
    if streak >= 3:
        add(
            85,
            "streak",
            f"{streak} days running with an order logged. Keep the chain alive.",
            "celebrate",
            "🔥",
        )

    # --- Pace projection ---------------------------------------------------
    if day_of_month >= 5 and value > 0 and last_value > 0:
        days_in_month = (next_month_start.date() - today.replace(day=1)).days
        projected = value / day_of_month * days_in_month
        if projected > last_value * 1.05:
            add(
                80,
                "pace",
                f"At this pace you'll finish {month_name} around {_inr(projected)} — ahead of last month.",
                "positive",
                "🚀",
            )

    # --- Raw momentum ------------------------------------------------------
    if last_value > 0 and value > last_value:
        add(
            75,
            "momentum_value",
            f"You're already {_inr(value - last_value)} ahead of all of last month, with {days_left} days to go.",
            "celebrate",
            "💪",
        )
    elif last_count > 0 and count > last_count:
        add(
            74,
            "momentum_count",
            f"{count} orders this month — you've already beaten last month's {last_count}.",
            "celebrate",
            "💪",
        )

    # --- Breadth of customers ---------------------------------------------
    if customers > best_customers > 0:
        add(
            70,
            "breadth_best",
            f"{customers} different customers ordered from you this month — your widest spread yet.",
            "celebrate",
            "🌍",
        )
    elif customers >= 5:
        add(
            60,
            "breadth",
            f"{customers} customers served this month. Every one of those is a relationship.",
            "positive",
            "🤝",
        )

    # --- Month shape -------------------------------------------------------
    if day_of_month <= 3:
        add(
            55,
            "fresh_month",
            f"Fresh month, clean slate. The first order of {month_name} is up for grabs.",
            "positive",
            "🌅",
        )

    if count == 0 and day_of_month > 3:
        # Deliberately not "you have no orders". Forward-looking only.
        add(
            50,
            "recovery",
            "Every big month starts with one order. Today's as good a day as any.",
            "gentle",
            "🌱",
        )
    elif 1 <= count <= 3:
        add(
            45,
            "warmup",
            f"{count} {'order' if count == 1 else 'orders'} in. Momentum builds one call at a time.",
            "positive",
            "🌤️",
        )

    if days_left <= 5 and count > 0:
        add(
            72,
            "final_push",
            f"{days_left} {'day' if days_left == 1 else 'days'} left in {month_name}. Time for a strong finish.",
            "push",
            "⏱️",
        )

    # --- Evergreen fallback ------------------------------------------------
    pool_index = _daily_seed(user_id, today, "evergreen") % len(EVERGREEN_MESSAGES)
    evergreen = {
        "priority": 0,
        "rule": "evergreen",
        "text": EVERGREEN_MESSAGES[pool_index],
        "tone": "positive",
        "emoji": "✨",
    }

    if not candidates:
        return {k: evergreen[k] for k in ("text", "tone", "emoji", "rule")}

    # Rotate among the close contenders. Taking strictly the top rule would show
    # "you're ahead of last month" every day for three weeks; instead we take the
    # leading band (top priority and anything within 15 of it) and let the daily
    # seed choose inside it. Every ~5th day an evergreen line gets a turn too, so
    # the strip doesn't become pure dashboard.
    candidates.sort(key=lambda c: c["priority"], reverse=True)
    top = candidates[0]["priority"]
    band = [c for c in candidates if top - c["priority"] <= 15][:4]

    seed = _daily_seed(user_id, today)
    if top < 85 and seed % 5 == 0:
        band = band + [evergreen]

    chosen = band[seed % len(band)]
    return {k: chosen[k] for k in ("text", "tone", "emoji", "rule")}


# ---------------------------------------------------------------------------
# One-off celebrations
# ---------------------------------------------------------------------------


def build_celebration(
    stats: Dict[str, Any],
    seen_keys: List[str],
    now: Optional[dt.datetime] = None,
) -> Optional[Dict[str, Any]]:
    """
    Highest-value celebration this rep has earned and not yet been shown.

    Keys are month-scoped so the same achievement can be earned again next
    month. Returns None when there's nothing to celebrate.
    """
    now = now or dt.datetime.now()
    period = now.strftime("%Y-%m")
    month_name = now.strftime("%B")
    seen = set(seen_keys or [])

    this_month = stats.get("this_month") or {}
    last_month = stats.get("last_month") or {}
    count = this_month.get("total_count") or 0
    value = float(this_month.get("total_value") or 0)
    last_value = float(last_month.get("total_value") or 0)
    history = stats.get("monthly_history") or []
    best_prev_value = max((float(m.get("value") or 0) for m in history), default=0.0)

    earned: List[Dict[str, Any]] = []

    if history and count >= 3 and value > best_prev_value > 0:
        earned.append(
            {
                "rank": 100,
                "key": f"personal_best:{period}",
                "title": "Your best month ever! 🏆",
                "message": f"{_inr(value)} in {month_name} — that's a new personal record. Take a second to enjoy it.",
                "emoji": "🏆",
            }
        )

    for step in (10000000, 5000000, 2500000, 1000000, 500000):
        if value >= step:
            earned.append(
                {
                    "rank": 90 + (step // 1000000),
                    "key": f"value_milestone:{period}:{step}",
                    "title": f"{_inr(step)} crossed! 🎉",
                    "message": f"You've passed {_inr(step)} in {month_name}. That's a serious month.",
                    "emoji": "🎉",
                }
            )
            break

    for step in (200, 150, 100, 75, 50, 25):
        if count >= step:
            earned.append(
                {
                    "rank": 60 + (step // 25),
                    "key": f"orders_milestone:{period}:{step}",
                    "title": f"{step} orders this month! 🚀",
                    "message": f"{count} orders logged in {month_name}. The consistency is showing.",
                    "emoji": "🚀",
                }
            )
            break

    if last_value > 0 and value > last_value:
        earned.append(
            {
                "rank": 50,
                "key": f"beat_last_month:{period}",
                "title": "You beat last month! 💪",
                "message": f"{_inr(value)} against last month's {_inr(last_value)} — and {month_name} isn't even done.",
                "emoji": "💪",
            }
        )

    unseen = [e for e in earned if e["key"] not in seen]
    if not unseen:
        return None

    best = max(unseen, key=lambda e: e["rank"])
    return {k: best[k] for k in ("key", "title", "message", "emoji")}
