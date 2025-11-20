from .root import connect_to_mongo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
import logging, asyncio, aiohttp, time, re, os, requests
from typing import Optional, Dict
from collections import OrderedDict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
clientId = os.getenv("CLIENT_ID")
clientSecret = os.getenv("CLIENT_SECRET")
grantType = os.getenv("GRANT_TYPE")
inventory_refresh_token = os.getenv("INVENTORY_REFRESH_TOKEN")
books_refresh_token = os.getenv("BOOKS_REFRESH_TOKEN")
org_id = os.getenv("ORG_ID")

# Rate limiting configuration
RATE_LIMIT = 1.0
MAX_CONCURRENT_REQUESTS = 2
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2

# API URLs
INVENTORY_URL = os.getenv("INVENTORY_URL")
BOOKS_URL = os.getenv("BOOKS_URL")
SLACK_URL = os.getenv("SLACK_URL")


def send_slack_notification(
    title: str, success: bool = True, details: Dict = None, error_msg: str = None
):
    """Enhanced Slack notification function with better formatting and error handling."""
    if not SLACK_URL:
        logger.warning("SLACK_URL not configured, skipping notification")
        return

    try:
        # Determine status and emoji
        if success:
            status = "✅ SUCCESS"
            color = "good"
            emoji = ":white_check_mark:"
        else:
            status = "❌ FAILED"
            color = "danger"
            emoji = ":x:"

        # Build blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title} - {status}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n*Job:* {title}",
                },
            },
        ]

        # Add details if provided
        if success and details:
            detail_text = ""
            if "processed" in details:
                detail_text += f"*Items Processed:* {details['processed']}\n"
            if "inserted" in details:
                detail_text += f"*New Records:* {details['inserted']}\n"
            if "duration" in details:
                detail_text += f"*Duration:* {details['duration']:.1f}s\n"
            if "pages" in details:
                detail_text += f"*Pages Checked:* {details['pages']}\n"

            if detail_text:
                blocks.append(
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": detail_text.strip()},
                    }
                )

        # Add error details if failed
        if not success and error_msg:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:* ```{error_msg[:500]}```",  # Limit error message length
                    },
                }
            )

        payload = {"blocks": blocks}

        response = requests.post(SLACK_URL, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"Slack notification sent successfully for {title}")
        else:
            logger.error(
                f"Slack notification failed: {response.status_code} - {response.text}"
            )

    except Exception as e:
        logger.error(f"Error sending Slack notification: {e}")


class RateLimiter:
    """Rate limiter to ensure we don't exceed API limits."""

    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = rate
        self.updated_at = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            elapsed = now - self.updated_at
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.updated_at = now

            if self.tokens >= 1:
                self.tokens -= 1
                return

            sleep_time = (1 - self.tokens) / self.rate
            await asyncio.sleep(sleep_time)
            self.tokens = 0


class ZohoAPIClient:
    """Base Zoho API client with rate limiting and connection pooling."""

    def __init__(self, service_type: str = "books"):
        self.rate_limiter = RateLimiter(RATE_LIMIT)
        self.access_token = None
        self.session = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.service_type = service_type

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=5,
            limit_per_host=5,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )

        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": f"Zoho-{self.service_type.title()}-Sync/1.0"},
        )

        self.access_token = await self.get_access_token(self.service_type)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_access_token(self, tkn: str) -> Optional[str]:
        """Get access token for Zoho API."""
        url = None
        if tkn == "inventory":
            url = INVENTORY_URL.format(
                clientId=clientId,
                clientSecret=clientSecret,
                grantType=grantType,
                inventory_refresh_token=inventory_refresh_token,
            )
        elif tkn == "books":
            url = BOOKS_URL.format(
                clientId=clientId,
                clientSecret=clientSecret,
                grantType=grantType,
                books_refresh_token=books_refresh_token,
            )

        if not url:
            logger.error("Missing token type")
            return None

        try:
            async with self.session.post(url) as response:
                if response.status == 200:
                    data = await response.json()
                    access_token = data.get("access_token", "")
                    logger.info(
                        f"Got {tkn.capitalize()} Access Token: ...{access_token[-4:]}"
                    )
                    return access_token
                else:
                    text = await response.text()
                    logger.error(
                        f"Failed to get access token: {response.status} - {text}"
                    )
                    return None
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None

    async def make_request(
        self, url: str, max_retries: int = RETRY_ATTEMPTS
    ) -> Optional[Dict]:
        """Make a rate-limited request with retries."""
        headers = {"Authorization": f"Zoho-oauthtoken {self.access_token}"}

        for attempt in range(max_retries):
            try:
                await self.rate_limiter.acquire()

                async with self.semaphore:
                    async with self.session.get(url, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            retry_after = int(
                                response.headers.get("Retry-After", RETRY_DELAY)
                            )
                            logger.warning(
                                f"Rate limited, waiting {retry_after}s before retry"
                            )
                            await asyncio.sleep(retry_after)
                            continue
                        elif response.status in [500, 502, 503, 504]:
                            if attempt < max_retries - 1:
                                wait_time = RETRY_DELAY * (2**attempt)
                                logger.warning(
                                    f"Server error {response.status}, retrying in {wait_time}s"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                        else:
                            text = await response.text()
                            logger.error(f"Request failed: {response.status} - {text}")
                            return None

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Request timeout, attempt {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                else:
                    logger.error("Request failed after timeout retries")
                    return None
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request error: {e}, retrying...")
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                else:
                    logger.error(f"Request failed after retries: {e}")
                    return None

        return None


def sort_dict_recursively(obj):
    """Recursively sort dictionary keys to ensure consistent structure."""
    if isinstance(obj, dict):
        return OrderedDict(
            sorted({k: sort_dict_recursively(v) for k, v in obj.items()}.items())
        )
    elif isinstance(obj, list):
        return [sort_dict_recursively(item) for item in obj]
    else:
        return obj


def parse_datetime_field(date_str):
    """Parse various datetime formats from Zoho API."""
    if not date_str:
        return None

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.warning(f"Could not parse datetime string: {date_str}")
    return date_str


def process_invoice_data(invoice_data):
    """Process invoice data to ensure proper formatting and datetime conversion."""
    invoice_data["invoice_id"] = str(invoice_data["invoice_id"])

    datetime_fields = ["created_time", "date", "due_date", "last_modified_time"]

    for field in datetime_fields:
        if field in invoice_data:
            datetime_value = parse_datetime_field(invoice_data[field])
            if field == "created_time" or field == "date":
                if field == "date" and "created_time" not in invoice_data:
                    invoice_data["created_at"] = datetime_value
                elif field == "created_time":
                    invoice_data["created_at"] = datetime_value
                    invoice_data.pop("created_time", None)
            elif isinstance(datetime_value, datetime):
                invoice_data[field] = datetime_value

    if "created_at" not in invoice_data:
        logger.warning(
            f"No created_at field for invoice {invoice_data.get('invoice_id')}"
        )
        if "date" in invoice_data:
            invoice_data["created_at"] = parse_datetime_field(invoice_data["date"])

    return sort_dict_recursively(invoice_data)


def process_credit_note_data(credit_note_data):
    """Process credit note data to ensure proper formatting and datetime conversion."""
    credit_note_data["creditnote_id"] = str(credit_note_data["creditnote_id"])

    datetime_fields = ["created_time", "date", "due_date", "last_modified_time"]

    for field in datetime_fields:
        if field in credit_note_data:
            datetime_value = parse_datetime_field(credit_note_data[field])
            if field == "created_time" or field == "date":
                if field == "date" and "created_time" not in credit_note_data:
                    credit_note_data["created_at"] = datetime_value
                elif field == "created_time":
                    credit_note_data["created_at"] = datetime_value
                    credit_note_data.pop("created_time", None)
            elif isinstance(datetime_value, datetime):
                credit_note_data[field] = datetime_value

    if "created_at" not in credit_note_data:
        logger.warning(
            f"No created_at field for credit note {credit_note_data.get('creditnote_id')}"
        )
        if "date" in credit_note_data:
            credit_note_data["created_at"] = parse_datetime_field(
                credit_note_data["date"]
            )

    return sort_dict_recursively(credit_note_data)


def process_shipment_data(shipment_data):
    """Process shipment data to ensure proper formatting and datetime conversion."""
    shipment_data["shipment_id"] = str(shipment_data.get("shipment_id", ""))

    # Handle all datetime fields - convert to MongoDB date format
    datetime_fields = [
        "created_time", "date", "last_modified_time",
        "created_time_formatted", "last_modified_time_formatted",
        "mail_first_viewed_time", "mail_last_viewed_time"
    ]

    for field in datetime_fields:
        if field in shipment_data and shipment_data[field]:
            datetime_value = parse_datetime_field(shipment_data[field])
            if isinstance(datetime_value, datetime):
                shipment_data[field] = datetime_value

    # Set created_at from created_time or date
    if "created_time" in shipment_data:
        shipment_data["created_at"] = shipment_data.get("created_time")
    elif "date" in shipment_data:
        date_val = parse_datetime_field(shipment_data["date"])
        if isinstance(date_val, datetime):
            shipment_data["created_at"] = date_val
        else:
            shipment_data["created_at"] = shipment_data["date"]

    if "created_at" not in shipment_data:
        logger.warning(f"No created_at field for shipment {shipment_data.get('shipment_id')}")

    return sort_dict_recursively(shipment_data)


def find_product_id_with_mongo(item_name: str, products_collection) -> Optional[str]:
    """Find product ID by querying MongoDB with various matching strategies."""
    if not item_name:
        return None

    item_name_clean = item_name.strip()

    # Strategy 1: Exact match (case-insensitive)
    product = products_collection.find_one(
        {"$or": [{"item_name": item_name_clean}, {"name": item_name_clean}]}, {"_id": 1}
    )

    if product:
        return product["_id"]

    # Strategy 2: Case-insensitive exact match
    product = products_collection.find_one(
        {
            "$or": [
                {
                    "item_name": {
                        "$regex": f"^{re.escape(item_name_clean)}$",
                        "$options": "i",
                    }
                },
                {
                    "name": {
                        "$regex": f"^{re.escape(item_name_clean)}$",
                        "$options": "i",
                    }
                },
            ]
        },
        {"_id": 1},
    )

    if product:
        return product["_id"]

    # Strategy 3: Simple text search using MongoDB text search (if text index exists)
    try:
        product = products_collection.find_one(
            {"$text": {"$search": item_name_clean}}, {"_id": 1}
        )

        if product:
            return product["_id"]
    except Exception:
        pass

    return None

async def invoices_cron():
    """Cron job for resyncing invoices from previous month till today - delete and reinsert."""
    logger.info("🚀 Starting invoice resync from previous month till today (delete and reinsert)...")
    start_time = time.time()
    
    # Calculate previous month date range dynamically (from previous month till today)
    today = datetime.now()
    
    # Calculate previous month by subtracting one month
    if today.month == 1:
        # If current month is January, previous month is December of last year
        prev_month = 12
        prev_year = today.year - 1
    else:
        # Otherwise, just subtract 1 from current month
        prev_month = today.month - 1
        prev_year = today.year
    
    # Get first day of previous month
    month_start = datetime(prev_year, prev_month, 1, 0, 0, 0, 0)
    
    # Get end date as today (till current date)
    month_end = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    period_description = f"{month_start.strftime('%B %Y')} till {today.strftime('%B %d, %Y')}"
    logger.info(f"📅 Target period: {period_description} ({month_start.date()} to {month_end.date()})")
    
    all_invoices = []
    deleted_count = 0
    
    try:
        _, db = connect_to_mongo()
        collection = db["invoices"]

        # Step 1: Delete existing invoices for the target period
        logger.info(f"🗑️ Deleting existing invoices from {period_description}...")
        delete_result = collection.delete_many({
            "date": {
                "$gte": month_start.strftime("%Y-%m-%d"),
                "$lte": month_end.strftime("%Y-%m-%d")
            }
        })
        deleted_count = delete_result.deleted_count
        logger.info(f"✅ Deleted {deleted_count} existing invoices from {period_description}")

        async with ZohoAPIClient("books") as api_client:
            if not api_client.access_token:
                logger.error("Failed to get access token")
                return

            # Step 2: Fetch all invoices from the target period
            page = 1
            total_fetched = 0
            
            while True:
                logger.info(f"Fetching invoices page {page}...")
                
                # Format dates for API (YYYY-MM-DD)
                date_from = month_start.strftime("%Y-%m-%d")
                date_to = month_end.strftime("%Y-%m-%d")
                
                invoices_url = (
                    f"https://www.zohoapis.com/books/v3/invoices?"
                    f"page={page}&"
                    f"per_page=200&"
                    f"sort_column=created_time&"
                    f"sort_order=D&"
                    f"date_start={date_from}&"
                    f"date_end={date_to}&"
                    f"organization_id={org_id}"
                )

                data = await api_client.make_request(invoices_url)
                if not data or "invoices" not in data:
                    logger.info(f"No invoices found on page {page}")
                    break

                page_invoices = data["invoices"]
                page_count = len(page_invoices)
                total_fetched += page_count
                
                logger.info(f"Found {page_count} invoices on page {page} (Total: {total_fetched})")

                if page_count == 0:
                    break

                # Collect invoice IDs for detailed fetching
                invoice_ids = [str(inv["invoice_id"]) for inv in page_invoices]

                # Fetch detailed data for all invoices on this page
                logger.info(f"Fetching details for {len(invoice_ids)} invoices from page {page}...")

                # Create tasks for fetching invoice details
                detail_tasks = []
                for inv_id in invoice_ids:
                    detail_url = f"https://www.zohoapis.com/books/v3/invoices/{inv_id}?organization_id={org_id}"
                    detail_tasks.append(api_client.make_request(detail_url))

                # Execute all detail requests with limited concurrency
                semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests

                async def fetch_detail_with_semaphore(task):
                    async with semaphore:
                        return await task

                detail_results = await asyncio.gather(
                    *[fetch_detail_with_semaphore(task) for task in detail_tasks],
                    return_exceptions=True,
                )

                # Process results from this page
                page_processed = 0
                for i, result in enumerate(detail_results):
                    try:
                        if isinstance(result, Exception):
                            logger.error(f"Error fetching invoice {invoice_ids[i]}: {result}")
                            continue

                        if result and "invoice" in result:
                            processed_invoice = process_invoice_data(result["invoice"])
                            all_invoices.append(processed_invoice)
                            page_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing invoice {invoice_ids[i]}: {e}")

                logger.info(f"✅ Processed {page_processed}/{page_count} invoices from page {page}")

                # Check if we have more pages
                page_info = data.get("page_context", {})
                has_more_page = page_info.get("has_more_page", False)
                
                if not has_more_page or page_count < 200:  # If less than full page, we're done
                    logger.info("Reached last page or no more data")
                    break
                
                page += 1

            # Step 3: Bulk insert all processed invoices
            if all_invoices:
                logger.info(f"💾 Inserting {len(all_invoices)} processed invoices...")
                collection.insert_many(all_invoices, ordered=False)
                logger.info(f"✅ Successfully inserted {len(all_invoices)} invoices for period: {period_description}")
            else:
                logger.info(f"No invoices to insert for period: {period_description}")

            duration = time.time() - start_time
            
            # Send success notification
            send_slack_notification(
                f"Invoice Resync - {period_description}",
                success=True,
                details={
                    "period": period_description,
                    "deleted": deleted_count,
                    "inserted": len(all_invoices),
                    "total_fetched": total_fetched,
                    "pages": page,
                    "duration": duration,
                },
            )
            
            logger.info(f"🎉 Invoice resync from {period_description} completed successfully!")
            logger.info(f"📊 Summary: Deleted {deleted_count}, Inserted {len(all_invoices)} invoices in {duration:.1f}s")

    except Exception as e:
        error_msg = f"Error in invoice resync from {period_description}: {e}"
        logger.error(error_msg)
        send_slack_notification(
            f"Invoice Resync Error - {period_description}", 
            success=False, 
            error_msg=str(e)
        )
async def credit_notes_cron():
    """Cron job for syncing recent credit notes from first 2 pages."""
    logger.info("🚀 Starting incremental credit notes sync (first 2 pages)...")
    start_time = time.time()
    all_new_credit_notes = []
    new_creditnote_ids = []
    try:
        _, db = connect_to_mongo()
        collection = db["credit_notes"]

        async with ZohoAPIClient("books") as api_client:
            if not api_client.access_token:
                logger.error("Failed to get access token")
                return

            # Fetch credit notes from first 2 pages (most recent first)
            for page in range(1, 3):  # Pages 1, 2
                logger.info(f"Fetching credit notes page {page}/2...")

                creditnotes_url = (
                    f"https://www.zohoapis.com/books/v3/creditnotes?"
                    f"page={page}&"
                    f"per_page=200&"
                    f"sort_column=created_time&"
                    f"sort_order=D&"
                    f"organization_id={org_id}"
                )

                data = await api_client.make_request(creditnotes_url)
                if not data or "creditnotes" not in data:
                    logger.info(f"No credit notes found on page {page}")
                    continue

                page_credit_notes = data["creditnotes"]
                logger.info(
                    f"Found {len(page_credit_notes)} credit notes on page {page}"
                )

                # Check which credit notes are new (batch check for efficiency)
                page_creditnote_ids = [
                    str(cn["creditnote_id"]) for cn in page_credit_notes
                ]
                existing_ids = set()

                # Batch check existing credit notes
                existing_docs = collection.find(
                    {"creditnote_id": {"$in": page_creditnote_ids}},
                    {"creditnote_id": 1},
                )
                for doc in existing_docs:
                    existing_ids.add(doc["creditnote_id"])

                # Collect new credit note IDs
                for credit_note in page_credit_notes:
                    credit_note_id = str(credit_note["creditnote_id"])
                    if credit_note_id not in existing_ids:
                        new_creditnote_ids.append(credit_note_id)

                logger.info(
                    f"Page {page}: {len(page_creditnote_ids) - len(existing_ids)} new credit notes found"
                )

            # Fetch detailed data for all new credit notes concurrently
            if new_creditnote_ids:
                logger.info(
                    f"Fetching details for {len(new_creditnote_ids)} new credit notes..."
                )

                # Create tasks for fetching credit note details
                detail_tasks = []
                for credit_note_id in new_creditnote_ids:
                    detail_url = f"https://www.zohoapis.com/books/v3/creditnotes/{credit_note_id}?organization_id={org_id}"
                    detail_tasks.append(api_client.make_request(detail_url))

                # Execute all detail requests with limited concurrency
                semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent requests

                async def fetch_detail_with_semaphore(task):
                    async with semaphore:
                        return await task

                detail_results = await asyncio.gather(
                    *[fetch_detail_with_semaphore(task) for task in detail_tasks],
                    return_exceptions=True,
                )

                # Process results
                for i, result in enumerate(detail_results):
                    try:
                        if isinstance(result, Exception):
                            logger.error(
                                f"Error fetching credit note {new_creditnote_ids[i]}: {result}"
                            )
                            continue

                        if result and "creditnote" in result:
                            processed_credit_note = process_credit_note_data(
                                result["creditnote"]
                            )
                            all_new_credit_notes.append(processed_credit_note)
                    except Exception as e:
                        logger.error(
                            f"Error processing credit note {new_creditnote_ids[i]}: {e}"
                        )

                # Bulk insert all new credit notes
                if all_new_credit_notes:
                    collection.insert_many(all_new_credit_notes, ordered=False)
                    logger.info(
                        f"✅ Inserted {len(all_new_credit_notes)} new credit notes"
                    )
                else:
                    logger.info("No new credit notes to insert after processing")
            else:
                logger.info("No new credit notes found in first 2 pages")

        duration = time.time() - start_time
        send_slack_notification(
            "Credit Notes Cron",
            success=True,
            details={
                "processed": len(all_new_credit_notes),
                "duration": duration,
            },
        )
    except Exception as e:
        logger.error(f"Error in credit notes sync: {e}")
        send_slack_notification(
            "Credit Notes Cron Error", success=False, error_msg=str(e)
        )


async def shipments_cron():
    """Cron job for syncing all shipments from Zoho Inventory daily."""
    logger.info("🚀 Starting daily shipments sync...")
    start_time = time.time()

    total_processed = 0
    total_inserted = 0
    total_errors = 0

    try:
        _, db = connect_to_mongo()
        collection = db["shipments"]

        async with ZohoAPIClient("inventory") as api_client:
            if not api_client.access_token:
                logger.error("Failed to get access token")
                return

            # Get total pages first
            logger.info("🔍 Determining total number of pages...")
            shipments_url = (
                f"https://www.zohoapis.com/inventory/v1/shipmentorders?"
                f"response_option=2&"
                f"organization_id={org_id}"
            )

            data = await api_client.make_request(shipments_url)
            if not data:
                logger.error("Failed to get response from shipments API")
                return

            page_context = data.get("page_context", {})
            total_pages = page_context.get("total_pages", 1)
            total_shipments = page_context.get("total", 0)

            logger.info(f"📊 Found {total_shipments} total shipments across {total_pages} pages")
            logger.info(f"🚀 Starting reverse processing: Page {total_pages} → Page 1")

            # Process pages in reverse order (oldest first)
            for page in range(total_pages, 0, -1):
                logger.info(f"--- Processing Page {page}/{total_pages} ---")

                page_url = (
                    f"https://www.zohoapis.com/inventory/v1/shipmentorders?"
                    f"page={page}&"
                    f"per_page=200&"
                    f"organization_id={org_id}"
                )

                page_data = await api_client.make_request(page_url)
                if not page_data:
                    logger.warning(f"No data returned for page {page}")
                    continue

                # Get shipments from response
                page_shipments = page_data.get("shipmentorders") or page_data.get("shipment_orders")
                if not page_shipments:
                    logger.warning(f"No shipments found for page {page}")
                    continue

                logger.info(f"Page {page}: Found {len(page_shipments)} shipments")

                # Filter new shipments
                new_shipment_ids = []
                existing_count = 0

                for shipment in page_shipments:
                    ship_id = str(
                        shipment.get("shipment_order_id") or
                        shipment.get("shipmentorder_id") or
                        shipment.get("shipment_id") or
                        ""
                    )
                    if not ship_id:
                        continue

                    total_processed += 1
                    existing_doc = collection.find_one({"shipment_id": ship_id}, {"_id": 1})
                    if not existing_doc:
                        new_shipment_ids.append(ship_id)
                    else:
                        existing_count += 1

                logger.info(f"Page {page}: {len(new_shipment_ids)} new, {existing_count} existing")

                if new_shipment_ids:
                    # Fetch details for new shipments
                    logger.info(f"Fetching details for {len(new_shipment_ids)} shipments...")

                    shipments_to_insert = []

                    # Process in batches
                    batch_size = 5
                    for i in range(0, len(new_shipment_ids), batch_size):
                        batch_ids = new_shipment_ids[i:i + batch_size]

                        detail_tasks = []
                        for ship_id in batch_ids:
                            detail_url = (
                                f"https://www.zohoapis.com/inventory/v1/shipmentorders/{ship_id}?"
                                f"organization_id={org_id}"
                            )
                            detail_tasks.append(api_client.make_request(detail_url))

                        results = await asyncio.gather(*detail_tasks, return_exceptions=True)

                        for j, result in enumerate(results):
                            try:
                                if isinstance(result, Exception):
                                    logger.error(f"Error fetching shipment {batch_ids[j]}: {result}")
                                    total_errors += 1
                                    continue

                                shipment_detail = None
                                if result:
                                    shipment_detail = result.get("shipment_order") or result.get("shipmentorder")

                                if shipment_detail:
                                    processed = process_shipment_data(shipment_detail)
                                    shipments_to_insert.append(processed)
                                else:
                                    total_errors += 1
                            except Exception as e:
                                logger.error(f"Error processing shipment: {e}")
                                total_errors += 1

                    # Bulk insert
                    if shipments_to_insert:
                        try:
                            result = collection.insert_many(shipments_to_insert, ordered=False)
                            inserted = len(result.inserted_ids)
                            total_inserted += inserted
                            logger.info(f"✅ Inserted {inserted} shipments from page {page}")
                        except Exception as e:
                            logger.error(f"Error during bulk insert for page {page}: {e}")
                            # Fallback to individual inserts
                            for doc in shipments_to_insert:
                                try:
                                    collection.insert_one(doc)
                                    total_inserted += 1
                                except Exception as e2:
                                    logger.error(f"Failed to insert shipment: {e2}")
                                    total_errors += 1

                # Brief pause between pages
                if page > 1:
                    await asyncio.sleep(1)

            duration = time.time() - start_time

            logger.info(f"🎉 Shipments sync completed!")
            logger.info(f"📊 Summary: Processed {total_processed}, Inserted {total_inserted}, Errors {total_errors}")

            send_slack_notification(
                "Shipments Cron",
                success=True,
                details={
                    "processed": total_processed,
                    "inserted": total_inserted,
                    "pages": total_pages,
                    "duration": duration,
                },
            )

    except Exception as e:
        error_msg = f"Error in shipments sync: {e}"
        logger.error(error_msg)
        send_slack_notification("Shipments Cron Error", success=False, error_msg=str(e))


async def stock_cron():
    """Cron job for syncing warehouse stock data daily."""
    logger.info("🚀 Starting daily warehouse stock sync...")
    start_time = time.time()
    try:
        _, db = connect_to_mongo()
        zoho_stock_collection = db["zoho_stock"]
        products_collection = db["products"]

        # Get yesterday's date
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        target_date = datetime.strptime(yesterday, "%Y-%m-%d")

        # Check if we already have data for this date
        existing_count = zoho_stock_collection.count_documents({"date": target_date})
        if existing_count > 0:
            logger.info(
                f"Warehouse stock data for {yesterday} already exists ({existing_count} records)"
            )
            return

        async with ZohoAPIClient("inventory") as api_client:
            if not api_client.access_token:
                logger.error("Failed to get access token")
                return

            # Fetch the first page to determine total pages
            warehouse_url = (
                f"https://inventory.zoho.com/api/v1/reports/warehouse?"
                f"page=1&per_page=2000&sort_column=item_name&sort_order=A&"
                f"response_option=1&filter_by=TransactionDate.CustomDate&"
                f"show_actual_stock=false&to_date={yesterday}&organization_id={org_id}"
            )

            data = await api_client.make_request(warehouse_url)
            if not data or "warehouse_stock_info" not in data:
                logger.info(f"No warehouse stock data found for {yesterday}")
                return

            warehouse_stock = data["warehouse_stock_info"]
            total_pages = data.get("page_context", {}).get("total_pages", 1)

            # Fetch remaining pages if there are more than 1 page
            if total_pages > 1:
                logger.info(f"Fetching {total_pages} pages for {yesterday}")
                
                for page in range(2, total_pages + 1):
                    page_url = (
                        f"https://inventory.zoho.com/api/v1/reports/warehouse?"
                        f"page={page}&per_page=2000&sort_column=item_name&sort_order=A&"
                        f"response_option=1&filter_by=TransactionDate.CustomDate&"
                        f"show_actual_stock=false&to_date={yesterday}&organization_id={org_id}"
                    )
                    
                    page_data = await api_client.make_request(page_url)
                    if page_data and "warehouse_stock_info" in page_data:
                        warehouse_stock.extend(page_data["warehouse_stock_info"])

            logger.info(f"Processing {len(warehouse_stock)} warehouse stock items for {yesterday}")
            processed_data = []

            for item in warehouse_stock:
                if not isinstance(item, dict):
                    continue

                pupscribe_stock = None
                item_name = None
                item_id = None
                # Handle NEW API structure (warehouse_stock array)
                if "warehouse_stock" in item:
                    for warehouse_entry in item["warehouse_stock"]:
                        if warehouse_entry.get("warehouse_name") == "Pupscribe Enterprises Private Limited":
                            pupscribe_stock = int(warehouse_entry.get("quantity_available_for_sale", 0))
                            item_name = warehouse_entry.get("item_name", "")
                            item_id = warehouse_entry.get("item_id", "")
                            break  # Found the warehouse we want, no need to check others
                
                # Handle OLD API structure (backwards compatibility)
                elif "warehouses" in item:
                    item_name = item.get("item_name", "")
                    for warehouse in item.get("warehouses", []):
                        if warehouse.get("warehouse_name") == "Pupscribe Enterprises Private Limited":
                            pupscribe_stock = int(warehouse.get("quantity_available_for_sale", 0))
                            item_id = warehouse.get("item_id", "")
                            break
                
                # Fallback: Direct warehouse entry (old structure)
                elif item.get("warehouse_name") == "Pupscribe Enterprises Private Limited":
                    pupscribe_stock = int(item.get("quantity_available_for_sale", 0))
                    item_name = item.get("item_name", "")
                    item_id = item.get("item_id", "")

                # Skip if we couldn't find Pupscribe stock or item name
                if pupscribe_stock is None or not item_name:
                    continue

                # Find matching product ID
                product_id = find_product_id_with_mongo(item_name, products_collection)

                # Prepare document for MongoDB
                stock_document = {
                    "item_name": item_name,
                    "stock": pupscribe_stock,
                    "date": target_date,
                    "product_id": product_id,
                    "created_at": datetime.now(),
                    "zoho_item_id": item_id,
                }

                processed_data.append(stock_document)

            if processed_data:
                zoho_stock_collection.insert_many(processed_data, ordered=False)
                logger.info(
                    f"✅ Inserted {len(processed_data)} warehouse stock records for {yesterday}"
                )
            else:
                logger.info(f"No Pupscribe warehouse stock found for {yesterday}")

            duration = time.time() - start_time
            send_slack_notification(
                "Stock Cron",
                success=True,
                details={
                    "processed": len(processed_data),
                    "duration": duration,
                    "pages_fetched": total_pages,
                    "total_items": len(warehouse_stock),
                },
            )
    except Exception as e:
        logger.error(f"Error in warehouse stock sync: {e}")
        send_slack_notification("Stock Cron Error", success=False, error_msg=str(e))

def setup_cron_jobs(scheduler_instance: AsyncIOScheduler):
    """Setup all cron jobs with the provided scheduler."""
    try:
        # Clear existing jobs to avoid duplicates
        scheduler_instance.remove_all_jobs()
        
        # Add jobs with timezone awareness
        scheduler_instance.add_job(
            invoices_cron,
            "cron",
            hour=15,
            minute=40,
            id="invoices_cron",
            replace_existing=True,
            misfire_grace_time=300  # 5 minutes grace period
        )

        scheduler_instance.add_job(
            credit_notes_cron,
            "cron",
            hour=15,
            minute=0,
            id="credit_notes_cron",
            replace_existing=True,
            misfire_grace_time=300
        )

        scheduler_instance.add_job(
            stock_cron,
            "cron",
            hour=15,
            minute=30,
            id="stock_cron",
            replace_existing=True,
            misfire_grace_time=300
        )

        scheduler_instance.add_job(
            shipments_cron,
            "cron",
            hour=16,
            minute=0,
            id="shipments_cron",
            replace_existing=True,
            misfire_grace_time=300
        )

        logger.info(f"✅ {len(scheduler_instance.get_jobs())} cron jobs set up successfully")
        
        # Log next run times for debugging
        for job in scheduler_instance.get_jobs():
            logger.info(f"📅 Job '{job.id}' next run: {job.next_run_time}")
            
    except Exception as e:
        logger.error(f"❌ Error setting up cron jobs: {e}")
        raise

jobstores = {
    "default": {
        "type": "mongodb",
        "host": os.getenv("MONGO_URI"),
        "port": 27017,
        "database": os.getenv("DB_NAME"),
        "collection": "cron_jobs",
    }
}

scheduler = AsyncIOScheduler(jobstores=jobstores)


def _job_event_listener(event):
    if event.exception:
        logging.error(f"Job {event.job_id} raised an exception!")
    else:
        logging.info(f"Job {event.job_id} completed successfully!")


def cron_startup():
    logging.info("Starting Cron Scheduler...")
    scheduler.start()
    setup_cron_jobs(scheduler)
    scheduler.add_listener(_job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    logging.info("Scheduler started.")


def cron_shutdown():
    logging.info("Shutting down Cron Scheduler...")
    scheduler.shutdown()



