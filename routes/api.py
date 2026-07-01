from fastapi import APIRouter, Depends
from .users import router as users
from .customers import router as customers
from .products import router as products
from .zoho import router as zoho
from .orders import router as orders
from .util import router as util
from .admin import router as admin
from .admin_blog_posts import router as admin_blog_posts
from .catalogues import router as catalogues
from .trainings import router as trainings
from .daily_visits import router as daily_visits
from .hooks import router as hooks
from .announcements import router as announcements
from .invoices import router as invoices
from .shipments import router as shipments
from .webhooks import router as webhooks
from .attendance import router as attendence
from .potential_customers import router as potential_customers
from .expected_reorders import router as expected_reorders
from .return_orders import router as return_orders
from .targeted_customers import router as targeted_customers
from .attendance import router as attendance
from .external_links import router as external_links
from .customer_analytics import router as customer_analytics
from .permissions import router as permissions_router
from .customer_creation_requests import router as customer_creation_requests
from .catalogue_leads import router as catalogue_leads
from .brand_leads import router as brand_leads
from .customer_portal import router as customer_portal
from .credit_notes import router as credit_notes
from .careers import router as careers
from .contact_submissions import router as contact_submissions
from .blog import router as blog
from .customer_activity import router as customer_activity
from .customer_address_details import router as customer_address_details
from .chats import router as chats
from .notifications import router as notifications
from .expense_estimates import router as expense_estimates
from .admin_expense_estimates import router as admin_expense_estimates
from .cheques import router as cheques
from .salesperson_customer_logins import router as salesperson_customer_logins
from .linktree import router as linktree
from .payments import router as payments
from ..config.auth import JWTBearer

router = APIRouter()

# ── Public routes (no auth required) ─────────────────────────────────────────
# users: login/register/forgot-password are public; individual sensitive
#        endpoints inside users.py use JWTBearer directly
router.include_router(users, prefix="/users", tags=["User"])
# orders: contains shared order-form access (?shared=true) which is intentionally public
router.include_router(orders, prefix="/orders", tags=["Orders"])
# products / catalogues: read-only catalog data exposed to public website
router.include_router(products, prefix="/products", tags=["Product"])
router.include_router(catalogues, prefix="/catalogues", tags=["Catalogues"])
# public-facing lead / submission forms
router.include_router(catalogue_leads, prefix="/catalogue_leads", tags=["Catalogue Leads"])
router.include_router(brand_leads, prefix="/brand_leads", tags=["Brand Leads"])
router.include_router(careers, prefix="/careers", tags=["Careers"])
router.include_router(contact_submissions, prefix="/contact_submissions", tags=["Contact Submissions"])
router.include_router(customer_creation_requests, prefix="/customer_creation_requests", tags=["Customer Creation Requests"])
# public blog
router.include_router(blog, prefix="/blog", tags=["Blog"])
# public link-tree landing page (content managed via /admin/linktree)
router.include_router(linktree, prefix="/linktree", tags=["Link Tree"])
# payments: Razorpay payment links for the (public) order form + webhook callback
router.include_router(payments, prefix="/payments", tags=["Payments"])
# utility helpers (city list etc.) — read-only, non-sensitive
router.include_router(util, prefix="/util", tags=["Util"])
# permissions: each endpoint carries its own HTTPBearer dependency
router.include_router(permissions_router, prefix="/permissions", tags=["Permissions"])
# webhooks: authenticated by Zoho IP / HMAC — NOT by user JWT
router.include_router(webhooks, prefix="/zoho/webhooks", tags=["Zoho"])

# ── Protected routes (require valid JWT) ─────────────────────────────────────
_jwt = [Depends(JWTBearer())]

router.include_router(
    admin, prefix="/admin", tags=["Admin"], dependencies=_jwt
)
router.include_router(
    admin_blog_posts, prefix="/admin/blog", tags=["Admin Blog"], dependencies=_jwt
)
router.include_router(
    customers, prefix="/customers", tags=["Customer"], dependencies=_jwt
)
router.include_router(
    invoices, prefix="/invoices", tags=["Invoice"], dependencies=_jwt
)
router.include_router(
    shipments, prefix="/shipments", tags=["Shipments"], dependencies=_jwt
)
router.include_router(
    daily_visits, prefix="/daily_visits", tags=["Daily Visits"], dependencies=_jwt
)
router.include_router(
    hooks, prefix="/hooks", tags=["Hooks"], dependencies=_jwt
)
router.include_router(
    trainings, prefix="/trainings", tags=["Trainings"], dependencies=_jwt
)
router.include_router(
    announcements, prefix="/announcements", tags=["Announcements"], dependencies=_jwt
)
router.include_router(
    zoho, prefix="/zoho", tags=["Zoho"], dependencies=_jwt
)
router.include_router(
    potential_customers, prefix="/potential_customers", tags=["Potential Customers"], dependencies=_jwt
)
router.include_router(
    targeted_customers, prefix="/targeted_customers", tags=["Targeted Customers"], dependencies=_jwt
)
router.include_router(
    expected_reorders, prefix="/expected_reorders", tags=["Expected Reorders"], dependencies=_jwt
)
router.include_router(
    return_orders, prefix="/return_orders", tags=["Return Reorders"], dependencies=_jwt
)
router.include_router(
    external_links, prefix="/external_links", tags=["External Links"], dependencies=_jwt
)
router.include_router(
    attendance, prefix="/attendance", tags=["Attendance"]
)
router.include_router(
    customer_analytics, prefix="/customer_analytics", tags=["Customer Analytics"], dependencies=_jwt
)
router.include_router(
    customer_portal, prefix="/customer_portal", tags=["Customer Portal"], dependencies=_jwt
)
router.include_router(
    credit_notes, prefix="/credit-notes", tags=["Credit Notes"], dependencies=_jwt
)
router.include_router(
    customer_activity, prefix="/customer_activity", tags=["Customer Activity"], dependencies=_jwt
)
router.include_router(
    customer_address_details, prefix="/customer_address_details", tags=["Customer Address Details"], dependencies=_jwt
)
router.include_router(
    chats, prefix="/chats", tags=["Chats"]
)
router.include_router(
    notifications, prefix="/notifications", tags=["Notifications"], dependencies=_jwt
)
router.include_router(
    expense_estimates, prefix="/expense-estimates", tags=["Expense Estimates"], dependencies=_jwt
)
router.include_router(
    admin_expense_estimates, prefix="/admin/expense-estimates", tags=["Admin Expense Estimates"], dependencies=_jwt
)
router.include_router(
    cheques, prefix="/cheques", tags=["Cheques"], dependencies=_jwt
)
router.include_router(
    salesperson_customer_logins,
    prefix="/salesperson/customer-logins",
    tags=["Salesperson Customer Logins"],
    dependencies=_jwt,
)

@router.get("/")
def hello_world():
    return "Application is Running"
