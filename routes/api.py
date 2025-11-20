from fastapi import APIRouter, Depends
from .users import router as users
from .customers import router as customers
from .products import router as products
from .zoho import router as zoho
from .orders import router as orders
from .util import router as util
from .admin import router as admin
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
from config.auth import JWTBearer 

router = APIRouter()

router.include_router(users, prefix="/users", tags=["User"])
router.include_router(daily_visits, prefix="/daily_visits", tags=["Daily Visits"])
router.include_router(hooks, prefix="/hooks", tags=["Hooks"])
router.include_router(catalogues, prefix="/catalogues", tags=["Catalogues"])
router.include_router(trainings, prefix="/trainings", tags=["Trainings"])
router.include_router(announcements, prefix="/announcements", tags=["Announcements"])
router.include_router(customers, prefix="/customers", tags=["Customer"])
router.include_router(products, prefix="/products", tags=["Product"])
router.include_router(zoho, prefix="/zoho", tags=["Zoho"])
router.include_router(orders, prefix="/orders", tags=["Orders"])
router.include_router(
    admin, prefix="/admin", tags=["Admin"], dependencies=[Depends(JWTBearer())]
)
router.include_router(util, prefix="/util", tags=["Util"])
router.include_router(invoices, prefix="/invoices", tags=["Invoice"])
router.include_router(shipments, prefix="/shipments", tags=["Shipments"])
router.include_router(webhooks, prefix="/zoho/webhooks", tags=["Zoho"])
router.include_router(attendence, prefix="/attendance", tags=["Attendance"])
router.include_router(
    potential_customers, prefix="/potential_customers", tags=["Potential Customers"]
)
router.include_router(
    targeted_customers, prefix="/targeted_customers", tags=["Targeted Customers"]
)
router.include_router(
    expected_reorders, prefix="/expected_reorders", tags=["Expected Reorders"]
)
router.include_router(return_orders, prefix="/return_orders", tags=["Return Reorders"])

router.include_router(external_links, prefix="/external_links", tags=["External Links"])

router.include_router(attendance, prefix="/attendance", tags=["Attendance"])

router.include_router(customer_analytics, prefix="/customer_analytics", tags=["Customer Analytics"])

router.include_router(permissions_router, prefix="/permissions", tags=["Permissions"])


@router.get("/")
def hello_world():
    return "Application is Running"
