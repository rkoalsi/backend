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
from .webhooks import router as webhooks
from .attendance import router as attendance
from backend.config.auth import JWTBearer  # type: ignore

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
router.include_router(webhooks, prefix="/zoho/webhooks", tags=["Zoho"])
router.include_router(attendance, prefix="/attendance", tags=["Attendance"])


@router.get("/")
def hello_world():
    return "Application is Running"
