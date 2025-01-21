from fastapi import APIRouter
from .users import router as users
from .customers import router as customers
from .products import router as products
from .zoho import router as zoho
from .orders import router as orders
from .util import router as util
from .admin import router as admin
from .salespeople import router as salespeople
from .webhooks import router as webhooks

router = APIRouter()

router.include_router(salespeople, prefix="/salespeoples", tags=["SalesPeople"])
router.include_router(users, prefix="/users", tags=["User"])
router.include_router(customers, prefix="/customers", tags=["Customer"])
router.include_router(products, prefix="/products", tags=["Product"])
router.include_router(zoho, prefix="/zoho", tags=["Zoho"])
router.include_router(orders, prefix="/orders", tags=["Orders"])
router.include_router(admin, prefix="/admin", tags=["Admin"])
router.include_router(util, prefix="/util", tags=["Util"])
router.include_router(webhooks, prefix="/zoho/webhooks", tags=["Zoho"])


@router.get("/")
def hello_world():
    return "Application is Running"
