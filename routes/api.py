from fastapi import APIRouter
from .users import router as users
from .customers import router as customers
from .products import router as products
from .zoho import router as zoho
from .util import router as util

router = APIRouter()

router.include_router(users, prefix="/users", tags=["User"])
router.include_router(customers, prefix="/customers", tags=["Customer"])
router.include_router(products, prefix="/products", tags=["Product"])
router.include_router(zoho, prefix="/zoho", tags=["Zoho"])
router.include_router(util, prefix="/util", tags=["Util"])
