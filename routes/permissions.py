from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Optional
import jwt
import pymongo
from pymongo import MongoClient
import os
from functools import lru_cache
from config.root import connect_to_mongo
from config.auth import JWT_SECRET_KEY

client, db = connect_to_mongo()
permissions_collection = db.get_collection("permissions")
# FastAPI router and security
router = APIRouter(tags=["permissions"])
security = HTTPBearer()


class PermissionService:
    """Service class to handle all permission-related operations"""

    def __init__(self):
        self.permissions_collection = permissions_collection

    def get_user_menu_items(self, user_roles: List[str]) -> List[dict]:
        """Get menu items that user has access to"""
        try:
            print(f"Fetching menu items for roles: {user_roles}")
            
            # Find all active permissions where user has access
            permissions_docs = list(
                self.permissions_collection.find({
                    "is_active": True,
                    "allowed_roles": {"$in": user_roles}
                })
            )

            print(f"Found {len(permissions_docs)} matching permissions")

            if not permissions_docs:
                return []

            # Sort by order
            permissions_docs.sort(key=lambda x: x.get("order", 0))
            
            # Remove MongoDB ObjectId for JSON serialization
            for doc in permissions_docs:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return permissions_docs

        except Exception as e:
            print(f"Error fetching menu items: {e}")
            return []

    def get_user_dashboard_sections(self, user_roles: List[str]) -> List[str]:
        """Get dashboard sections derived from menu items"""
        try:
            # Find all active permissions where user has access
            permissions_docs = list(
                self.permissions_collection.find({
                    "is_active": True,
                    "allowed_roles": {"$in": user_roles}
                })
            )

            if not permissions_docs:
                return []

            # Extract routes for dashboard
            allowed_sections = []
            for item in permissions_docs:
                # Extract route from path (remove /admin/ prefix)
                path = item.get("path", "")
                if path.startswith("/admin/"):
                    route = path.replace("/admin/", "")
                elif path == "/admin":
                    route = "dashboard"
                else:
                    route = item.get("id")  # fallback to id

                allowed_sections.append(route)

            return allowed_sections

        except Exception as e:
            print(f"Error fetching dashboard sections: {e}")
            return []

    def can_access_route(self, user_roles: List[str], route_path: str) -> bool:
        """Check if user can access specific route"""
        try:
            # Check if there's a permission document that matches the route and user roles
            permission_doc = self.permissions_collection.find_one({
                "path": route_path,
                "is_active": True,
                "allowed_roles": {"$in": user_roles}
            })

            return permission_doc is not None

        except Exception as e:
            print(f"Error checking route access: {e}")
            return False

    def get_all_menu_items(self) -> List[dict]:
        """Get all menu items (admin only)"""
        try:
            permissions_docs = list(self.permissions_collection.find({}))
            # Convert ObjectId to string
            for doc in permissions_docs:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            return permissions_docs
        except Exception as e:
            print(f"Error fetching all menu items: {e}")
            return []

    def update_menu_item(self, item_id: str, item_data: dict) -> bool:
        """Update a menu item (admin only)"""
        try:
            result = self.permissions_collection.update_one(
                {"id": item_id},
                {"$set": item_data}
            )
            return result.matched_count > 0
        except Exception as e:
            print(f"Error updating menu item: {e}")
            return False

    def add_menu_item(self, item_data: dict) -> bool:
        """Add a new menu item"""
        try:
            result = self.permissions_collection.insert_one(item_data)
            return result.inserted_id is not None
        except Exception as e:
            print(f"Error adding menu item: {e}")
            return False

    def delete_menu_item(self, item_id: str) -> bool:
        """Delete a menu item"""
        try:
            result = self.permissions_collection.delete_one({"id": item_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting menu item: {e}")
            return False


# Create service instance
permission_service = PermissionService()


def decode_token(credentials: HTTPAuthorizationCredentials):
    """Decode and validate JWT token"""
    try:
        payload = jwt.decode(
            credentials.credentials, JWT_SECRET_KEY, algorithms=["HS256"]
        )
        return payload
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


def require_admin_role(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to require admin role"""
    payload = decode_token(credentials)
    payload_data = payload.get("data", {})
    user_roles = payload_data.get("role", [])
    
    # Handle both string and list roles
    if isinstance(user_roles, str):
        user_roles = [user_roles]

    if "admin" not in user_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required"
        )
    return payload


# API Routes


@router.get("/menu-items")
def get_user_menu_items(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get menu items that current user has access to"""
    payload = decode_token(credentials)
    payload_data = payload.get("data", {})
    user_roles = payload_data.get("role", [])
    
    # Handle both string and list roles
    if isinstance(user_roles, str):
        user_roles = [user_roles]

    menu_items = permission_service.get_user_menu_items(user_roles)
    print("/menu-items", f"Found {len(menu_items)} items for roles: {user_roles}")
    return {"menu_items": menu_items}


@router.get("/dashboard-sections")
def get_user_dashboard_sections(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get dashboard sections derived from menu items"""
    payload = decode_token(credentials)
    payload_data = payload.get("data", {})
    user_roles = payload_data.get("role", [])
    
    # Handle both string and list roles
    if isinstance(user_roles, str):
        user_roles = [user_roles]
    
    sections = permission_service.get_user_dashboard_sections(user_roles)
    print("/dashboard-sections", f"Found {len(sections)} sections for roles: {user_roles}")
    return {"dashboard_sections": sections}


@router.post("/check-route-access")
def check_route_access(
    route_data: dict, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Check if user can access specific route"""
    payload = decode_token(credentials)
    payload_data = payload.get("data", {})
    user_roles = payload_data.get("role", [])
    route_path = route_data.get("route_path")
    
    # Handle both string and list roles
    if isinstance(user_roles, str):
        user_roles = [user_roles]
    
    print(f"Checking access for route: {route_path}, roles: {user_roles}")
    
    if not route_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="route_path is required"
        )

    can_access = permission_service.can_access_route(user_roles, route_path)
    print("/check-route-access", f"Access result: {can_access}")
    return {"can_access": can_access}


@router.get("/user-permissions")
def get_user_permissions(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get all permissions for current user"""
    payload = decode_token(credentials)
    payload_data = payload.get("data", {})
    user_roles = payload_data.get("role", [])
    
    # Handle both string and list roles
    if isinstance(user_roles, str):
        user_roles = [user_roles]
    
    menu_items = permission_service.get_user_menu_items(user_roles)
    dashboard_sections = permission_service.get_user_dashboard_sections(user_roles)

    return {
        "user_id": str(payload.get("user_id")),
        "roles": user_roles,
        "menu_items": menu_items,
        "dashboard_sections": dashboard_sections,
    }


