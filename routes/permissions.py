from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional
import jwt
from pymongo import MongoClient
from functools import lru_cache
from config.root import connect_to_mongo
from config.auth import JWT_SECRET_KEY
from pydantic import BaseModel
from bson import ObjectId

client, db = connect_to_mongo()
permissions_collection = db.get_collection("permissions")
users_collection = db.get_collection("users")

# FastAPI router and security
router = APIRouter(tags=["permissions"])
security = HTTPBearer()


class UserUpdateModel(BaseModel):
    name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    status: Optional[str] = None


class PermissionService:
    """Service class to handle all permission-related operations"""

    def __init__(self):
        self.permissions_collection = permissions_collection
        self.users_collection = users_collection

    def get_user_menu_items(self, user_roles: List[str]) -> List[dict]:
        """Get menu items that user has access to"""
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

    def get_all_users(self) -> List[dict]:
        """Get all users (admin and sales_admin only)"""
        try:
            users_docs = list(self.users_collection.find({}).sort("name", 1))
            # Convert ObjectId to string and add permissions for each user
            for doc in users_docs:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                # Get user permissions based on role
                user_role = doc.get('role', '')
                user_roles = [user_role] if user_role else []
                doc['permissions'] = self.get_user_menu_items(user_roles)
                
            return users_docs
        except Exception as e:
            print(f"Error fetching all users: {e}")
            return []

    def update_user(self, user_id: str, user_data: dict, current_user_role: str, current_user_id: str) -> bool:
        """Update user with role-based restrictions"""
        try:
            # Get the target user
            target_user = self.users_collection.find_one({"_id": ObjectId(user_id)})
            if not target_user:
                return False

            target_user_role = target_user.get('role', '')
            
            # Role-based access control
            if current_user_role == 'admin':
                # Admins can only edit themselves, not other admins
                if target_user_role == 'admin' and str(target_user['_id']) != current_user_id:
                    return False
            elif current_user_role == 'sales_admin':
                # Sales_admin can edit everyone except admins
                if target_user_role == 'admin':
                    return False
            else:
                # Other roles cannot edit users
                return False

            # Remove None values from update data
            filtered_data = {k: v for k, v in user_data.items() if v is not None}
            
            if not filtered_data:
                return False

            result = self.users_collection.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": filtered_data}
            )
            return result.matched_count > 0
        except Exception as e:
            print(f"Error updating user: {e}")
            return False

    def can_edit_user(self, target_user_id: str, current_user_role: str, current_user_id: str) -> dict:
        """Check if current user can edit target user"""
        try:
            target_user = self.users_collection.find_one({"_id": ObjectId(target_user_id)})
            if not target_user:
                return {"can_edit": False, "reason": "User not found"}

            target_user_role = target_user.get('role', '')
            
            if current_user_role == 'admin':
                # Admins can only edit themselves, not other admins
                if target_user_role == 'admin' and str(target_user['_id']) != current_user_id:
                    return {"can_edit": False, "reason": "Admins cannot edit other admins"}
                return {"can_edit": True, "reason": "Admin can edit this user"}
            elif current_user_role == 'sales_admin':
                # Sales_admin can edit everyone except admins
                if target_user_role == 'admin':
                    return {"can_edit": False, "reason": "Sales admin cannot edit admins"}
                return {"can_edit": True, "reason": "Sales admin can edit this user"}
            else:
                return {"can_edit": False, "reason": "Insufficient permissions"}
                
        except Exception as e:
            print(f"Error checking edit permissions: {e}")
            return {"can_edit": False, "reason": "Error checking permissions"}


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


def require_admin_or_sales_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to require admin or sales_admin role"""
    payload = decode_token(credentials)
    payload_data = payload.get("data", {})
    user_roles = payload_data.get("role", [])
    
    # Handle both string and list roles
    if isinstance(user_roles, str):
        user_roles = [user_roles]

    if "admin" not in user_roles and "sales_admin" not in user_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Admin or Sales Admin access required"
        )
    return payload


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


# New endpoints for user management

@router.get("/users")
def get_all_users(_ = Depends(require_admin_or_sales_admin)):
    """Get all users with their permissions (admin and sales_admin only)"""
    users = permission_service.get_all_users()
    return {"users": users}


@router.put("/users/{user_id}")
def update_user(
    user_id: str,
    user_data: UserUpdateModel,
    payload = Depends(require_admin_or_sales_admin)
):
    """Update user information with role-based restrictions"""
    payload_data = payload.get("data", {})
    current_user_role = payload_data.get("role", "")
    current_user_id = str(payload.get("user_id", ""))
    
    # Handle both string and list roles
    if isinstance(current_user_role, list):
        current_user_role = current_user_role[0] if current_user_role else ""
    
    # Convert Pydantic model to dict
    update_data = user_data.dict()
    
    success = permission_service.update_user(
        user_id, update_data, current_user_role, current_user_id
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot update this user due to role restrictions"
        )
    
    return {"message": "User updated successfully"}


@router.get("/users/{user_id}/edit-permissions")
def check_user_edit_permissions(
    user_id: str,
    payload = Depends(require_admin_or_sales_admin)
):
    """Check if current user can edit target user"""
    payload_data = payload.get("data", {})
    current_user_role = payload_data.get("role", "")
    current_user_id = str(payload.get("user_id", ""))
    
    # Handle both string and list roles
    if isinstance(current_user_role, list):
        current_user_role = current_user_role[0] if current_user_role else ""
    
    result = permission_service.can_edit_user(user_id, current_user_role, current_user_id)
    return result