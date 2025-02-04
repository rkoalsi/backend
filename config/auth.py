# auth.py
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import os
from dotenv import load_dotenv
from jwt.exceptions import PyJWTError

load_dotenv()

# Load your secret key (and optionally the algorithm) from environment variables
JWT_SECRET_KEY = os.getenv("SECRET_KEY")
JWT_ALGORITHM = os.getenv("ALGORITHM")


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(
            JWTBearer, self
        ).__call__(request)
        if credentials:
            if credentials.scheme != "Bearer":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Invalid authentication scheme.",
                )
            if not self.verify_jwt(credentials.credentials):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Invalid or expired token.",
                )
            return (
                credentials.credentials
            )  # Optionally, you could return the decoded payload
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid authorization code.",
            )

    def verify_jwt(self, token: str) -> bool:
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            # Optionally: validate additional fields (e.g., expiration, audience, etc.)
            return True
        except PyJWTError as e:
            print(f"JWT verification failed: {e}")
            return False


# Optionally, a dependency to decode the token and return its payload:
def get_current_user(token: str = Depends(JWTBearer())):
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload  # or construct a user object based on payload data
    except PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
        )
