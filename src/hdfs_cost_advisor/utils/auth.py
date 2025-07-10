import hashlib
import hmac
import secrets
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import jwt
import logging

logger = logging.getLogger(__name__)

class AuthType(Enum):
    NONE = "none"
    API_KEY = "api_key"
    JWT = "jwt"
    KERBEROS = "kerberos"

@dataclass
class AuthConfig:
    auth_type: AuthType = AuthType.NONE
    secret_key: Optional[str] = None
    api_keys: Dict[str, str] = None  # key_id -> key_value
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24
    kerberos_principal: Optional[str] = None
    kerberos_keytab: Optional[str] = None

class AuthenticationError(Exception):
    """Authentication failed"""
    pass

class AuthorizationError(Exception):
    """Authorization failed"""
    pass

class AuthManager:
    def __init__(self, config: AuthConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize based on auth type
        if config.auth_type == AuthType.API_KEY:
            self._init_api_key_auth()
        elif config.auth_type == AuthType.JWT:
            self._init_jwt_auth()
        elif config.auth_type == AuthType.KERBEROS:
            self._init_kerberos_auth()
    
    def _init_api_key_auth(self):
        """Initialize API key authentication"""
        if not self.config.api_keys:
            # Generate a default API key for development
            default_key = self._generate_api_key()
            self.config.api_keys = {"default": default_key}
            self.logger.warning(f"Generated default API key: {default_key}")
        
        self.logger.info("API key authentication initialized")
    
    def _init_jwt_auth(self):
        """Initialize JWT authentication"""
        if not self.config.secret_key:
            raise ValueError("JWT secret key is required")
        
        self.logger.info("JWT authentication initialized")
    
    def _init_kerberos_auth(self):
        """Initialize Kerberos authentication"""
        if not self.config.kerberos_principal:
            raise ValueError("Kerberos principal is required")
        
        # Note: Full Kerberos implementation would require additional setup
        self.logger.info("Kerberos authentication initialized (placeholder)")
    
    def authenticate(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Authenticate user credentials"""
        
        if self.config.auth_type == AuthType.NONE:
            return {"authenticated": True, "user": "anonymous", "roles": ["user"]}
        
        elif self.config.auth_type == AuthType.API_KEY:
            return self._authenticate_api_key(credentials)
        
        elif self.config.auth_type == AuthType.JWT:
            return self._authenticate_jwt(credentials)
        
        elif self.config.auth_type == AuthType.KERBEROS:
            return self._authenticate_kerberos(credentials)
        
        else:
            raise AuthenticationError(f"Unsupported authentication type: {self.config.auth_type}")
    
    def _authenticate_api_key(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Authenticate using API key"""
        api_key = credentials.get("api_key")
        if not api_key:
            raise AuthenticationError("API key is required")
        
        # Find matching API key
        for key_id, key_value in self.config.api_keys.items():
            if self._verify_api_key(api_key, key_value):
                return {
                    "authenticated": True,
                    "user": key_id,
                    "roles": ["user"],
                    "auth_method": "api_key"
                }
        
        raise AuthenticationError("Invalid API key")
    
    def _authenticate_jwt(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Authenticate using JWT token"""
        token = credentials.get("token")
        if not token:
            raise AuthenticationError("JWT token is required")
        
        try:
            payload = jwt.decode(
                token,
                self.config.secret_key,
                algorithms=[self.config.jwt_algorithm]
            )
            
            # Check expiration
            if payload.get("exp", 0) < time.time():
                raise AuthenticationError("Token expired")
            
            return {
                "authenticated": True,
                "user": payload.get("user", "unknown"),
                "roles": payload.get("roles", ["user"]),
                "auth_method": "jwt",
                "token_payload": payload
            }
            
        except jwt.InvalidTokenError as e:
            raise AuthenticationError(f"Invalid JWT token: {str(e)}")
    
    def _authenticate_kerberos(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Authenticate using Kerberos (placeholder implementation)"""
        # This would require integration with Kerberos libraries
        # For now, return a placeholder response
        principal = credentials.get("principal")
        if not principal:
            raise AuthenticationError("Kerberos principal is required")
        
        # Placeholder validation
        if principal == self.config.kerberos_principal:
            return {
                "authenticated": True,
                "user": principal,
                "roles": ["user"],
                "auth_method": "kerberos"
            }
        
        raise AuthenticationError("Kerberos authentication failed")
    
    def authorize(self, user_info: Dict[str, Any], required_role: str = "user") -> bool:
        """Check if user has required authorization"""
        
        if not user_info.get("authenticated", False):
            return False
        
        user_roles = user_info.get("roles", [])
        
        # Simple role hierarchy
        role_hierarchy = {
            "user": 1,
            "admin": 2,
            "superuser": 3
        }
        
        required_level = role_hierarchy.get(required_role, 1)
        user_level = max(role_hierarchy.get(role, 0) for role in user_roles)
        
        return user_level >= required_level
    
    def generate_jwt_token(self, user: str, roles: list = None) -> str:
        """Generate JWT token for user"""
        if self.config.auth_type != AuthType.JWT:
            raise ValueError("JWT authentication is not enabled")
        
        if roles is None:
            roles = ["user"]
        
        payload = {
            "user": user,
            "roles": roles,
            "iat": time.time(),
            "exp": time.time() + (self.config.jwt_expiration_hours * 3600)
        }
        
        return jwt.encode(payload, self.config.secret_key, algorithm=self.config.jwt_algorithm)
    
    def _generate_api_key(self) -> str:
        """Generate a new API key"""
        return secrets.token_urlsafe(32)
    
    def _verify_api_key(self, provided_key: str, stored_key: str) -> bool:
        """Verify API key using constant-time comparison"""
        return hmac.compare_digest(provided_key, stored_key)
    
    def create_api_key(self, key_id: str) -> str:
        """Create a new API key"""
        if self.config.auth_type != AuthType.API_KEY:
            raise ValueError("API key authentication is not enabled")
        
        new_key = self._generate_api_key()
        self.config.api_keys[key_id] = new_key
        
        self.logger.info(f"Created new API key for: {key_id}")
        return new_key
    
    def revoke_api_key(self, key_id: str) -> bool:
        """Revoke an API key"""
        if self.config.auth_type != AuthType.API_KEY:
            raise ValueError("API key authentication is not enabled")
        
        if key_id in self.config.api_keys:
            del self.config.api_keys[key_id]
            self.logger.info(f"Revoked API key for: {key_id}")
            return True
        
        return False
    
    def list_api_keys(self) -> list:
        """List all API key IDs"""
        if self.config.auth_type != AuthType.API_KEY:
            raise ValueError("API key authentication is not enabled")
        
        return list(self.config.api_keys.keys())
    
    def get_auth_info(self) -> Dict[str, Any]:
        """Get authentication configuration info"""
        return {
            "auth_type": self.config.auth_type.value,
            "jwt_algorithm": self.config.jwt_algorithm if self.config.auth_type == AuthType.JWT else None,
            "jwt_expiration_hours": self.config.jwt_expiration_hours if self.config.auth_type == AuthType.JWT else None,
            "api_keys_count": len(self.config.api_keys) if self.config.api_keys else 0,
            "kerberos_principal": self.config.kerberos_principal if self.config.auth_type == AuthType.KERBEROS else None
        }

# Decorator for authentication
def require_auth(auth_manager: AuthManager, required_role: str = "user"):
    """Decorator to require authentication for functions"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Extract credentials from kwargs or args
            credentials = kwargs.get("credentials") or (args[0] if args else {})
            
            try:
                user_info = auth_manager.authenticate(credentials)
                
                if not auth_manager.authorize(user_info, required_role):
                    raise AuthorizationError(f"Insufficient privileges. Required role: {required_role}")
                
                # Add user info to kwargs
                kwargs["user_info"] = user_info
                
                return func(*args, **kwargs)
                
            except (AuthenticationError, AuthorizationError) as e:
                logger.error(f"Authentication/Authorization failed: {e}")
                raise
            
        return wrapper
    return decorator

# Utility functions
def hash_password(password: str, salt: Optional[str] = None) -> tuple:
    """Hash password with salt"""
    if salt is None:
        salt = secrets.token_hex(16)
    
    pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000)
    return pwd_hash.hex(), salt

def verify_password(password: str, hash_value: str, salt: str) -> bool:
    """Verify password against hash"""
    pwd_hash, _ = hash_password(password, salt)
    return hmac.compare_digest(pwd_hash, hash_value)

def generate_secure_token(length: int = 32) -> str:
    """Generate a secure random token"""
    return secrets.token_urlsafe(length)

# Example usage and testing
if __name__ == "__main__":
    # Example configuration
    config = AuthConfig(
        auth_type=AuthType.API_KEY,
        api_keys={"test_user": "test_key_123"}
    )
    
    auth_manager = AuthManager(config)
    
    # Test authentication
    try:
        user_info = auth_manager.authenticate({"api_key": "test_key_123"})
        print(f"Authentication successful: {user_info}")
        
        # Test authorization
        if auth_manager.authorize(user_info, "user"):
            print("Authorization successful")
        else:
            print("Authorization failed")
            
    except AuthenticationError as e:
        print(f"Authentication failed: {e}")
    except AuthorizationError as e:
        print(f"Authorization failed: {e}")