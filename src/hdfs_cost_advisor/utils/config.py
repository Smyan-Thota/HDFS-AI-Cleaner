from pydantic import BaseModel
from pydantic_settings import BaseSettings
from typing import Optional
import os
from ..hdfs.client import HDFSConfig
from ..llm.client import LLMProvider
from ..cost.calculator import StorageCosts

class LLMConfig(BaseModel):
    provider: LLMProvider = LLMProvider.ANTHROPIC
    api_key: str
    model_name: Optional[str] = None
    max_tokens: int = 3000
    temperature: float = 0.3

class CostConfig(BaseModel):
    storage_costs: StorageCosts = StorageCosts()

class Settings(BaseSettings):
    # Server configuration
    server_host: str = "localhost"
    server_port: int = 8000
    log_level: str = "INFO"
    
    # HDFS configuration
    hdfs_host: str = "localhost"
    hdfs_port: int = 9000
    hdfs_user: str = "hadoop"
    hdfs_auth_type: str = "simple"
    hdfs_namenode_web_port: int = 9870
    
    # LLM configuration
    llm_provider: str = "anthropic"
    llm_api_key: str
    llm_model_name: Optional[str] = None
    llm_max_tokens: int = 3000
    llm_temperature: float = 0.3
    
    # Cost configuration
    standard_storage_cost_per_gb: float = 0.04
    cold_storage_cost_per_gb: float = 0.01
    archive_storage_cost_per_gb: float = 0.005
    metadata_cost_per_file: float = 0.0001
    network_cost_per_gb: float = 0.01
    
    # Redis configuration (optional)
    redis_url: Optional[str] = None
    redis_enabled: bool = False
    
    # Security configuration
    enable_auth: bool = False
    auth_secret_key: Optional[str] = None
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
        # Environment variable prefixes
        env_prefix = ""
        
        # Allow custom environment variable names
        fields = {
            "hdfs_host": {"env": ["HDFS_HOST", "hdfs_host"]},
            "hdfs_port": {"env": ["HDFS_PORT", "hdfs_port"]},
            "hdfs_user": {"env": ["HDFS_USER", "hdfs_user"]},
            "hdfs_auth_type": {"env": ["HDFS_AUTH_TYPE", "hdfs_auth_type"]},
            "hdfs_namenode_web_port": {"env": ["HDFS_NAMENODE_WEB_PORT", "hdfs_namenode_web_port"]},
            "llm_provider": {"env": ["LLM_PROVIDER", "llm_provider"]},
            "llm_api_key": {"env": ["LLM_API_KEY", "llm_api_key"]},
            "llm_model_name": {"env": ["LLM_MODEL_NAME", "llm_model_name"]},
            "llm_max_tokens": {"env": ["LLM_MAX_TOKENS", "llm_max_tokens"]},
            "llm_temperature": {"env": ["LLM_TEMPERATURE", "llm_temperature"]},
            "redis_url": {"env": ["REDIS_URL", "redis_url"]},
            "redis_enabled": {"env": ["REDIS_ENABLED", "redis_enabled"]},
            "enable_auth": {"env": ["ENABLE_AUTH", "enable_auth"]},
            "auth_secret_key": {"env": ["AUTH_SECRET_KEY", "auth_secret_key"]},
            "log_level": {"env": ["LOG_LEVEL", "log_level"]}
        }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Validate configuration
        self._validate_config()
        
        # Set up derived configurations
        self._setup_derived_configs()
    
    def _validate_config(self):
        """Validate configuration settings"""
        if not self.llm_api_key:
            raise ValueError("LLM API key is required")
        
        if self.llm_provider not in ["openai", "anthropic", "google"]:
            raise ValueError(f"Unsupported LLM provider: {self.llm_provider}")
        
        if self.hdfs_port < 1 or self.hdfs_port > 65535:
            raise ValueError("HDFS port must be between 1 and 65535")
        
        if self.hdfs_namenode_web_port < 1 or self.hdfs_namenode_web_port > 65535:
            raise ValueError("HDFS namenode web port must be between 1 and 65535")
        
        if self.enable_auth and not self.auth_secret_key:
            raise ValueError("Auth secret key is required when authentication is enabled")
    
    def _setup_derived_configs(self):
        """Set up derived configuration objects"""
        # HDFS configuration
        self.hdfs = HDFSConfig(
            host=self.hdfs_host,
            port=self.hdfs_port,
            user=self.hdfs_user,
            auth_type=self.hdfs_auth_type,
            namenode_web_port=self.hdfs_namenode_web_port
        )
        
        # LLM configuration
        self.llm = LLMConfig(
            provider=LLMProvider(self.llm_provider),
            api_key=self.llm_api_key,
            model_name=self.llm_model_name,
            max_tokens=self.llm_max_tokens,
            temperature=self.llm_temperature
        )
        
        # Cost configuration
        self.cost = CostConfig(
            storage_costs=StorageCosts(
                standard_storage_cost_per_gb=self.standard_storage_cost_per_gb,
                cold_storage_cost_per_gb=self.cold_storage_cost_per_gb,
                archive_storage_cost_per_gb=self.archive_storage_cost_per_gb,
                metadata_cost_per_file=self.metadata_cost_per_file,
                network_cost_per_gb=self.network_cost_per_gb
            )
        )
    
    @classmethod
    def from_env_file(cls, env_file: str = ".env"):
        """Load configuration from environment file"""
        return cls(_env_file=env_file)
    
    @classmethod
    def from_dict(cls, config_dict: dict):
        """Load configuration from dictionary"""
        return cls(**config_dict)
    
    def to_dict(self) -> dict:
        """Convert configuration to dictionary"""
        return {
            "server": {
                "host": self.server_host,
                "port": self.server_port,
                "log_level": self.log_level
            },
            "hdfs": {
                "host": self.hdfs_host,
                "port": self.hdfs_port,
                "user": self.hdfs_user,
                "auth_type": self.hdfs_auth_type,
                "namenode_web_port": self.hdfs_namenode_web_port
            },
            "llm": {
                "provider": self.llm_provider,
                "api_key": "***REDACTED***",
                "model_name": self.llm_model_name,
                "max_tokens": self.llm_max_tokens,
                "temperature": self.llm_temperature
            },
            "cost": {
                "standard_storage_cost_per_gb": self.standard_storage_cost_per_gb,
                "cold_storage_cost_per_gb": self.cold_storage_cost_per_gb,
                "archive_storage_cost_per_gb": self.archive_storage_cost_per_gb,
                "metadata_cost_per_file": self.metadata_cost_per_file,
                "network_cost_per_gb": self.network_cost_per_gb
            },
            "redis": {
                "enabled": self.redis_enabled,
                "url": self.redis_url
            },
            "security": {
                "enable_auth": self.enable_auth,
                "auth_secret_key": "***REDACTED***" if self.auth_secret_key else None
            }
        }
    
    def get_connection_string(self) -> str:
        """Get HDFS connection string"""
        return f"hdfs://{self.hdfs_host}:{self.hdfs_port}"
    
    def get_namenode_web_url(self) -> str:
        """Get NameNode web UI URL"""
        return f"http://{self.hdfs_host}:{self.hdfs_namenode_web_port}"
    
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return os.getenv("ENVIRONMENT", "development").lower() == "production"
    
    def get_log_config(self) -> dict:
        """Get logging configuration"""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                },
                "detailed": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "level": self.log_level
                },
                "file": {
                    "class": "logging.FileHandler",
                    "filename": "hdfs_cost_advisor.log",
                    "formatter": "detailed",
                    "level": "DEBUG"
                }
            },
            "loggers": {
                "hdfs_cost_advisor": {
                    "handlers": ["console", "file"],
                    "level": self.log_level,
                    "propagate": False
                }
            },
            "root": {
                "handlers": ["console"],
                "level": self.log_level
            }
        }

# Default configuration instance
default_settings = None

def get_settings() -> Settings:
    """Get application settings (singleton pattern)"""
    global default_settings
    if default_settings is None:
        default_settings = Settings()
    return default_settings

def load_settings(config_file: Optional[str] = None) -> Settings:
    """Load settings from configuration file"""
    if config_file:
        return Settings.from_env_file(config_file)
    return Settings()

def validate_settings(settings: Settings) -> bool:
    """Validate settings configuration"""
    try:
        # Test HDFS connection
        from ..hdfs.client import HDFSClient
        hdfs_client = HDFSClient(settings.hdfs)
        
        # Test basic HDFS operations
        hdfs_client.check_path_exists("/")
        
        # Test LLM configuration (basic validation)
        if not settings.llm_api_key:
            raise ValueError("LLM API key is required")
        
        return True
    except Exception as e:
        print(f"Settings validation failed: {e}")
        return False