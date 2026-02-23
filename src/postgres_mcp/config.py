"""Configuration management for postgres-mcp."""

import os


class Config:
    """Configuration settings loaded from environment variables."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        self._load_config()

    def _load_config(self):
        """Load configuration from environment variables."""
        # Maximum allowed page size for queries (default: 500)
        max_page_size_str = os.getenv("POSTGRES_MCP_MAX_PAGE_SIZE", "500")
        try:
            self._max_page_size = int(max_page_size_str)
            if self._max_page_size < 1:
                raise ValueError("POSTGRES_MCP_MAX_PAGE_SIZE must be at least 1")
        except ValueError as e:
            raise ValueError(f"Invalid POSTGRES_MCP_MAX_PAGE_SIZE value '{max_page_size_str}': {e}") from e

        # Maximum payload size in MB (default: 5)
        max_payload_size_mb_str = os.getenv("POSTGRES_MCP_MAX_PAYLOAD_SIZE_MB", "5")
        try:
            self._max_payload_size_mb = int(max_payload_size_mb_str)
            if self._max_payload_size_mb < 1:
                raise ValueError("POSTGRES_MCP_MAX_PAYLOAD_SIZE_MB must be at least 1")
        except ValueError as e:
            raise ValueError(f"Invalid POSTGRES_MCP_MAX_PAYLOAD_SIZE_MB value '{max_payload_size_mb_str}': {e}") from e

        # Default page size for queries (default: 100)
        default_page_size_str = os.getenv("POSTGRES_MCP_DEFAULT_PAGE_SIZE", "100")
        try:
            self._default_page_size = int(default_page_size_str)
            if self._default_page_size < 1:
                raise ValueError("POSTGRES_MCP_DEFAULT_PAGE_SIZE must be at least 1")
            if self._default_page_size > self._max_page_size:
                raise ValueError(
                    f"POSTGRES_MCP_DEFAULT_PAGE_SIZE ({self._default_page_size}) cannot exceed POSTGRES_MCP_MAX_PAGE_SIZE ({self._max_page_size})"
                )
        except ValueError as e:
            raise ValueError(f"Invalid POSTGRES_MCP_DEFAULT_PAGE_SIZE value '{default_page_size_str}': {e}") from e
        
        allowed_hosts_str = os.getenv("POSTGRES_MCP_ALLOWED_HOSTS", "localhost,127.0.0.1")
        try:
            self._allowed_hosts = [host.strip() for host in allowed_hosts_str.split(",")]
        except Exception as e:
            raise ValueError(f"Invalid POSTGRES_MCP_ALLOWED_HOSTS value '{allowed_hosts_str}': {e}") from e

    @property
    def max_page_size(self) -> int:
        """Get the maximum allowed page size for queries."""
        return self._max_page_size

    @property
    def default_page_size(self) -> int:
        """Get the default page size for queries."""
        return self._default_page_size

    @property
    def max_payload_size_mb(self) -> int:
        """Get the maximum allowed payload size in MB."""
        return self._max_payload_size_mb
    
    @property
    def allowed_hosts(self) -> list[str]:
        """Get the list of allowed hosts."""
        return self._allowed_hosts

    def reload(self):
        """Reload configuration from environment variables."""
        self._load_config()


# Module-level configuration instance - import this directly
config = Config()
