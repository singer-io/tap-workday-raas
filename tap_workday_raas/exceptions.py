class WorkdayRaasError(Exception):
    """Base exception for tap-workday-raas."""


class WorkdayRaasAuthenticationError(WorkdayRaasError):
    """Raised when credentials are invalid or expired (HTTP 401 / authentication failure)."""
