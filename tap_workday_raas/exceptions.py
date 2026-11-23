class WorkdayRaasError(Exception):
    """Base exception for tap-workday-raas."""


class WorkdayRaasAuthenticationError(WorkdayRaasError):
    """Raised when OAuth authentication fails (e.g. HTTP 401/403) or token refresh cannot be completed."""


class WorkdayRaasReportTooLargeError(WorkdayRaasError):
    """Raised when a report exceeds the maximum size that can be buffered in memory."""
