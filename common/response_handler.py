from common.schemas import APIResponse
from datetime import datetime


def success_response(data: any = None, message: str = "Success") -> dict:
    return APIResponse(
        success=True,
        message=message,
        data=data,
        timestamp=datetime.now()
    ).dict()


def error_response(error: str, message: str = "Error") -> dict:
    return APIResponse(
        success=False,
        message=message,
        error=error,
        timestamp=datetime.now()
    ).dict()