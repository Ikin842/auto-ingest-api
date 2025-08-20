import time
import traceback
from starlette.responses import JSONResponse

def error_response(e):
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal Server Error",
            "status_code": 500,
            "detail": str(e)
        }
    )

def success_response(start_time, message):
    return JSONResponse(
        content={
            "execute_time": round(time.time() - start_time, 4),
            "message": "success",
            "status_code": 200,
            "data": message
        }
    )
