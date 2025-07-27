from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, APIRouter
from fastapi.responses import RedirectResponse, HTMLResponse
from routes import auto_ingest

app = FastAPI(
    title="Auto Ingest",
    description="",
    version="0.0.1",
)
app.add_middleware(
    CORSMiddleware,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_origins=["*"]
)

@app.get('/', include_in_schema=False)
def redirect():
    return RedirectResponse('/docs')

@app.get("/rapidoc", response_class=HTMLResponse, include_in_schema=False)
async def rapidoc():
    return f"""
        <!doctype html>
        <html>
            <head>
                <meta charset="utf-8">
                <script 
                    type="module" 
                    src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"
                ></script>
            </head>
            <body>
                <rapi-doc spec-url="{app.openapi_url}"></rapi-doc>
            </body> 
        </html>
    """

app.include_router(auto_ingest.app, prefix='/auto_ingest', tags=['auto_ingest'])
