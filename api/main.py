from fastapi import FastAPI
from api.routers import  auth, backup, ingest, reports


app = FastAPI(title="API")
app.include_router(auth.router)
app.include_router(ingest.router)
app.include_router(backup.router)
app.include_router(reports.router)
