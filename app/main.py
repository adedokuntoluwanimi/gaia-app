from fastapi import FastAPI

from app.api.routes import jobs

app = FastAPI(
    title="GAIA Backend",
    version="0.1.0",
)

app.include_router(
    jobs.router,
    prefix="/jobs",
    tags=["jobs"],
)
