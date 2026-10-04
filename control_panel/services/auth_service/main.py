import uvicorn

from fastapi import FastAPI

from healthcheck import router as health_router
from auth.router import router as auth_router

from configurations import development_mode_settings

app = FastAPI(docs_url="/docs" if development_mode_settings.development_mode else None,
              redoc_url="/redoc" if development_mode_settings.development_mode else None)

app.include_router(health_router)
app.include_router(auth_router, prefix="/api")


def main():
    uvicorn.run("main:app",
                host='0.0.0.0',
                port=443,
                reload=False)


if __name__ == '__main__':
    main()
