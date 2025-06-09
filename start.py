import os

import uvicorn

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 8000))

    env = os.getenv("ENV", "prod").lower()
    reload_enabled = env in ("dev", "test")

    uvicorn.run(
        'app.main:app',
        host="0.0.0.0",
        port=port,
        lifespan='on',
        access_log=False,
        loop='uvloop',
        reload=reload_enabled
    )
