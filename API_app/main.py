from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from API_app.utils import process_and_dispatch, scan_local_folder_task
from Shared.logger_config import get_logger
from Shared.config import settings

logger = get_logger("ingestion-service")

app = FastAPI(
    title="Ingestion Service",
    description="Gateway for Reshet The Writers Pipeline"
)


@app.post("/upload/")
def upload_image(file: UploadFile = File(...)):
    file_data = file.file.read()
    image_id = process_and_dispatch(file_data, file.filename)
    return {"message": "Image accepted and in pipeline", "image_id": image_id}


@app.post("/scan-folder/")
async def scan_folder(background_tasks: BackgroundTasks):
    background_tasks.add_task(scan_local_folder_task)
    return {"message": f"Started scanning folder: {settings.INPUT_FOLDER} in the background."}


if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting API Service on {settings.API_HOST}:{settings.API_PORT}")
    uvicorn.run("API_app.main:app", host=settings.API_HOST, port=settings.API_PORT, reload=True)
