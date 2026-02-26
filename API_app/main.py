import logging
from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from utils import process_and_dispatch, scan_local_folder_task
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ingestion-service")

INPUT_FOLDER = os.getenv("INPUT_FOLDER", "./input_images")

app = FastAPI(title="Ingestion Service", description="Gateway for Reshet Hakatavim Pipeline")


@app.post("/upload/")
async def upload_image(file: UploadFile = File(...)):
    file_data = await file.read()
    image_id = process_and_dispatch(file_data, file.filename)
    return {"message": "Image accepted and in pipeline", "image_id": image_id}


@app.post("/scan-folder/")
async def scan_folder(background_tasks: BackgroundTasks):
    background_tasks.add_task(scan_local_folder_task)
    return {"message": f"Started scanning folder: {INPUT_FOLDER} in the background."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
