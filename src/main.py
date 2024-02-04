from fastapi import FastAPI, File, UploadFile
from datetime import datetime
import os
import shutil

from PySparkProcessor import PySparkProcessor

app = FastAPI()

os.makedirs('batch_files', exist_ok=True)
os.makedirs('processed_batches', exist_ok=True)
os.makedirs('in_process', exist_ok=True)
os.makedirs('processed_files', exist_ok=True)

@app.post("/process")
async def process_file(file: UploadFile = File(...)):
    # Process File
    temp_file_path = f"./in_process/{file.filename}"
    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    # Process here
    try:
        processor = PySparkProcessor()
        processor.process_csv(temp_file_path)
        message = "File processed successfully"
    except Exception as e:
        message = f"Error processing file: {e}"

    # move files after processing
    dest_file_path = f'processed_files/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{file.filename}'
    shutil.move(temp_file_path, dest_file_path)

    return {"message": f"{message}"}

@app.post("/batch")
async def upload_batch_file(file: UploadFile = File(...)):
    # Save File in batch_files folder
    file_location = f"batch_files/{file.filename}"
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"message": f"File saved for batch processing: {file.filename}"}

@app.post("/process_batch")
async def process_batch_files():
    batch_files = os.listdir('batch_files')
    if not batch_files:
        return {"message": "No batch files to process"}

    for filename in batch_files:
        # Process here
        src_file_path = f'batch_files/{filename}'
        dest_file_path = f'processed_batches/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{filename}'
        # move files after processing
        shutil.move(src_file_path, dest_file_path)

    return {"message": "All batch files processed successfully"}