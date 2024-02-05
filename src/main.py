from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import os
import shutil

from PySparkProcessor import PySparkProcessor

app = FastAPI()

templates = Jinja2Templates(directory="templates")

os.makedirs('rejected_files', exist_ok=True)
os.makedirs('in_process', exist_ok=True)
os.makedirs('processed_files', exist_ok=True)

@app.post("/load_jobs")
async def process_jobs_file(file: UploadFile = File(...), bloks: int = 1):
    file_path, filename = save_file(file)
    return process_file("job", file_path, filename, bloks)

@app.post("/load_departments")
async def process_departments_file(file: UploadFile = File(...), bloks: int = 1):
    file_path, filename = save_file(file)
    return process_file("department", file_path, filename, bloks)

@app.post("/load_employees")
async def process_departments_file(file: UploadFile = File(...), bloks: int = 1):
    file_path, filename = save_file(file)
    return process_file("employee", file_path, filename, bloks)

def save_file(file: UploadFile) -> (str, str):
    temp_file_path = f"./in_process/{file.filename}"
    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return temp_file_path, file.filename

def process_file(file_type: str, file_path: str, filename: str, blocks: int):
    # Process here
    try:
        processor = PySparkProcessor()
        needs_preprocess = file_type == "employee"
        processed_records = 0
        if needs_preprocess:
            valid_employees_df = processor.preprocess_employees_file(file_path, blocks)
            processed_records = processor.process_employees_df(valid_employees_df, blocks)
        else:
            processed_records = processor.process_csv(file_path, file_type)

        message = f"File processed successfully, records inserted: {processed_records}"
        # move files after processing
        dest_file_path = f'processed_files/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{filename}'
        shutil.move(file_path, dest_file_path)
    except Exception as e:
        message = f"Error processing file: {e}"
        # move to rejected folder
        dest_file_path = f'rejected_files/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{filename}'
        shutil.move(file_path, dest_file_path)

    return {"message": f"{message}"}


@app.get("/employees_job_2021", response_class=HTMLResponse)
async def employees_job_2021(request: Request):
    data = None
    processor = PySparkProcessor()
    data = processor.get_employees_job_2021()
    return templates.TemplateResponse("employees_job_2021.html", {"request": request, "data": data})

@app.get("/hired_by_department_2021", response_class=HTMLResponse)
async def hired_by_department_2021(request: Request):
    data = None
    processor = PySparkProcessor()
    data = processor.get_hired_by_department_2021()
    return templates.TemplateResponse("hired_by_department_2021.html", {"request": request, "data": data})