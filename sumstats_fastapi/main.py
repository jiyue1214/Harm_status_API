# Basic
import os, sys
# Fast API
from typing import TypeVar
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Pagination
from fastapi_pagination import Page, add_pagination, paginate
from fastapi_pagination.customization import CustomizedPage, UseParamsFields

# Data Extraction
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from extract_data import DaraExtractor

#------- Define the app the pagination---------------------------
app = FastAPI()
add_pagination(app)  # important! add pagination to your app
T = TypeVar("T")
# 👇 Define a custom Page type with size default 5, limit max 1000
CustomPage = CustomizedPage[
    Page[T],
    UseParamsFields(
        size=Query(100, ge=1, le=1_000),  # default = 5, min = 1, max = 1000
    ),
]

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------API setting -----------------------------------------
# Serve static files (images)
input_sql="/Users/yueji/Documents/GitHub/harmonisation_status_table/test_data/new_order_sumstats.db"
extracted_data=DaraExtractor(db_path=input_sql,table_name="studies")
@app.get("/", response_model=CustomPage[dict])
def get_all_studies():
    data = extracted_data.extract_all()
    return paginate(data)
# http://127.0.0.1:8000?page=1&size=100


@app.get("/harmonised", response_model=CustomPage[dict])
def get_harmonised_studies():
    # Get the filtered data on the fly by calling the extract_data function
    data = extracted_data.extract_by_column("Harm_status","harmonised")
    return paginate(data)

@app.get("/fail_harmonisation", response_model=CustomPage[dict])
def get_failed_studies():
    # Get the filtered data on the fly by calling the extract_data function
    query = "Raw_file_type != 'not_harm' AND 'Harm_status' != 'harmonised'"
    data = extracted_data.extract_by_custom_query(query)
    return paginate(data)
# http://127.0.0.1:8000/fail_harmonisation?page=1&size=100

@app.get("/dropping_rate", response_model=CustomPage[dict])
def get_drop_rate():
    # Get the filtered data on the fly by calling the extract_data function
    query = "Harmonisation_drop_rate IS NOT NULL"
    data = extracted_data.extract_columns(["Study","PMID","Harmonisation_drop_rate"],query)
    return paginate(data)

@app.get("/GCST/{gcst_id}")
def get_harmonised_studies(gcst_id):
    # Get the filtered data on the fly by calling the extract_data function
    # e.g. GCST006551
    data = extracted_data.extract_by_column("Study",gcst_id)
    return JSONResponse(content=data)

@app.get("/PMID/{pmid}")
def get_harmonised_studies(pmid):
    # e.g. 33983923
    data = extracted_data.extract_by_column("PMID",pmid)
    return JSONResponse(content=data)

#app.mount("/dashboard", WSGIMiddleware(dash_app.server))

# Start the FastAPI server

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", log_level="info")
