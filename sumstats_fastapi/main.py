# Basic
import os, sys
# Fast API
from typing import TypeVar
from fastapi import FastAPI, Query, Request
from ftplib import FTP
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


# Pagination
from fastapi_pagination import Page, add_pagination, paginate
from fastapi_pagination.customization import CustomizedPage, UseParamsFields

# Data Extraction
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from extract_data import DataExtractor

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
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------Read data -----------------------------------------
# Serve static files (images)
ftp_url = "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/sumstats_harm_status/Harm_sumstats_status.db"
extracted_data=DataExtractor(ftp_url=ftp_url,table_name="studies")

# ------------API route -----------------------------------------
@app.get("/query", response_model=CustomPage[dict])
def get_query(request: Request):
    """
    Accepts dynamic query conditions, for example:
    /query?Harm_drop_rate>0.8&Harm_status=harmonised
    """
    # Get all query parameters
    query_params = dict(request.query_params)
    print(query_params)
    data = extracted_data.extract_by_custom_query(query_params)
    return paginate(data)

@app.get("/all_studies", response_model=CustomPage[dict])
def get_all_studies():
    data = extracted_data.extract_all()
    return paginate(data)
# http://127.0.0.1:8000/all_studies?page=1&size=100

@app.get("/harmonised", response_model=CustomPage[dict])
def get_harmonised_studies():
    # Get the filtered data on the fly by calling the extract_data function
    data = extracted_data.extract_by_column("Harm_status","harmonised")
    return paginate(data)

@app.get("/fail_harmonisation", response_model=CustomPage[dict])
def get_failed_studies():
    # Get the filtered data on the fly by calling the extract_data function
    query = "Raw_file_type != 'not_harm' AND Harm_status != 'harmonised'"
    data = extracted_data.extract_by_custom_query(query)
    return paginate(data)
# http://127.0.0.1:8000/fail_harmonisation?page=1&size=100

@app.get("/dropping_rate", response_model=CustomPage[dict])
def get_drop_rate():
    # Get the filtered data on the fly by calling the extract_data function
    query = "Harm_drop_rate != 'NA' "
    data = extracted_data.extract_columns(["Study","PMID","Genotyping_type","Publication_date","Harm_drop_rate"],query)
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

# ----------Plotly Data----------------------------------

@app.get("/plotly/status_bar")
def get_harm_status():
    query="""
    SELECT Harm_status, COUNT(DISTINCT Study) AS num_unique_studies
    FROM studies
    GROUP BY Harm_status;
    """
    data = extracted_data._execute_query(query,())
    return JSONResponse(content=data)

@app.get("/plotly/harmed_six_month")
def get_last_six_month_harmonised_studies():
    # Get the filtered data on the fly by calling the extract_data function
    query="""
    SELECT 
        strftime('%Y-%m', Latest_harm_start_date) AS month,
        COUNT(*) AS num_studies
    FROM studies
    WHERE Harm_status = 'harmonised'
        AND Latest_harm_start_date != 'NaT'
        AND Latest_harm_start_date BETWEEN date('now', '-5 months') AND date('now')
    GROUP BY month
    ORDER BY month;
    """
    data = extracted_data._execute_query(query,())
    return JSONResponse(content=data)

@app.get("/plotly/drop_rate/{genotyp_type}")
def get_drop_rate_in_10_years(genotyp_type: str):
    query=f"""
    SELECT 
        Study, Genotyping_type, Harm_drop_rate,
        strftime('%Y', Publication_date) AS year
    FROM studies
    WHERE Harm_status = 'harmonised'
        AND Harm_drop_rate != 'NA'
        AND Harm_drop_rate >= 0.15
        AND Genotyping_type = '{genotyp_type}'
        AND Publication_date != 'NaT'
        AND Publication_date BETWEEN date('now', '-10 years') AND date('now')
    """
    data = extracted_data._execute_query(query,())
    return JSONResponse(content=data)

#app.mount("/dashboard", WSGIMiddleware(dash_app.server))

# ------------Start the FastAPI server -----------------------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", log_level="info")
