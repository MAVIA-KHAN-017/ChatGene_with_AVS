from fastapi import HTTPException
from fastapi.responses import JSONResponse
import pyodbc

from database import get_database_connection,execute_query_with_retry
from fastapi import APIRouter
from fastapi import Query
from datetime import date


router = APIRouter()
cursor = get_database_connection()["cursor"]



@router.get("/top_selling_products/", tags=["Sales"])
async def top_selling_products(
    start_date: str = Query(None, title="Start Date", description="Start date for receivables"),
    end_date: str = Query(None, title="End Date", description="End date for receivables"),
    page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(10, title="Page Size", description="Number of items per page")
):
    try:
        offset = (page - 1) * page_size
        query = """
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
   
            SELECT 
                tpm.product_name,
                SUM(TTDD.qty) AS total_quantity
            FROM
                Trade_Transaction_Master_D TTMD
            JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
            JOIN 
                Trade_Product_Master tpm ON TTDD.product_id = tpm.product_id
            WHERE
                TTMD.invoice_code LIKE 'SI-%'
                AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
            GROUP BY 
                tpm.product_name
            ORDER BY 
                total_quantity DESC    
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
        """

        params = (
            start_date,
            end_date,
            offset,
            page_size
        )

        result = execute_query_with_retry(query, params)

        keys = ["product_name", "total_quantity"]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)





@router.get("/top_selling_products_proc/",tags=["Sales"])
async def top_selling_products(
    start_date: str = Query(None, title="Start Date", description="Start date for receivables"),
    end_date: str = Query(None, title="End Date", description="End date for receivables"),
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    )
):
    try:


     
        query = """
        EXEC TopSellingProduct @StartDate=?, @EndDate=?, @productSearch=?
        """

     

        params = (
            start_date,
            end_date,
            product_search
        )

        result = execute_query_with_retry(query, params)

        keys = ["product_name", "total_quantity"]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
