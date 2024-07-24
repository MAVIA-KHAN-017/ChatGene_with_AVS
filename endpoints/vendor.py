from fastapi import Depends, Path, Query
from fastapi.responses import JSONResponse
import pyodbc
from database import get_database_connection,execute_query_with_retry
from fastapi import APIRouter

router = APIRouter()
cursor = get_database_connection()["cursor"]
# connection = get_database_connection()["connection"]

from fastapi import Query


@router.get("/vendor_summary/",tags=["Stock"])
async def get_vendor_summary(
    page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(
        10, title="Page Size", description="Number of items per page"
    ),
):
    try:
        offset = (page - 1) * page_size
        query = """
    
            SELECT
                ttm.vendor_id,
                v.description AS vendor_name,
                gc.description AS currency,
                SUM(ttm.net_amount) AS total_net_amount,
                COUNT(ttm.invoice_id) AS total_invoices,
                SUM(ttm.paid_amount) AS total_paid_amount,
                SUM(ttm.net_amount - ttm.paid_amount) AS balance_amount
            FROM
                Trade_Transaction_Master_D ttm
            JOIN
                Trade_Vendor_Master v ON ttm.vendor_id = v.vendor_id
            JOIN
                GEN_Currency gc ON ttm.currency_id = gc.currency_id
            JOIN 
                GEN_Transaction_Type_Master gttm ON ttm.Transaction_type_id = gttm.transaction_type_id
            WHERE
                ttm.active = 1 
                AND   (ttm.invoice_code  LIKE 'PI-%' ) 
            GROUP BY
                ttm.vendor_id, v.description, gc.description
            ORDER BY
                ttm.vendor_id ASC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
   

        """
        params = (offset, page_size)
        result = execute_query_with_retry(query,params)

        keys = [
            "vendor_id",
            "vendor_name",
            "currency",
            "total_net_amount",
            "total_invoices",
            "total_paid_amount",
            "balance_amount",
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/detailed_vendor_information/{vendor_id}",tags=["Stock"])
async def get_detailed_vendor_information(
    vendor_id: str = Path(
        ..., title="Vendor ID", description="Unique identifier of the vendor."
    ),
    page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(
        10, title="Page Size", description="Number of items per page"
    ),
):
    try:
        offset = (page - 1) * page_size
        query = """
          
        SELECT 
            tvm.vendor_id,
            ttm.invoice_id,
            ttd.invoice_detail_id,
            ttm.invoice_code,
            tvm.description AS vendor_name,
            tpm.product_name,
            YEAR(ttm.invoice_date) AS t_year,
            MONTH(ttm.invoice_date) AS t_month,
            '-' AS currency,
            COALESCE(ttd.net_amount, 0) AS total_detailed_amount,
            CASE
                WHEN ttd.invoice_detail_id = (
                    SELECT MAX(ttd2.invoice_detail_id) 
                    FROM Trade_Transaction_Detail_D ttd2 
                    WHERE ttd2.invoice_id = ttd.invoice_id
                ) THEN COALESCE(ttm.tax_amount, 0)
                ELSE NULL
            END AS total_tax_amount,
            CASE
                WHEN ttd.invoice_detail_id = (
                    SELECT MAX(ttd2.invoice_detail_id) 
                    FROM Trade_Transaction_Detail_D ttd2 
                    WHERE ttd2.invoice_id = ttd.invoice_id
                ) THEN COALESCE(ttm.net_amount , 0)
                ELSE NULL
            END AS total_paid_amount
        FROM 
            Trade_Vendor_Master tvm
        LEFT JOIN 
            Trade_Transaction_Master_D ttm ON tvm.vendor_id = ttm.vendor_id
        LEFT JOIN 
            Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
        LEFT JOIN 
            Trade_Product_Master tpm ON tpm.product_id = ttd.product_id
        LEFT JOIN
            GEN_Currency gc on ttm.currency_id = gc.currency_id
        WHERE 
            ttm.active = 1
            AND tvm.vendor_id = ?
            AND ttm.invoice_code LIKE 'PI-%'
        ORDER BY 
            ttm.invoice_id, ttd.invoice_detail_id
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
        """
        params = (
            vendor_id,
            offset,page_size
        )
        result = execute_query_with_retry(query,params)

        keys = [
            "vendor_id",
            "invoice_id",
            "invoice_detail_id",
            "invoice_code",
            "vendor_name",
            "product_name",
            "t_year",
            "t_month",
            "currency",
            "total_detailed_amount",
            "total_tax_amount",
            "total_paid_amount",
         
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/vendor_summary_proc/",tags=["Stock"])
async def get_vendor_summary(
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    )
):
    try:
        query = """
        EXEC VendorSummary @customerSearch=?
        """

        params = (customer_search)

        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "vendor_name",
            "currency",
            "total_net_amount",
            "total_invoices",
            "total_paid_amount",
            "balance_amount",
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/detailed_vendor_information_proc",tags=["Stock"])
async def get_detailed_vendor_information(
    vendor_id: str = Query(
        ..., title="Vendor ID", description="Unique identifier of the vendor."
    ),
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
            invoice_id: str = Query(
        None, title="Invoice code Filter", description="invoice code"
    ),
    invoice_code: str = Query(
        None, title="Invoice code Filter", description="invoice code"
    )
):
    try:


        query = """
        EXEC DetailedVendorSummary @vendorId=?, @productSearch=?,@Invoiceid=?,@Invoicecode =?
        """

        params = (vendor_id,product_search,invoice_id,invoice_code)

        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "invoice_id",
            "invoice_detail_id",
            "invoice_code",
            "vendor_name",
            "product_name",
            "t_year",
            "t_month",
            "currency",
            "total_detailed_amount",
            "total_tax_amount",
            "total_paid_amount",
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
