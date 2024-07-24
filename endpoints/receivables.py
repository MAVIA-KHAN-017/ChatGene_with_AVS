from fastapi import Depends, Path, HTTPException
from fastapi.responses import JSONResponse
import pyodbc
from datetime import datetime, date
from database import get_database_connection, execute_query_with_retry
from fastapi import APIRouter
from fastapi import Query, Depends
import json
from decimal import Decimal


router = APIRouter()
cursor = get_database_connection()["cursor"]
# connection = get_database_connection()["connection"]


@router.get("/receivables_today", tags=["Receivables"])
async def get_receivables_today(
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    ),
    invoice_id: str = Query(None, title="Invoice id Filter", description="invoice id"),
    invoice_code: str = Query(
        None, title="Invoice code Filter", description="invoice code"
    ),
):
    try:
        query = """
        DECLARE @customerSearch NVARCHAR(50) = ?;
        DECLARE @Invoiceid NVARCHAR(50)=?;
        DECLARE @Invoicecode NVARCHAR(50)=?;
            WITH CreditNoteAmount AS (
                SELECT 
                    TTDD.reference_invoice__Detailid,
                    TTMD.invoice_id,
                    TTMD.invoice_code,
                    TTDD.invoice_detail_id,
                    TTMD.total_amount AS CreditNoteTotal,
                    TTMD.tax_amount AS CNA_tax_amount,
                    TTMD.paid_amount AS CNA_paid_amount,
                    TTMD.net_amount AS CreditNoteTotalAmount
                FROM
                    Trade_Transaction_Master_D TTMD
                INNER JOIN
                    Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
                WHERE 
                    TTMD.invoice_code LIKE 'CN-%'
            )
            SELECT 
                DISTINCT TTMD.invoice_id,
                TTMD.vendor_id,
                TVM.description,
                TTMD.invoice_code,
                gc.description AS currency,
                TTMD.total_amount - COALESCE(CNA.CreditNoteTotal, 0) AS total_amount,
                TTMD.tax_amount - COALESCE(CNA.CNA_tax_amount, 0) AS total_tax,
                TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount, 0) AS total_net_amount,
                TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0) AS total_paid_amount,
                (TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount, 0)) - (TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_receivables_amount,
                MIN(CAST(TTMD.invoice_date AS DATE)) OVER (PARTITION BY TTMD.invoice_id) AS invoice_date
            FROM
                Trade_Transaction_Master_D TTMD
            INNER JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
            INNER JOIN
                Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
            INNER JOIN
                Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
            INNER JOIN
                Accounts_Segment AS as2 ON TTDD.segment_id = as2.segment_id
            INNER JOIN
                GEN_Currency gc ON TTMD.currency_id = gc.currency_id
            LEFT JOIN 
                CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
            WHERE 
                TTMD.active = 1
                AND (TVM.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '')
                AND CAST(TTMD.invoice_date AS DATE) = CAST(GETDATE() AS DATE)
                AND TTMD.invoice_code LIKE 'SI-%'
                AND (TTMD.invoice_code LIKE '%' + @Invoicecode + '%' OR @Invoicecode = '') 
                AND (TTMD.invoice_id LIKE '%' + @Invoiceid + '%' OR @Invoiceid = '');
      
        """
        params = (customer_search, invoice_id, invoice_code)
        result = execute_query_with_retry(query, params)

        keys = [
            "invoice_id",
            "vendor_id",
            "description",
            "invoice_code",
            "currency",
            "total_amount",
            "tax_amount",
            "total_net_amount",
            "total_paid_amount",
            "total_receivables_amount",
            "invoice_date",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_start_end_date", tags=["Receivables"])
async def get_receivables_start_end_date(
    start_date: str = Query(
        None, title="Start Date", description="Start date for receivables"
    ),
    end_date: str = Query(
        None, title="End Date", description="End date for receivables"
    ),
    page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(
        10, title="Page Size", description="Number of items per page"
    ),
):
    try:
        offset = (page - 1) * page_size
        query = """
            DECLARE @StartDate DATE = ?
            DECLARE @EndDate DATE = ?
         

            ;WITH CreditNoteAmount AS (
            SELECT 
                TTDD.reference_invoice__Detailid,
                TTMD.invoice_id,
                TTMD.invoice_code,
                TTDD.invoice_detail_id,
                TTMD.total_amount AS CreditNoteTotal,
                TTMD.tax_amount AS CNA_tax_amount,
                TTMD.paid_amount AS CNA_paid_amount,
                TTMD.net_amount AS CreditNoteTotalAmount
            FROM
                Trade_Transaction_Master_D TTMD
            INNER JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
            WHERE 
                TTMD.invoice_code LIKE 'CN-%' 
        )

        SELECT
            DISTINCT (TTMD.invoice_id), 
            TTMD.vendor_id,
            TVM.description,
            TTMD.invoice_code,
            gc.description AS currency,
        --    TTMD.total_amount AS B_CN_amount,
        --	COALESCE(CNA.CreditNoteTotal, 0) AS CreditNoteTotal,
            TTMD.total_amount - COALESCE(CNA.CreditNoteTotal, 0) AS total_amount,
        --    TTMD.tax_amount AS B_CN_total_tax,
        --    COALESCE(CNA.CNA_tax_amount, 0) AS CNA_tax_amount,
            TTMD.tax_amount - COALESCE(CNA.CNA_tax_amount,0) AS total_tax,
        --    TTMD.net_amount AS B_CN_net_amount,
        --    COALESCE(CNA.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
            TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount,0) AS total_net_amount,
            COALESCE(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0), 0) AS total_paid_amount,
        (TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount, 0)) - COALESCE(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0), 0) AS total_receivables_amount,   
        CAST(TTMD.invoice_date AS DATE) AS invoice_date 
        FROM
            Trade_Transaction_Master_D TTMD
        INNER JOIN
            Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
        INNER JOIN
            Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
        INNER JOIN
            Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
        INNER JOIN
            Accounts_Segment AS as2 ON TTDD.segment_id = as2.segment_id
        INNER JOIN
            GEN_Currency gc ON TTMD.currency_id = gc.currency_id
        LEFT JOIN 
            CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
        WHERE 
            TTMD.active = 1
            AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) 
            AND TTMD.invoice_code LIKE 'SI-%'
            ORDER BY 
                ttmd.vendor_id
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
            
        """
        params = (start_date, end_date, offset, page_size)
        result = execute_query_with_retry(query, params)

        keys = [
            "invoice_id",
            "vendor_id",
            "description",
            "invoice_code",
            "currency",
            "total_amount",
            "tax_amount",
            "total_net_amount",
            "total_paid_amount",
            "total_receivables_amount",
            "invoice_date",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_start_end_date__proc", tags=["Receivables"])
async def get_receivables_start_end_date(
    start_date: str = Query(
        ..., title="Start Date", description="Start date for receivables"
    ),
    end_date: str = Query(
        ..., title="End Date", description="End date for receivables"
    ),
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    ),
    invoice_id: str = Query(
        None, title="Invoice code Filter", description="invoice code"
    ),
    invoice_code: str = Query(
        None, title="Invoice code Filter", description="invoice code"
    ),
):
    try:

        query = """
        EXEC ReceivablesStartEndDateSearch @StartDate=?, @EndDate=?,@customerSearch=?,@Invoiceid=?,@Invoicecode=?
        """

        params = (start_date, end_date, customer_search, invoice_id, invoice_code)

        result = execute_query_with_retry(query, params)

        keys = [
            "invoice_id",
            "vendor_id",
            "description",
            "invoice_code",
            "currency",
            "total_amount",
            "tax_amount",
            "total_net_amount",
            "total_paid_amount",
            "total_receivables_amount",
            "invoice_date",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_today_total", tags=["Receivables"])
async def get_receivables_today_total():
    try:
        query = """

;WITH CreditNoteAmount AS (
    SELECT 
        TTDD.reference_invoice__Detailid,
        TTMD.invoice_id,
        TTMD.invoice_code,
        TTDD.invoice_detail_id,
        TTMD.total_amount AS CreditNoteTotal,
        TTMD.tax_amount AS CNA_tax_amount,
        TTMD.paid_amount AS CNA_paid_amount,
        TTMD.net_amount AS CreditNoteTotalAmount
    FROM
        Trade_Transaction_Master_D TTMD
    INNER JOIN
        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
    WHERE 
        TTMD.invoice_code LIKE 'CN-%' 
),
RankedInvoices AS (
    SELECT 
        ttmd.invoice_id, 
        ttmd.net_amount,
        ttmd.paid_amount,
        COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
        COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
        ROW_NUMBER() OVER(PARTITION BY ttmd.invoice_id ORDER BY ttmd.invoice_id) AS RowNumber
    FROM 
        Trade_Transaction_Master_D ttmd 
    INNER JOIN
        Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id  
    LEFT JOIN 
        CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
    WHERE 
     ttmd.active = 1
        AND ttmd.invoice_code LIKE 'SI-%' 
        AND CAST(TTMD.invoice_date AS DATE) = CAST(GETDATE() AS DATE)
)
SELECT 
    SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
    SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
FROM 
    RankedInvoices
WHERE 
    RowNumber = 1;
    
        """

        result = execute_query_with_retry(query)

        keys = ["total_net_amount", "total_receivables_amount"]
        result_dict = dict(zip(keys, result[0]))

        return {"result": result_dict}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_overall_total", tags=["Receivables"])
async def get_receivables_overall_total():
    try:
        query = """
  

WITH CreditNoteAmount AS (
    SELECT 
        TTDD.reference_invoice__Detailid,
        TTMD.invoice_id,
        TTMD.invoice_code,
        TTDD.invoice_detail_id,
        TTMD.total_amount AS CreditNoteTotal,
        TTMD.tax_amount AS CNA_tax_amount,
        TTMD.paid_amount AS CNA_paid_amount,
        TTMD.net_amount AS CreditNoteTotalAmount
    FROM
        Trade_Transaction_Master_D TTMD
    INNER JOIN
        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
    WHERE 
        TTMD.invoice_code LIKE 'CN-%' 
),
RankedInvoices AS (

    SELECT 
        ttmd.invoice_id, 
        ttmd.net_amount,
        ttmd.paid_amount,
        COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
        COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
        ROW_NUMBER() OVER(PARTITION BY ttmd.invoice_id ORDER BY ttmd.invoice_id) AS RowNumber
    FROM 
        Trade_Transaction_Master_D ttmd 
    INNER JOIN
        Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id  
    LEFT JOIN 
        CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
    WHERE 
    	ttmd.active = 1
        AND ttmd.invoice_code LIKE 'SI-%' 
       
)
SELECT 
    SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
    SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
FROM 
    RankedInvoices
WHERE 
    RowNumber = 1;

    

                
        """

        result = execute_query_with_retry(query)

        keys = ["total_net_amount", "total_receivables_amount"]
        result_dict = dict(zip(keys, result[0]))

        return {"result": result_dict}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_start_end_date_total", tags=["Receivables"])
async def get_receivables_start_end_date_total(
    start_date: date = Query(
        None, title="Start Date", description="Start date for receivables"
    ),
    end_date: date = Query(
        None, title="End Date", description="End date for receivables"
    ),
):
    try:
        query = """
            DECLARE @StartDate DATE = ?
            DECLARE @EndDate DATE = ?
;WITH CreditNoteAmount AS (
    SELECT 
        TTDD.reference_invoice__Detailid,
        TTMD.invoice_id,
        TTMD.invoice_code,
        TTDD.invoice_detail_id,
        TTMD.total_amount AS CreditNoteTotal,
        TTMD.tax_amount AS CNA_tax_amount,
        TTMD.paid_amount AS CNA_paid_amount,
        TTMD.net_amount AS CreditNoteTotalAmount
    FROM
        Trade_Transaction_Master_D TTMD
    INNER JOIN
        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
    WHERE 
        TTMD.invoice_code LIKE 'CN-%' 
),
RankedInvoices AS (

    SELECT 
        ttmd.invoice_id, 
        ttmd.net_amount,
        ttmd.paid_amount,
        COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
        COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
        ROW_NUMBER() OVER(PARTITION BY ttmd.invoice_id ORDER BY ttmd.invoice_id) AS RowNumber
    FROM 
        Trade_Transaction_Master_D ttmd 
    INNER JOIN
        Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id  
    LEFT JOIN 
        CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
    WHERE 
    	ttmd.active = 1
    	AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
        AND ttmd.invoice_code LIKE 'SI-%' 
       
)
SELECT 
    SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
    SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
FROM 
    RankedInvoices
WHERE 
    RowNumber = 1;

        """
        params = (start_date, end_date)
        result = execute_query_with_retry(query, params)

        keys = ["total_net_amount", "total_receivables_amount"]
        result_dict = dict(zip(keys, result[0]))

        return {"result": result_dict}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


# @router.get("/receivables_by_city",tags=["Receivables"])
# async def get_invoices_by_city(
#     city_filter: str = Query("", title="City Filter", description="City filter"),
#     customer_search: str = Query(
#         "", title="customer_search", description="Unique identifier of the vendor."
#     ),
# ):
#     try:
#         query = """
#             DECLARE @CityFilter NVARCHAR(50) = ?;
#             DECLARE @customerSearch NVARCHAR(50) = ?;
#              WITH CreditNoteAmount AS (
# 			    SELECT
# 			        TTDD.reference_invoice__Detailid,
# 			        TTMD.invoice_id,
# 			        TTMD.invoice_code,
# 			        TTDD.invoice_detail_id,
# 			        SUM(amount) AS CreditNoteTotal,
# 			         SUM(TTMD.paid_amount) AS CNA_paid_amount,
# 			        SUM(TTMD.net_amount) AS CreditNoteTotalAmount,
# 			        SUM(TTMD.tax_amount) AS CNA_tax_amount,
# 			        SUM(TTMD.net_amount) AS B_CN_net_amount,
# 			        SUM(TTMD.tax_amount) AS B_CN_total_tax
# 			    FROM
# 			        Trade_Transaction_Master_D TTMD
# 			    INNER JOIN
# 			        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
# 			    WHERE
# 			        TTMD.invoice_code LIKE 'CN-%'
# 			    GROUP BY
# 			        TTDD.reference_invoice__Detailid,
# 			        TTMD.invoice_id,
# 			        TTMD.invoice_code,
# 			        TTDD.invoice_detail_id
# 			)
#             SELECT
#             ttmd.vendor_id,
# 		    tvm.description,
#             COUNT(DISTINCT ttmd.invoice_id) AS total_invoices,
#             gc.description  AS currency,
# --            COALESCE(SUM(ttmd.total_amount), 0) AS B_CN_amount,
# --            SUM(CNA.CreditNoteTotal) AS CreditNoteTotal,
#           	SUM(TTMD.total_amount) - COALESCE(SUM(CNA.CreditNoteTotal),0) AS total_amount,
# --            COALESCE(SUM(TTMD.tax_amount), 0) AS B_CNA_tax_amount,
# --            SUM(DISTINCT CNA.B_CN_total_tax) AS CNA_tax_amount,
#             SUM(TTMD.tax_amount) - COALESCE(SUM(DISTINCT CNA.CNA_tax_amount),0) AS tax_amount,
# --            SUM(TTMD.net_amount) AS B_CN_net_amount,
# --            SUM(DISTINCT CNA.B_CN_net_amount) AS CreditNoteTotalAmount,
#             SUM(TTMD.net_amount) - COALESCE(SUM(DISTINCT CNA.CreditNoteTotalAmount),0) AS total_net_amount,
#          	SUM(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_paid_amount,
#         	SUM(TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount, 0)) - SUM(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_receivables_amount
#             FROM
#                 Trade_Transaction_Master_D ttmd
#             JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             JOIN
#                 GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id
#             JOIN
#                 Trade_Vendor_Master tvm ON ttmd.vendor_id = tvm.vendor_id
#             JOIN
#                 GEN_Currency gc on ttmd.currency_id=gc.currency_id
#             LEFT JOIN
#    				CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
#             WHERE
#                 ttmd.active = 1
#                 AND (tvm.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '')
#                 AND      (@CityFilter = '' OR gttm.description LIKE '%' + @CityFilter)
#                 AND (ttmd.invoice_code  LIKE 'SI-%')
#             GROUP BY
# 	            	ttmd.vendor_id,tvm.description,gc.description
# 	        ORDER BY
# 	            	ttmd.vendor_id


#         """

#         params = (city_filter, customer_search)
#         result = execute_query_with_retry(query, params)

#         keys = [
#             "vendor_id",
#             "description",
#             "total_invoices",
#             "currency",
#             "total_amount",
#             "tax_amount",
#             "total_net_amount",
#             "total_paid_amount",
#             "total_receivables_amount",
#             "invoice_date",
#         ]
#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/receivables_by_segment",tags=["Receivables"])
# async def get_invoices_by_segment(
#     segment_filter: str = Query(
#         "", title="Segment Filter", description="Segment filter"
#     ),
#     customer_search: str = Query(
#         "", title="customer_search", description="Unique identifier of the vendor."
#     ),
# ):
#     try:
#         query = """
#             DECLARE @Segment NVARCHAR(50) = ?;
#             DECLARE @customerSearch NVARCHAR(50) = ?;
#              WITH CreditNoteAmount AS (
# 			    SELECT
# 			        TTDD.reference_invoice__Detailid,
# 			        TTMD.invoice_id,
# 			        TTMD.invoice_code,
# 			        TTDD.invoice_detail_id,
# 			        SUM(amount) AS CreditNoteTotal,
# 			         SUM(TTMD.paid_amount) AS CNA_paid_amount,
# 			        SUM(TTMD.net_amount) AS CreditNoteTotalAmount,
# 			        SUM(TTMD.tax_amount) AS CNA_tax_amount,
# 			        SUM(TTMD.net_amount) AS B_CN_net_amount,
# 			        SUM(TTMD.tax_amount) AS B_CN_total_tax
# 			    FROM
# 			        Trade_Transaction_Master_D TTMD
# 			    INNER JOIN
# 			        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
# 			    WHERE
# 			        TTMD.invoice_code LIKE 'CN-%'
# 			    GROUP BY
# 			        TTDD.reference_invoice__Detailid,
# 			        TTMD.invoice_id,
# 			        TTMD.invoice_code,
# 			        TTDD.invoice_detail_id
# 			)
#             SELECT
#             ttmd.vendor_id,
# 		    tvm.description,
#             COUNT(DISTINCT ttmd.invoice_id) AS total_invoices,
#             gc.description  AS currency,
# --            COALESCE(SUM(ttmd.total_amount), 0) AS B_CN_amount,
# --            SUM(CNA.CreditNoteTotal) AS CreditNoteTotal,
#           	SUM(TTMD.total_amount) - COALESCE(SUM(CNA.CreditNoteTotal),0) AS total_amount,
# --            COALESCE(SUM(TTMD.tax_amount), 0) AS B_CNA_tax_amount,
# --            SUM(DISTINCT CNA.B_CN_total_tax) AS CNA_tax_amount,
#             SUM(TTMD.tax_amount) - COALESCE(SUM(DISTINCT CNA.CNA_tax_amount),0) AS tax_amount,
# --            SUM(TTMD.net_amount) AS B_CN_net_amount,
# --            SUM(DISTINCT CNA.B_CN_net_amount) AS CreditNoteTotalAmount,
#             SUM(TTMD.net_amount) - COALESCE(SUM(DISTINCT CNA.CreditNoteTotalAmount),0) AS total_net_amount,
#          	SUM(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_paid_amount,
#         	SUM(TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount, 0)) - SUM(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_receivables_amount
#             FROM
#                 Trade_Transaction_Master_D ttmd
#             JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             JOIN
#                 GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id
#             JOIN
#                 Trade_Vendor_Master tvm ON ttmd.vendor_id = tvm.vendor_id
#             JOIN
#                 GEN_Currency gc on ttmd.currency_id=gc.currency_id
#             LEFT JOIN
#    			CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
#             WHERE
#                 ttmd.active = 1
#                 AND (tvm.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '')
#                 AND      (@Segment = '' OR gttm.description LIKE '%' + @Segment + '%')
# 				AND (ttmd.invoice_code  LIKE 'SI-%')
# 			GROUP BY
# 	            	ttmd.vendor_id,tvm.description,gc.description
# 	        ORDER BY
# 	            	ttmd.vendor_id


#         """

#         params = (segment_filter, customer_search)
#         result = execute_query_with_retry(query, params)

#         keys = [
#             "vendor_id",
#             "description",
#             "total_invoices",
#             "currency",
#             "total_amount",
#             "tax_amount",
#             "total_net_amount",
#             "total_paid_amount",
#             "total_receivables_amount",
#             "invoice_date",
#         ]
#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/receivables_by_category",tags=["Receivables"])
# async def get_invoices_by_category(
#     category_filter: str = Query(
#         "", title="Category Filter", description="Category filter"
#     ),
#     customer_search: str = Query(
#         "", title="customer_search", description="Unique identifier of the vendor."
#     ),
# ):
#     try:
#         query = """
#             DECLARE @CategoryFilter NVARCHAR(50) = ?;
#             DECLARE @customerSearch NVARCHAR(50) = ?;

#              WITH CreditNoteAmount AS (
# 			    SELECT
# 			        TTDD.reference_invoice__Detailid,
# 			        TTMD.invoice_id,
# 			        TTMD.invoice_code,
# 			        TTDD.invoice_detail_id,
# 			        SUM(amount) AS CreditNoteTotal,
# 			         SUM(TTMD.paid_amount) AS CNA_paid_amount,
# 			        SUM(TTMD.net_amount) AS CreditNoteTotalAmount,
# 			        SUM(TTMD.tax_amount) AS CNA_tax_amount
# 			    FROM
# 			        Trade_Transaction_Master_D TTMD
# 			    INNER JOIN
# 			        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
# 			    WHERE
# 			        TTMD.invoice_code LIKE 'CN-%'
# 			    GROUP BY
# 			        TTDD.reference_invoice__Detailid,
# 			        TTMD.invoice_id,
# 			        TTMD.invoice_code,
# 			        TTDD.invoice_detail_id
# 			)
#             SELECT
#             TTMD.vendor_id,
#             tvm.description,
#             COUNT(DISTINCT TTMD.invoice_id) AS total_invoices,
#             gc.description  AS currency,
# --            COALESCE(SUM(ttmd.total_amount), 0) AS B_CN_amount,
# --            SUM(CNA.CreditNoteTotal) AS CreditNoteTotal,
#           	SUM(TTMD.total_amount) - COALESCE(SUM(CNA.CreditNoteTotal),0) AS total_amount,
# --            COALESCE(SUM(TTMD.tax_amount), 0) AS B_CNA_tax_amount,
# --            SUM(DISTINCT CNA.B_CN_total_tax) AS CNA_tax_amount,
#             SUM(TTMD.tax_amount) - COALESCE(SUM(DISTINCT CNA.CNA_tax_amount),0) AS tax_amount,
# --            SUM(TTMD.net_amount) AS B_CN_net_amount,
# --            SUM(DISTINCT CNA.B_CN_net_amount) AS CreditNoteTotalAmount,
#             SUM(TTMD.net_amount) - COALESCE(SUM(DISTINCT CNA.CreditNoteTotalAmount),0) AS total_net_amount,
#          	SUM(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_paid_amount,
#         	SUM(TTMD.net_amount - COALESCE(CNA.CreditNoteTotalAmount, 0)) - SUM(TTMD.paid_amount - COALESCE(CNA.CNA_paid_amount, 0)) AS total_receivables_amount
#             FROM
#                 Trade_Transaction_Master_D TTMD
#             JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             JOIN
#                 GEN_Transaction_Type_Master gttm ON TTMD.Transaction_type_id = gttm.transaction_type_id
#             JOIN
#                 Trade_Vendor_Master tvm ON TTMD.vendor_id = tvm.vendor_id
#             JOIN
#                 GEN_Currency gc on TTMD.currency_id=gc.currency_id
#             LEFT JOIN
#    			CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
#             WHERE
#                 TTMD.active = 1
#                 AND
#                 (@CategoryFilter = '' OR (@CategoryFilter = 'Compressor' AND (gttm.description LIKE '%AT%' OR gttm.description LIKE '%PMP%')) OR
#                 (@CategoryFilter = 'Electrical' AND (gttm.description LIKE '%ABB%' OR gttm.description LIKE '%CHT%' OR gttm.description LIKE '%ABB-RTL%' OR gttm.description LIKE '%GE%')) OR
#                 (@CategoryFilter = 'Rice' AND gttm.description LIKE '%RIC%'))
#                 AND (tvm.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '')
#                 AND (TTMD.invoice_code  LIKE 'SI-%')
#             GROUP BY
#             TTMD.vendor_id,tvm.description,gc.description
#             ORDER BY
#              TTMD.vendor_id
#         """

#         params = (category_filter, customer_search)
#         result = execute_query_with_retry(query, params)

#         keys = [
#             "vendor_id",
#             "description",
#             "total_invoices",
#             "currency",
#             "total_amount",
#             "tax_amount",
#             "total_net_amount",
#             "total_paid_amount",
#             "total_receivables_amount",
#             "invoice_date",
#         ]
#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/total_receivables_by_city",tags=["Receivables"])
# async def get_invoices_by_city(
#     city_filter: str = Query("", title="City Filter", description="City filter"),
# ):
#     try:
#         query = """
#             DECLARE @CityFilter NVARCHAR(50) = ?;

# WITH CreditNoteAmount AS (
#     SELECT
#         TTDD.reference_invoice__Detailid,
#         TTMD.invoice_id,
#         TTMD.invoice_code,
#         TTDD.invoice_detail_id,
#         TTMD.total_amount AS CreditNoteTotal,
#         TTMD.tax_amount AS CNA_tax_amount,
#         TTMD.paid_amount AS CNA_paid_amount,
#         TTMD.net_amount AS CreditNoteTotalAmount
#     FROM
#         Trade_Transaction_Master_D TTMD
#     INNER JOIN
#         Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#     WHERE
#         TTMD.invoice_code LIKE 'CN-%'
# ),
# RankedInvoices AS (

#     SELECT
#         ttmd.invoice_id AS invoice_id,
#         ttmd.net_amount,
#         ttmd.paid_amount,
#         COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
#         COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
#         ROW_NUMBER() OVER(PARTITION BY ttmd.invoice_id ORDER BY ttmd.invoice_id) AS RowNumber
#     FROM
#         Trade_Transaction_Master_D ttmd
#     INNER JOIN
#         Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id
#     INNER JOIN
#         GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id
#     LEFT JOIN
#         CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
#     WHERE
#     	ttmd.active = 1
#     	AND      (@CityFilter = '' OR gttm.description LIKE '%' + @CityFilter)
#         AND ttmd.invoice_code LIKE 'SI-%'

# )
# SELECT
# 	COUNT(invoice_id) AS total_invoices,
#     SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
#     SUM(paid_amount - CNA_paid_amount) AS total_paid_amount,
#     SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
# FROM
#     RankedInvoices
# WHERE
#     RowNumber = 1;


#         """

#         params = city_filter
#         result = execute_query_with_retry(query, params)
#         keys = [
#             "total_invoices",
#             "total_net_amount",
#             "total_paid_amount",
#             "total_receivables_amount",
#         ]
#         result_dicts = dict(zip(keys, result[0]))

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/total_receivables_by_segment",tags=["Receivables"])
# async def get_invoices_by_segment(
#     segment_filter: str = Query(
#         "", title="Segment Filter", description="Segment filter"
#     ),
# ):
#     try:
#         query = """
#          DECLARE @Segment NVARCHAR(50) = ?;
# WITH CreditNoteAmount AS (
#     SELECT
#         TTDD.reference_invoice__Detailid,
#         TTMD.invoice_id,
#         TTMD.invoice_code,
#         TTDD.invoice_detail_id,
#         TTMD.total_amount AS CreditNoteTotal,
#         TTMD.tax_amount AS CNA_tax_amount,
#         TTMD.paid_amount AS CNA_paid_amount,
#         TTMD.net_amount AS CreditNoteTotalAmount
#     FROM
#         Trade_Transaction_Master_D TTMD
#     INNER JOIN
#         Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#     WHERE
#         TTMD.invoice_code LIKE 'CN-%'
# ),
# RankedInvoices AS (

#     SELECT
#         ttmd.invoice_id AS invoice_id,
#         ttmd.net_amount,
#         ttmd.paid_amount,
#         COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
#         COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
#         ROW_NUMBER() OVER(PARTITION BY ttmd.invoice_id ORDER BY ttmd.invoice_id) AS RowNumber
#     FROM
#         Trade_Transaction_Master_D ttmd
#     INNER JOIN
#         Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id
#     INNER JOIN
#         GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id
#     LEFT JOIN
#         CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
#     WHERE
#     	ttmd.active = 1
#     	AND      (@Segment = '' OR gttm.description LIKE '%' + @Segment + '%')
#         AND ttmd.invoice_code LIKE 'SI-%'

# )
# SELECT
# 	COUNT(invoice_id) AS total_invoices,
#     SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
#     SUM(paid_amount - CNA_paid_amount) AS total_paid_amount,
#     SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
# FROM
#     RankedInvoices
# WHERE
#     RowNumber = 1;


#         """

#         params = segment_filter
#         result = execute_query_with_retry(query, params)
#         keys = [
#             "total_invoices",
#             "total_net_amount",
#             "total_paid_amount",
#             "total_receivables_amount",
#         ]
#         result_dicts = dict(zip(keys, result[0]))

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/total_receivables_by_category",tags=["Receivables"])
# async def get_invoices_by_category(
#     category_filter: str = Query(
#         "", title="Category Filter", description="Category filter"
#     ),
# ):
#     try:
#         query = """
#             DECLARE @CategoryFilter NVARCHAR(50) = ?;

# WITH CreditNoteAmount AS (
#     SELECT
#         TTDD.reference_invoice__Detailid,
#         TTMD.invoice_id,
#         TTMD.invoice_code,
#         TTDD.invoice_detail_id,
#         TTMD.total_amount AS CreditNoteTotal,
#         TTMD.tax_amount AS CNA_tax_amount,
#         TTMD.paid_amount AS CNA_paid_amount,
#         TTMD.net_amount AS CreditNoteTotalAmount
#     FROM
#         Trade_Transaction_Master_D TTMD
#     INNER JOIN
#         Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#     WHERE
#         TTMD.invoice_code LIKE 'CN-%'
# ),
# RankedInvoices AS (

#     SELECT
#         ttmd.invoice_id AS invoice_id,
#         ttmd.net_amount,
#         ttmd.paid_amount,
#         COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
#         COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
#         ROW_NUMBER() OVER(PARTITION BY ttmd.invoice_id ORDER BY ttmd.invoice_id) AS RowNumber
#     FROM
#         Trade_Transaction_Master_D ttmd
#     INNER JOIN
#         Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id
#     INNER JOIN
#         GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id
#     LEFT JOIN
#         CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
#     WHERE
#     	ttmd.active = 1
#          AND
#         (@CategoryFilter = '' OR (@CategoryFilter = 'Compressor' AND (gttm.description LIKE '%AT%' OR gttm.description LIKE '%PMP%')) OR
#         (@CategoryFilter = 'Electrical' AND (gttm.description LIKE '%ABB%' OR gttm.description LIKE '%CHT%' OR gttm.description LIKE '%ABB-RTL%' OR gttm.description LIKE '%GE%')) OR
#         (@CategoryFilter = 'Rice' AND gttm.description LIKE '%RIC%'))
#         AND ttmd.invoice_code LIKE 'SI-%'

# )
# SELECT
# 	COUNT(invoice_id) AS total_invoices,
#     SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
#     SUM(paid_amount - CNA_paid_amount) AS total_paid_amount,
#     SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
# FROM
#     RankedInvoices
# WHERE
#     RowNumber = 1;

#         """

#         params = category_filter
#         result = execute_query_with_retry(query, params)
#         keys = [
#             "total_invoices",
#             "total_net_amount",
#             "total_paid_amount",
#             "total_receivables_amount",
#         ]
#         result_dicts = dict(zip(keys, result[0]))

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_with_filter", tags=["Receivables"])
async def receivables_with_filter(
    Category_Filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    Segment: str = Query(..., title="Segment Filter", description="Segment filter"),
    City_Filter: str = Query(
        ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
    ),
    start_date: str = Query(
        None,
        title="Start Date",
        description="Start date of the filter range (format: 'YYYY-MM-DD')",
    ),
    end_date: str = Query(
        None,
        title="End Date",
        description="End date of the filter range (format: 'YYYY-MM-DD')",
    ),
    page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(
        10, title="Page Size", description="Number of items per page"
    ),
):
    try:

        offset = (page - 1) * page_size
        query = """
        


                DECLARE @CategoryFilter NVARCHAR(50) = ?;
                DECLARE @Segment NVARCHAR(50) = ?;
                DECLARE @CityFilter NVARCHAR(50) = ?;
        
                DECLARE @StartDate DATE = ?;
                DECLARE @EndDate DATE = ?;

                -- Common table expression for credit note amounts
                WITH CreditNoteAmount AS (
                    SELECT 
                        TTDD.reference_invoice__Detailid,
                        TTMD.invoice_id,
                        TTMD.invoice_code,
                        TTDD.invoice_detail_id,
                        TTMD.total_amount AS CreditNoteTotal,
                        TTMD.tax_amount AS CNA_tax_amount,
                        TTMD.paid_amount AS CNA_paid_amount,
                        TTMD.net_amount AS CreditNoteTotalAmount
                    FROM
                        Trade_Transaction_Master_D TTMD
                    INNER JOIN
                        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
                    WHERE 
                        TTMD.invoice_code LIKE 'CN-%' 
                ),
                -- Common table expression for ranking invoices
                RankedInvoices AS (
                    SELECT 
                        TTMD.invoice_id,
                        TTDD.invoice_detail_id,
                        TTMD.vendor_id,
                        TVM.description AS vendor_description,
                        TTMD.total_amount,
                        TTMD.tax_amount,
                        TTMD.net_amount,
                        TTMD.paid_amount,
                        gc.description AS currency,
                        COALESCE(CNA.CreditNoteTotal, 0) AS CreditNoteTotal,
                        COALESCE(CNA.CNA_tax_amount, 0) AS CNA_tax_amount,
                        COALESCE(CNA.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
                        COALESCE(CNA.CNA_paid_amount, 0) AS CNA_paid_amount,
                        ROW_NUMBER() OVER(PARTITION BY TTMD.invoice_id ORDER BY TTDD.invoice_detail_id) AS RowNumber
                    FROM
                        Trade_Transaction_Master_D TTMD
                    INNER JOIN
                        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
                    INNER JOIN
                        Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
                    INNER JOIN
                        Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
                    INNER JOIN
                        Accounts_Segment AS AS2 ON TTDD.segment_id = AS2.segment_id
                    INNER JOIN
                        GEN_Currency gc ON TTMD.currency_id = gc.currency_id
                    INNER JOIN 
                        GEN_Transaction_Type_Master gttm ON TTMD.Transaction_type_id = gttm.transaction_type_id
                    LEFT JOIN 
                        CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
                    WHERE
                        TTMD.active = 1
                        AND (@Segment = '' OR gttm.description LIKE '%' + @Segment + '%')
                        AND (@CategoryFilter = '' 
                            OR (@CategoryFilter = 'Compressor' AND (gttm.description LIKE '%AT%' OR gttm.description LIKE '%PMP%'))
                            OR (@CategoryFilter = 'Electrical' AND (gttm.description LIKE '%ABB%' OR gttm.description LIKE '%CHT%' OR gttm.description LIKE '%ABB-RTL%' OR gttm.description LIKE '%GE%'))
                            OR (@CategoryFilter = 'Rice' AND gttm.description LIKE '%RIC%')
                        )
                        AND (@CityFilter = '' OR gttm.description LIKE '%' + @CityFilter + '%')
                    
                        AND TTMD.invoice_code LIKE 'SI-%'
                        AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
                )
                -- Final selection and aggregation
                SELECT
                    vendor_id,
                    vendor_description,
                    COUNT(DISTINCT invoice_id) AS total_invoices,
                    currency,
                    SUM(total_amount) - COALESCE(SUM(CreditNoteTotal), 0) AS total_amount,
                    SUM(tax_amount) - COALESCE(SUM(CNA_tax_amount), 0) AS tax_amount,
                    SUM(net_amount) - COALESCE(SUM(CreditNoteTotalAmount), 0) AS total_net_amount,
                    SUM(paid_amount - COALESCE(CNA_paid_amount, 0)) AS total_paid_amount,
                    SUM(net_amount - COALESCE(CreditNoteTotalAmount, 0)) - SUM(paid_amount - COALESCE(CNA_paid_amount, 0)) AS total_receivables_amount   
                FROM
                    RankedInvoices
                WHERE 
                    RowNumber = 1
                GROUP BY
                    vendor_id, vendor_description, currency
                ORDER BY
                    vendor_id

        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
                """

        params = (
            Category_Filter,
            Segment,
            City_Filter,

            start_date,
            end_date,
            offset,
            page_size,
        )
        result = execute_query_with_retry(query, params)

        keys = [

            "vendor_id",
            "vendor_description",
            "total_invoices",
            "currency",
            "total_amount",
            "tax_amount",
            "total_net_amount",
            "total_paid_amount",
            "total_receivables_amount"
           
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)




@router.get("/receivables_with_filter_proc", tags=["Receivables"])
async def get_sales_statistics(
    category_filter: str = Query(
        None,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    city_filter: str = Query(
        None, title="City Filter", description="City filter (KHI, LHR, ISL)"
    ),
    segment_filter: str = Query(
        None, title="Segment Filter", description="Segment filter"
    ),
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    ),
    start_date: str = Query(
        None,
        title="Start Date",
        description="Start date of the filter range (format: 'YYYY-MM-DD')",
    ),
    end_date: str = Query(
        None,
        title="End Date",
        description="End date of the filter range (format: 'YYYY-MM-DD')",
    )


):

    try:

        query = """
        EXEC ReceivablessFilterSearch 
            @CategoryFilter = ?, 
            @CityFilter = ?, 
            @SegmentFilter = ?, 
            @customerSearch = ?, 
            @StartDate = ?, 
            @EndDate = ?
        """

        params = (
            category_filter,
            city_filter,
            segment_filter,
            customer_search,
            start_date,
            end_date,
        
        )

        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "vendor_description",
            "total_invoices",
            "currency",
            "total_amount",
            "tax_amount",
            "total_net_amount",
            "total_paid_amount",
            "total_receivables_amount"
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)





@router.get("/total_receivables_with_filter",tags=["Receivables"])
async def total_receivables_with_filter(
    Category_Filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    Segment: str = Query(
        ..., title="Segment Filter", description="Segment filter"
    ),
    City_Filter: str = Query(
        ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
    ),

    start_date: str = Query(
        None,
        title="Start Date",
        description="Start date of the filter range (format: 'YYYY-MM-DD')",
    ),
    end_date: str = Query(
        None,
        title="End Date",
        description="End date of the filter range (format: 'YYYY-MM-DD')",
    )


   
):
    try:

     
        query = """


            DECLARE @CategoryFilter NVARCHAR(50) = ?;
            DECLARE @Segment NVARCHAR(50) = ?;
            DECLARE @CityFilter NVARCHAR(50) = ?;
        
                DECLARE @StartDate DATE = ?;
                        DECLARE @EndDate DATE = ?;
            WITH CreditNoteAmount AS (
                SELECT
                    TTDD.reference_invoice__Detailid,
                    TTMD.invoice_id,
                    TTMD.invoice_code,
                    TTDD.invoice_detail_id,
                    TTMD.total_amount AS CreditNoteTotal,
                    TTMD.tax_amount AS CNA_tax_amount,
                    TTMD.paid_amount AS CNA_paid_amount,
                    TTMD.net_amount AS CreditNoteTotalAmount
                FROM
                    Trade_Transaction_Master_D TTMD
                INNER JOIN
                    Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
                WHERE
                    TTMD.invoice_code LIKE 'CN-%'
            ),
            RankedInvoices AS (
                SELECT
                    ttmd.invoice_id AS invoice_id,
                    ttmd.net_amount,
                    ttmd.paid_amount,
                    COALESCE(cna.CreditNoteTotalAmount, 0) AS CreditNoteTotalAmount,
                    COALESCE(cna.CNA_paid_amount, 0) AS CNA_paid_amount,
                    ROW_NUMBER() OVER(PARTITION BY TTMD.invoice_id ORDER BY TTDD.invoice_detail_id) AS RowNumber
                FROM
                    Trade_Transaction_Master_D ttmd
                INNER JOIN
                    Trade_Transaction_Detail_D TTDD ON ttmd.invoice_id = TTDD.invoice_id
                INNER JOIN
                    GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id
                LEFT JOIN
                    CreditNoteAmount cna ON TTDD.invoice_detail_id = cna.reference_invoice__Detailid
                WHERE
                    TTMD.active = 1
                    AND (@Segment = '' OR gttm.description LIKE '%' + @Segment + '%')
                    AND (@CategoryFilter = '' 
                        OR (@CategoryFilter = 'Compressor' AND (gttm.description LIKE '%AT%' OR gttm.description LIKE '%PMP%'))
                        OR (@CategoryFilter = 'Electrical' AND (gttm.description LIKE '%ABB%' OR gttm.description LIKE '%CHT%' OR gttm.description LIKE '%ABB-RTL%' OR gttm.description LIKE '%GE%'))
                        OR (@CategoryFilter = 'Rice' AND gttm.description LIKE '%RIC%')
                    )
                    AND (@CityFilter = '' OR gttm.description LIKE '%' + @CityFilter + '%')
                    AND TTMD.invoice_code LIKE 'SI-%'
                    AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
            )
            SELECT
                COUNT(invoice_id) AS total_invoices,
                SUM(net_amount - CreditNoteTotalAmount) AS total_net_amount,
                SUM(paid_amount - CNA_paid_amount) AS total_paid_amount,
                SUM(net_amount - CreditNoteTotalAmount) - SUM(paid_amount - CNA_paid_amount) AS total_receivables_amount
            FROM
                RankedInvoices
            WHERE
                RowNumber = 1;


                """


        params = (Category_Filter, Segment, City_Filter, start_date, end_date)
        result = execute_query_with_retry(query, params)



        keys = [
            "total_invoices",
            "total_net_amount",
            "total_paid_amount",
            "total_receivables_amount"
            
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)