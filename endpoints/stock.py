from fastapi import Query, Depends
from fastapi.responses import JSONResponse
import pyodbc
import json
from database import get_database_connection, execute_query_with_retry
from fastapi import APIRouter
from decimal import Decimal
from datetime import date
from typing import Optional

router = APIRouter()

cursor = get_database_connection()["cursor"]
# connection = get_database_connection()["connection"]


@router.get("/warehouse_sales_statistics",tags=["Stock"])
async def get_product_sales_statistics(
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
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
   
            SELECT
                TTDD.product_id AS product_id,
                TPM.product_code AS product_code,
                TPM.product_name AS product_name,
                GC.company_id  AS warehouse_id,
                GC.description AS warehouse_description,
                TPS.in_qty AS in_quantity,
                TPS.out_qty AS out_quantity,
                TPS.bal_qty AS balance_quantity,
                MAX(TPM.last_modification_datetime) AS last_change,
                SUM(TTDD.qty) AS total_sold_quantity
--                SUM(TTDD.sold_qty) as sold_quantity
               
             
            FROM
                Trade_Transaction_Master_D TTMD
            JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
            JOIN
                Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
            JOIN
                Trade_Product_WareHouse_Stock TPS ON TTDD.product_id = TPS.product_id
            JOIN
                GEN_Companies GC ON TPS.warehouse_id = GC.company_id
            JOIN
                GEN_Currency gcurr on TTMD.currency_id=gcurr.currency_id
            WHERE
            TPM.active = 1 
            AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
             
            GROUP BY
                TTDD.product_id,
                TPM.product_code,
                TPM.product_name,
                GC.company_id,
                GC.description,
                TPS.in_qty,
                TPS.out_qty,
                TPS.bal_qty
              
            ORDER BY 
                TTDD.product_id
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
        """

        params = (start_date, end_date, offset, page_size)
        result = execute_query_with_retry(query, params)

        keys = [
            "product_id",
            "product_code",
            "product_name",
            "warehouse_id",
            "warehouse_description",
            "in_quantity",
            "out_quantity",
            "balance_quantity",
            "last_change_date",
            "total_sold_quantity",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)




@router.get("/warehouse_sales_statistics_proc",tags=["Stock"])
async def get_product_sales_statistics(
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
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
    warehouse_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
):
    try:

        query = """
        EXEC WarehouseSearch @StartDate=?, @EndDate=?, @productSearch=?,@warehouseSearch=?
        """

        params = (start_date, end_date, product_search, warehouse_search)

        result = execute_query_with_retry(query, params)

        keys = [
            "product_id",
            "product_code",
            "product_name",
            "warehouse_id",
            "warehouse_description",
            "in_quantity",
            "out_quantity",
            "balance_quantity",
            "last_change_date",
            "total_sold_quantity"
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)






# @router.get("/stock_aging",tags=["Stock"])
# async def get_stock_aging(
#     start_date: str = Query(
#         None,
#         title="Start Date",
#         description="Start date of the filter range (format: 'YYYY-MM-DD')",
#     ),
#     end_date: str = Query(
#         None,
#         title="End Date",
#         description="End date of the filter range (format: 'YYYY-MM-DD')",
#     ),
#     page: int = Query(1, title="Page", description="Page number"),
#     page_size: int = Query(
#         10, title="Page Size", description="Number of items per page"
#     ),
# ):
#     try:

#         offset = (page - 1) * page_size
#         query = """
        
                  
#             DECLARE @StartDate DATE = ?;
#             DECLARE @EndDate DATE = ?;
#             SELECT
#                 p.product_id,
#                 p.product_name,
#                 gc.company_id as warehouse_id,
#                 gc.description as warehouse_description,
#                 MAX(TTMD.invoice_date) AS last_invoice_date,
#                 DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) AS days_since_last_transaction,
#                 t.in_qty AS total_in_qty,
#                 t.out_qty AS total_out_qty,
#                 t.bal_qty AS total_bal_qty,
#                 MAX(p.last_modification_datetime) AS last_change_date,
#                 CASE
#                     WHEN DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) <= 60 THEN '0-60 days'
#                     WHEN DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) > 60 AND DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) <= 120 THEN '61-120 days'
#                     WHEN DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) > 120 AND DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) <= 180 THEN '121-180 days'
#                     WHEN DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) > 180 AND DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) <= 365 THEN '181-365 days'
#                     WHEN DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) > 365 AND DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) <= 730 THEN '1-2 years'
#                     WHEN DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) > 730 AND DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) <= 1095 THEN '2-3 years'
#                     ELSE '3+ years'
#                 END AS days_category
#             FROM
#                 Trade_Transaction_Master_D TTMD
#             JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             JOIN 
#                 Trade_Product_Master p ON TTDD.product_id = p.product_id
#             JOIN
#                 Trade_Product_WareHouse_Stock t ON p.product_id = t.product_id
#             JOIN
#                 Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
#             JOIN
#                 GEN_Companies gc on t.warehouse_id=gc.company_id    
#             WHERE
#                 p.active = 1 
#                 AND p.is_working = 1 
#             --    AND t.bal_qty > 0 
#                 AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) 
#             GROUP BY
#                 p.product_id, 
#                 p.product_name, 
#                 as2.segment_id, 
#                 as2.description, 
#                 gc.company_id,
#                 gc.description, 
#                 t.in_qty, 
#                 t.out_qty, 
#                 t.bal_qty
#             ORDER BY
#                 last_invoice_date DESC    
    
#             OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
#         """

#         params = (start_date, end_date, offset, page_size)
#         result = execute_query_with_retry(query, params)

#         keys = [
#             "product_id",
#             "product_name",
#             "warehouse_id",
#             "warehouse_description",
#             "last_invoice_date",
#             "days_since_last_transaction",
#             "total_in_qty",
#             "total_out_qty",
#             "total_bal_qty",
#             "last_change_date",
#             "days_category"
#         ]
#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


@router.get("/stock_aging",tags=["Stock"])
async def get_stock_aging(
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
        
                  
            DECLARE @StartDate DATE =?;
            DECLARE @EndDate DATE =?;
	
            WITH BalQTy AS (
                SELECT 
                    t.product_id,
                    SUM(t.bal_qty) AS bal_qty
                FROM
                    Trade_Product_WareHouse_Stock t
                WHERE 
                    t.bal_qty > 0
                GROUP BY 
                    t.product_id
            ),  
            deyailQty AS (
                SELECT 
                    TTDD.product_id,
                    TPM.product_name,
                    TTMD.invoice_date,
                    TTDD.qty,
                    gc.description  AS currency,
                    TTDD.amount,
                    SUM(TTDD.qty) OVER (PARTITION BY TTDD.product_id ORDER BY TTMD.invoice_date DESC) AS running_qty,
                    SUM(TTDD.amount) OVER (PARTITION BY TTDD.product_id ORDER BY TTMD.invoice_date DESC) AS running_amount,
                    ROW_NUMBER() OVER (PARTITION BY TTDD.product_id ORDER BY TTMD.invoice_date DESC) AS row_num
                FROM
                    Trade_Transaction_Master_D TTMD
                JOIN
                    Trade_Transaction_Detail_D TTDD 
                    ON TTMD.invoice_id = TTDD.invoice_id
                JOIN
                    Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
                JOIN
            		GEN_Currency gc on TTMD.currency_id=gc.currency_id
                WHERE
                    TTMD.invoice_code LIKE 'PI-%'
            ),
            LatestInvoiceDates AS (
                SELECT 
                    product_id,
                    MAX(invoice_date) AS max_invoice_date
                FROM 
                    deyailQty
                GROUP BY 
                    product_id
            ),
            FilteredResults AS (
                SELECT 
                    b.product_id,
                    d.product_name,
                    b.bal_qty,
                    d.qty,
                    d.invoice_date,
                    d.running_qty,
                    d.currency,
                    d.amount,
                    d.running_amount,
                    l.max_invoice_date AS last_invoice_date,
                    DATEDIFF(DAY, l.max_invoice_date, GETDATE()) AS days_since_last_transaction,
                    CASE
                        WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 60 THEN '0-60 days'
                        WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 60 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 120 THEN '61-120 days'
                        WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 120 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 180 THEN '121-180 days'
                        WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 180 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 365 THEN '181-365 days'
                        WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 365 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 730 THEN '1-2 years'
                        WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 730 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 1095 THEN '2-3 years'
                        ELSE '3+ years'
                    END AS days_category,
                    d.row_num
                FROM 
                    BalQTy b
                JOIN 
                    deyailQty d 
                    ON b.product_id = d.product_id
                JOIN 
                    LatestInvoiceDates l
                    ON d.product_id = l.product_id
                WHERE
                    (d.running_qty - d.qty < b.bal_qty OR d.running_qty <= b.bal_qty)
                    AND (b.bal_qty > 1 OR d.row_num = 1)
                
            ),
            LastRows AS (
                SELECT
                    product_id,
                    MAX(row_num) AS max_row_num
                FROM
                    FilteredResults
                GROUP BY
                    product_id
            )
            
            SELECT 
                fr.product_id,
                fr.product_name,
                fr.bal_qty,
            --    fr.qty,
            --    fr.invoice_date,
            --    fr.running_qty,
            --    fr.amount,
                fr.currency,
                fr.running_amount,
                AVG(running_amount * 1.0 / bal_qty) OVER (PARTITION BY fr.product_id) AS average_running_amount_by_bal_qty,
                fr.last_invoice_date,
                fr.days_since_last_transaction,
                fr.days_category
            FROM 
                FilteredResults fr

            JOIN 
                LastRows lr
                ON fr.product_id = lr.product_id AND fr.row_num = lr.max_row_num
            WHERE
                (@StartDate = '' OR fr.last_invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
            
            ORDER BY 
                fr.last_invoice_date DESC

            
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
                """

        params = (start_date, end_date, offset, page_size)
        result = execute_query_with_retry(query, params)

        keys = [
            "product_id",
            "product_name",
            "bal_qty",
            # "qty",
            # "invoice_date",
            # "running_qty",
            # "amount",
            "currency",
            "running_amount",
            "average_running_amount_by_bal_qty",
            "last_invoice_date",
            "days_since_last_transaction",
            "days_category"
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/stock_aging_proc",tags=["Stock"])
async def get_stock_aging(
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
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    )
):

    try:


        query = """
        EXEC StockAgingSearch @StartDate=?, @EndDate=?,@productSearch=?
        """

        params = (start_date, end_date, product_search)

        result = execute_query_with_retry(query, params)

        keys = [
            "product_id",
            "product_name",
            "bal_qty",
            # "qty",
            # "invoice_date",
            # "running_qty",
            # "amount",
            "currency",
            "running_amount",
            "average_running_amount_by_bal_qty",
            "last_invoice_date",
            "days_since_last_transaction",
            "days_category"
        ]
        result_dicts = []

        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)








@router.get("/stock_summary",tags=["Stock"])
async def get_stock_summary(
    product_id: str = Query(
        ..., title="Product ID", description="Unique identifier of the vendor."
    ),
        page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(
        10, title="Page Size", description="Number of items per page"
    ),
):
    try:
        offset = (page - 1) * page_size
        query = """
    
        
           DECLARE @ProductId INT=?;
  WITH BalQTy AS (
    SELECT 
        t.product_id,
        SUM(t.bal_qty) AS bal_qty
    FROM
        Trade_Product_WareHouse_Stock t
    WHERE 
        t.bal_qty > 0
    GROUP BY 
        t.product_id
),  
deyailQty AS (
    SELECT 
        TTDD.product_id,
        TPM.product_name,
        TTMD.invoice_date,
        TTDD.qty,
        gc.description  AS currency,
        TTDD.amount,
        SUM(TTDD.qty) OVER (PARTITION BY TTDD.product_id ORDER BY TTMD.invoice_date DESC) AS running_qty,
        SUM(TTDD.amount) OVER (PARTITION BY TTDD.product_id ORDER BY TTMD.invoice_date DESC) AS running_amount,
        ROW_NUMBER() OVER (PARTITION BY TTDD.product_id ORDER BY TTMD.invoice_date DESC) AS row_num
    FROM
        Trade_Transaction_Master_D TTMD
    JOIN
        Trade_Transaction_Detail_D TTDD 
        ON TTMD.invoice_id = TTDD.invoice_id
    JOIN
        Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
                JOIN
            		GEN_Currency gc on TTMD.currency_id=gc.currency_id
    WHERE
        TTMD.invoice_code LIKE 'PI-%'
),
LatestInvoiceDates AS (
    SELECT 
        product_id,
        MAX(invoice_date) AS max_invoice_date
    FROM 
        deyailQty
    GROUP BY 
        product_id
),
FilteredResults AS (
    SELECT 
        b.product_id,
        d.product_name,
        b.bal_qty,
        d.qty,
        d.invoice_date,
        d.running_qty,
        d.currency,
        d.amount,
        d.running_amount,
        l.max_invoice_date AS last_invoice_date,
        DATEDIFF(DAY, l.max_invoice_date, GETDATE()) AS days_since_last_transaction,
        CASE
            WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 60 THEN '0-60 days'
            WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 60 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 120 THEN '61-120 days'
            WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 120 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 180 THEN '121-180 days'
            WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 180 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 365 THEN '181-365 days'
            WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 365 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 730 THEN '1-2 years'
            WHEN DATEDIFF(DAY, l.max_invoice_date, GETDATE()) > 730 AND DATEDIFF(DAY, l.max_invoice_date, GETDATE()) <= 1095 THEN '2-3 years'
            ELSE '3+ years'
        END AS days_category,
        d.row_num
    FROM 
        BalQTy b
    JOIN 
        deyailQty d 
        ON b.product_id = d.product_id
    JOIN 
        LatestInvoiceDates l
        ON d.product_id = l.product_id
    WHERE
        (d.running_qty - d.qty < b.bal_qty OR d.running_qty <= b.bal_qty)
        AND (b.bal_qty > 1 OR d.row_num = 1)
)

SELECT 
    fr.product_id,
    fr.product_name,
    fr.bal_qty,
    fr.qty,
    fr.invoice_date,
    fr.running_qty,
    fr.currency,
    fr.amount,
    fr.running_amount,
    AVG(running_amount * 1.0 / bal_qty) OVER (PARTITION BY fr.product_id) AS average_running_amount_by_bal_qty,
    fr.last_invoice_date,
    fr.days_since_last_transaction,
    fr.days_category
FROM 
    FilteredResults fr
WHERE 
	fr.product_id=@ProductId

ORDER BY 
    fr.last_invoice_date DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """

        params = (product_id, offset, page_size)
        result = execute_query_with_retry(query, params)

        keys = [
            "product_id",
            "product_name",
            "bal_qty",
            "qty",
            "invoice_date",
            "running_qty",
            "currency",
            "amount",
            "running_amount",
            "average_running_amount_by_bal_qty",
            "last_invoice_date",
            "days_since_last_transaction",
            "days_category"
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)







# @router.get("/stock_summary_proc",tags=["Stock"])
# async def get_stock_summary_search(
#     product_id: str = Query(
#         ..., title="Product ID", description="Unique identifier of the vendor."
#     ),
#     customer_search: str = Query(
#         "", title="customer_search", description="Unique identifier of the vendor."
#     ),
#     warehouse_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
# ):
#     try:

#         query = """
#         EXEC StockSummarySearch @productId=?, @customerSearch=?,@warehouseSearch=?
#         """

#         params = (product_id, customer_search,warehouse_search)

#         result = execute_query_with_retry(query, params)
#         keys = [
#             "invoice_detail_id",
#             "vendor_id",
#             "vendor_name",
#             "vendor_type",
#             "product_id",
#             "product_name",
#             "warehouse_description",
#             "StockMovementType",
#             "total_in_qty",
#             "invoice_date",
#             "description",
#             "total_amount",
#         ]
#         result_dicts = []

#         result_dicts = [dict(zip(keys, row)) for row in result]
#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)





