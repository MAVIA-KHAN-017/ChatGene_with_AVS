from datetime import datetime, date
from fastapi import Query, Depends
from fastapi.responses import JSONResponse
import pyodbc
import json
from database import get_database_connection, redis_client
from fastapi import APIRouter

router = APIRouter()

cursor = get_database_connection()["cursor"]


@router.get("/warehouse_sales_statistics_search")
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
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
            DECLARE @productSearch NVARCHAR(50) = ?;
            DECLARE @warehouseSearch NVARCHAR(50) = ?;

            SELECT
                TTDD.product_id AS product_id,
                TPM.product_code AS product_code,
                TPM.product_name AS product_name,
                TPM.description AS product_description,
                GC.company_code AS warehouse_code,
                GC.description AS warehouse_description,
                TPS.in_qty AS in_quantity,
                TPS.out_qty AS out_quantity,
                TPS.bal_qty AS balance_quantity,
                MAX(TTMD.last_modification_datetime) AS last_change,
                SUM(TTDD.qty) AS total_sold_quantity,
                gcurr.description  AS currency,
                SUM(TTDD.net_amount) AS total_sales_amount,
                MAX(TTDD.net_amount) AS max_sales_amount,
                MIN(TTDD.net_amount) AS min_sales_amount
            FROM
                Trade_Transaction_Master_D TTMD
            JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
            JOIN
                Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
            JOIN
                Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
            JOIN
                Trade_Product_WareHouse_Stock TPS ON TTDD.product_id = TPS.product_id
            JOIN
                GEN_Companies GC ON TPS.warehouse_id = GC.company_id
            JOIN
                GEN_Currency gcurr on TTMD.currency_id=gcurr.currency_id
            WHERE
                (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) AND
                (TPM.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '') AND
                (GC.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')

            GROUP BY
                TTDD.product_id,
                TPM.product_code,
                TPM.product_name,
                TPM.description,
                GC.company_code,
                GC.description,
                TPS.in_qty,
                TPS.out_qty,
                TPS.bal_qty,
                gcurr.description
            ORDER BY
                TTDD.product_id
        """

        result = cursor.execute(
            query, start_date, end_date, product_search, warehouse_search
        ).fetchall()

        keys = [
            "product_id",
            "product_code",
            "product_name",
            "product_description",
            "warehouse_code",
            "warehouse_description",
            "in_quantity",
            "out_quantity",
            "balance_quantity",
            "last_change",
            "overtime_sold_quantities",
            "currency",
            "total_sales_amount",
            "max_sales_amount",
            "min_sales_amount",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/sales_statistics_search")
async def get_sales_statistics(
    category_filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    city_filter: str = Query(
        ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
    ),
    segment_filter: str = Query(
        ..., title="Segment Filter", description="Segment filter"
    ),
    vendor_type: str = Query(
        ..., title="Segment Filter", description="Vendor type (Sales,Purchase,All)"
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
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
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


			DECLARE @CategoryFilter NVARCHAR(50) = ?;
            DECLARE @CityFilter NVARCHAR(50) = ?;
            DECLARE @SegmentFilter NVARCHAR(50) = ?;
            DECLARE @VendorType NVARCHAR(50) = ?;
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
            DECLARE @customerSearch NVARCHAR(50) = ?;
            DECLARE @productSearch NVARCHAR(50) = ?;
            DECLARE @warehouseSearch NVARCHAR(50) = ?;
            SELECT
                TTMD.vendor_id,
                TVM.description AS vendor_name,
                as2.description AS segment,
                TVM.vendor_type AS transaction_type,
                TTDD.product_id,
                TPM.product_name AS product_group,
                TTDD.qty AS qty,
                TTDD.rate AS unit_rate,
                gc.description  AS currency,
                COALESCE(SUM(TTDD.amount), 0) AS total_amount,
                TTMD.invoice_date
            FROM
                Trade_Transaction_Master_D TTMD
            INNER JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
            INNER JOIN
                Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
            INNER JOIN
                Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
            INNER JOIN
                Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
            INNER JOIN
            	GEN_Currency gc on TTMD.currency_id=gc.currency_id
            WHERE
                (@VendorType = '' OR TVM.vendor_type=@VendorType)
                AND (
        (@CategoryFilter = '' AND @CityFilter = '' AND @SegmentFilter = '')
        OR (@CategoryFilter = 'Compressor' AND @CityFilter = '' AND @SegmentFilter = '' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%'))
        OR (@CategoryFilter = 'Electrical' AND @CityFilter = '' AND @SegmentFilter = '' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%'))
        OR (@CategoryFilter = 'Rice' AND @CityFilter = '' AND @SegmentFilter = '' AND (as2.description LIKE 'RIC%'))
        OR (@CityFilter IN ('KHI', 'LHR', 'ISL') AND @CategoryFilter = '' AND @SegmentFilter = '' AND as2.description LIKE '%' + @CityFilter)
        OR (@CategoryFilter = 'Compressor' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND @SegmentFilter = '' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description LIKE '%' + @CityFilter)
        OR (@CategoryFilter = 'Electrical' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND @SegmentFilter = '' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description LIKE '%' + @CityFilter)
        OR (@CategoryFilter = 'Rice' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND @SegmentFilter = '' AND (as2.description LIKE 'RIC%') AND as2.description LIKE '%' + @CityFilter)
        OR (@SegmentFilter <> '' AND @CategoryFilter = '' AND @CityFilter = '' AND as2.description = @SegmentFilter)
        OR (@SegmentFilter <> '' AND @CategoryFilter = 'Compressor' AND @CityFilter = '' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description = @SegmentFilter)
        OR (@SegmentFilter <> '' AND @CategoryFilter = 'Electrical' AND @CityFilter = '' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description = @SegmentFilter)
        OR (@SegmentFilter <> '' AND @CategoryFilter = 'Rice' AND @CityFilter = '' AND (as2.description LIKE 'RIC%') AND as2.description = @SegmentFilter)
                ) AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) AND
            (TVM.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '') AND
            (TPM.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '') AND
            (as2.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')
            GROUP BY
                TTMD.vendor_id,
                TVM.description,
                TVM.vendor_type,
                TTDD.product_id,
                as2.description,
                TPM.product_name,
                TTDD.qty,
                TTDD.rate,
                gc.description,
                TTMD.invoice_date
            ORDER BY
                TTMD.vendor_id ASC
        """
        result = cursor.execute(
            query,
            (
                category_filter,
                city_filter,
                segment_filter,
                vendor_type,
                start_date,
                end_date,
                customer_search,
                product_search,
                warehouse_search,
            ),
        ).fetchall()
        keys = [
            "vendor_id",
            "vendor_name",
            "segment",
            "transaction_type",
            "product_id",
            "product_group",
            "qty",
            "unit_rate",
            "currency",
            "total_amount",
            "invoice_date",
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/receivables_start_end_date_search")
async def get_receivables_start_end_date(
    start_date: date = Query(
        ..., title="Start Date", description="Start date for receivables"
    ),
    end_date: date = Query(
        ..., title="End Date", description="End date for receivables"
    ),
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    ),
):
    try:
        query = """
            DECLARE @StartDate DATE = ?
            DECLARE @EndDate DATE = ?
            DECLARE @customerSearch NVARCHAR(50) = ?;

            SELECT
                ttmd.invoice_id,
                ttmd.vendor_id,
                tvm.description,
                gc.description  AS currency,
                COALESCE(SUM(ttmd.net_amount), 0) AS total_amount,
                COALESCE(SUM(ttmd.paid_amount), 0) AS total_paid_amount,
                COALESCE(SUM(ttmd.net_amount - ttmd.paid_amount), 0) AS total_receivables,
                MIN(CAST(ttmd.invoice_date AS DATETIME)) AS invoice_date
            FROM
                Trade_Transaction_Master_D ttmd
            JOIN
                GEN_Currency gc on ttmd.currency_id=gc.currency_id
            JOIN
                Trade_Vendor_Master tvm ON ttmd.vendor_id = tvm.vendor_id
            WHERE
                ttmd.active = 1
                AND ttmd.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)
                AND (tvm.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '')
            GROUP BY
                ttmd.invoice_id, ttmd.vendor_id, tvm.description,gc.description
            ORDER BY
                MIN(CAST(ttmd.invoice_date AS DATETIME))
        """
        result = cursor.execute(
            query,
            start_date,
            end_date,
            customer_search,
        ).fetchall()

        keys = [
            "invoice_id",
            "vendor_id",
            "description",
            "currency",
            "total_amount",
            "total_paid_amount",
            "total_receivables",
            "invoice_date",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/stock_aging_search")
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
    ),
    warehouse_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
):

    try:

        query = """
        
                  
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
            DECLARE @productSearch NVARCHAR(50) = ?;
            DECLARE @warehouseSearch NVARCHAR(50) = ?;

            SELECT
                p.product_id,
                p.product_name,
                as2.segment_id,
                as2.description,
                MAX(TTMD.invoice_date) AS last_invoice_date,
                DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) AS days_since_last_transaction,
                t.in_qty AS total_in_qty,
                t.out_qty AS total_out_qty,
                t.bal_qty AS total_bal_qty
            FROM
                Trade_Transaction_Master_D TTMD
            JOIN
                Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
            JOIN 
                Trade_Product_Master p ON TTDD.product_id = p.product_id
            JOIN
                Trade_Product_WareHouse_Stock t ON p.product_id = t.product_id
            JOIN
                Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
            WHERE
                p.active = 1 AND p.is_working = 1 AND t.bal_qty > 0 
                AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) AND
                (p.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '') AND
                (as2.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')
            GROUP BY
                p.product_id, p.product_name, as2.segment_id, as2.description, t.in_qty, t.out_qty, t.bal_qty
            ORDER BY
                last_invoice_date DESC    
    
            
        """

        result = cursor.execute(
            query, start_date, end_date, product_search, warehouse_search
        ).fetchall()

        keys = [
            "product_id",
            "product_name",
            "segment_id",
            "description",
            "last_invoice_date",
            "days_since_last_transaction",
            "total_in_qty",
            "total_out_qty",
            "total_bal_qty",
        ]
        result_dicts = []

        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/stock_summary_search")
async def get_stock_summary_search(
    product_id: str = Query(
        ..., title="Product ID", description="Unique identifier of the vendor."
    ),
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    ),

    warehouse_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
):
    try:
        query = """
        DECLARE @productId NVARCHAR(50) = ?;
        DECLARE @customerSearch NVARCHAR(50) = ?;
        DECLARE @warehouseSearch NVARCHAR(50) = ?;

        SELECT
            ttd.invoice_detail_id,
            TSL.VendorId AS vendor_id,
            TVM.description AS vendor_name,
            TVM.vendor_type,
            TSL.ProductId AS product_id,
            TPM.product_name,
            GC.description AS warehouse_description,
            TSL.StockMovementType,
            SUM(TSL.Qty) AS total_in_qty,
            ttmd.invoice_date,
            gttm.description,
            SUM(TSL.Amount) AS total_amount
        FROM
            Trade_StockValuationLedger TSL
        JOIN
            Trade_Vendor_Master TVM ON TSL.VendorId = TVM.vendor_id
        JOIN
            Trade_Product_Master TPM ON TSL.ProductId = TPM.product_id
        JOIN
            GEN_Companies GC ON TSL.WareHouseId = GC.company_id
        JOIN 
            Trade_Transaction_Master_D ttmd ON TVM.vendor_id = ttmd.vendor_id
        JOIN 
            GEN_Transaction_Type_Master gttm ON ttmd.Transaction_type_id = gttm.transaction_type_id 
        JOIN
            Trade_Transaction_Detail_D ttd ON ttmd.invoice_id = ttd.invoice_id
        JOIN
            Accounts_Segment as2 ON ttd.segment_id = as2.segment_id
        WHERE
            TVM.active = 1
            AND TPM.product_id = @productId AND
            (TVM.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '') AND
            (as2.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')
        GROUP BY
            ttd.invoice_detail_id,    
            TSL.VendorId,
            TVM.description,
            TVM.vendor_type,
            TSL.ProductId,
            TPM.product_name,
            GC.description,
            TSL.StockMovementType,
            ttmd.invoice_date,
            gttm.description
        ORDER BY 
            ttd.invoice_detail_id;
        """

        result = cursor.execute(
            query,
            product_id,
            customer_search,
            warehouse_search
        ).fetchall()
        keys = [
            "invoice_detail_id",
            "vendor_id",
            "vendor_name",
            "vendor_type",
            "product_id",
            "product_name",
            "warehouse_description",
            "StockMovementType",
            "total_in_qty",
            "invoice_date",
            "description",
            "total_amount",
        ]
        result_dicts = []

        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/top_selling_products_search/")
async def top_selling_products(
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    )
):
    try:
        query = """
            DECLARE @productSearch NVARCHAR(50) = ?;       

            SELECT 
                product_name,
                SUM(SOLD_QTY) AS total_sold_quantity
            FROM 
                Trade_Product_Master
            WHERE 
                (product_name LIKE '%' + @productSearch + '%' OR @productSearch = '')
            GROUP BY 
                product_name
            ORDER BY 
                total_sold_quantity DESC   
        """

        result = cursor.execute(query, product_search).fetchall()

        keys = ["product_name", "total_sold_quantity"]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/Top_Sale_or_purchase_product_by_start_end_date_search/")
async def get_product_sold_by_vendor_type(
    start_date: date = Query(
        ..., title="Start Date", description="Start date for receivables"
    ),
    end_date: date = Query(
        ..., title="End Date", description="End date for receivables"
    ),
    vendor_type: str = Query(
        ..., title="Segment Filter", description="Vendor type (Sales,Purchase,All)"
    ),
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    )
 
):
    try:

        query = """
            DECLARE @StartDate DATE = ?
            DECLARE @EndDate DATE = ?
            DECLARE @productSearch NVARCHAR(50) = ?; 
            SELECT 
                tpd.product_id,
                tpm.product_name,
                tvm.vendor_type AS transaction_type,
                SUM(tpd.qty) AS total_sold_quantity,
                MAX(ttd.invoice_date) AS last_sale_date
            FROM
                Trade_Product_Master tpm
            JOIN
                Trade_Transaction_Detail_D tpd ON tpm.product_id = tpd.product_id
            JOIN
                Trade_Transaction_Master_D ttd ON tpd.invoice_id = ttd.invoice_id
            JOIN
                Trade_Vendor_Master tvm ON ttd.vendor_id = tvm.vendor_id
            WHERE
                ttd.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate) AND
                tvm.vendor_type = ? AND
                (tpm.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '')
            GROUP BY 
                tpd.product_id, tpm.product_name, tvm.vendor_type
            ORDER BY 
                total_sold_quantity DESC
          
        """

        result = cursor.execute(
            query,
            (
                start_date,
                end_date,
                product_search,
                vendor_type,
          
            ),
        ).fetchall()

        keys = [
            "product_id",
            "product_name",
            "transaction_type",
            "total_sold_quantity",
            "last_sale_date",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)




@router.get("/vendor_summary_search/")
async def get_vendor_summary(
    customer_search: str = Query(
        "", title="customer_search", description="Unique identifier of the vendor."
    )
):
    try:
        query = """
        DECLARE @customerSearch NVARCHAR(50) = ?;       

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
        WHERE
            ttm.active = 1 AND
            v.vendor_type = 'Sale' AND
            (v.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '') 
        GROUP BY
            ttm.vendor_id, v.description, gc.description, v.vendor_type
        ORDER BY
            ttm.vendor_id ASC;

        """

        result = cursor.execute(query, customer_search).fetchall()

        keys = [
            "vendor_id",
            "vendor_name",
            "currency",
            "total_net_amount",
            "total_invoices",
            "total_paid_amount",
            "remaining_amount",
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
    
    

@router.get("/detailed_vendor_information_search")
async def get_detailed_vendor_information(
    vendor_id: str = Query(
        ..., title="Vendor ID", description="Unique identifier of the vendor."
    ),
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    )
):
    try:
        query = """
          DECLARE @productSearch NVARCHAR(50) = ?;
        DECLARE @vendorId NVARCHAR(50)=?;
           SELECT 
               tvm.vendor_id,
               ttm.invoice_id,
               ttd.invoice_detail_id,
               tvm.description AS vendor_name,
               tpm.product_name,
               YEAR(ttm.invoice_date) AS t_year,
               MONTH(ttm.invoice_date) AS t_month,
               gc.description AS currency,
               COALESCE(ttd.net_amount, 0) AS total_detailed_amount,
               COALESCE(ttm.paid_amount, 0) AS total_paid_amount
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
               ttm.active = 1 AND
               tvm.vendor_id = @vendorId AND
               (tpm.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '')
           ORDER BY 
               ttm.invoice_id
        """

        result = cursor.execute(query, product_search, vendor_id).fetchall()

        keys = [
            "vendor_id",
            "invoice_id",
            "invoice_detail_id",
            "vendor_name",
            "product_name",
            "t_year",
            "t_month",
            "currency",
            "total_paid_amount",
            "running_total_paid",
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)





# http://0.0.0.0:8000/warehouse_sales_statistics_search_cache?product_search=filter&warehouse_search=khi


# from fastapi import Query, Depends
# from fastapi.responses import JSONResponse
# import pyodbc
# import json
# from database import get_database_connection, redis_client
# from fastapi import APIRouter
# from fastapi import FastAPI, Query, HTTPException
# from pymemcache.client.base import Client as MemcacheClient
# import json
# import decimal  # Import the decimal module
# from datetime import datetime
# from endpoints.util import CustomJSONEncoder


# memcache_client = MemcacheClient(("localhost", 11211))
# router = APIRouter()
# cursor = get_database_connection()["cursor"]


# @router.get("/warehouse_sales_statistics_search_cache")
# async def get_product_sales_statistics(
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
#     product_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
#     warehouse_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
# ):
#     # Replace whitespace characters with underscores in product_search and warehouse_search
#     product_search_cleaned = product_search.replace(" ", "_")
#     warehouse_search_cleaned = warehouse_search.replace(" ", "_")

#     # Generate cache key based on cleaned inputs
#     cache_key = f"warehouse_sales_statistics_search_cache{product_search_cleaned}-{warehouse_search_cleaned}"
#     try:

#         # Try to get data from cache
#         cached_result = memcache_client.get(cache_key)
#         if cached_result:
#             return json.loads(cached_result)

#         query = """
#             DECLARE @StartDate DATE = ?;
#             DECLARE @EndDate DATE = ?;
#             DECLARE @productSearch NVARCHAR(50) = ?;
#             DECLARE @warehouseSearch NVARCHAR(50) = ?;

#             SELECT
#                 TTDD.product_id AS product_id,
#                 TPM.product_code AS product_code,
#                 TPM.product_name AS product_name,
#                 TPM.description AS product_description,
#                 GC.company_code AS warehouse_code,
#                 GC.description AS warehouse_description,
#                 TPS.in_qty AS in_quantity,
#                 TPS.out_qty AS out_quantity,
#                 TPS.bal_qty AS balance_quantity,
#                 MAX(TTMD.last_modification_datetime) AS last_change,
#                 SUM(TTDD.qty) AS total_sold_quantity,
#                 gcurr.description  AS currency,
#                 SUM(TTDD.net_amount) AS total_sales_amount,
#                 MAX(TTDD.net_amount) AS max_sales_amount,
#                 MIN(TTDD.net_amount) AS min_sales_amount
#             FROM
#                 Trade_Transaction_Master_D TTMD
#             JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             JOIN
#                 Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
#             JOIN
#                 Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
#             JOIN
#                 Trade_Product_WareHouse_Stock TPS ON TTDD.product_id = TPS.product_id
#             JOIN
#                 GEN_Companies GC ON TPS.warehouse_id = GC.company_id
#             JOIN
#                 GEN_Currency gcurr on TTMD.currency_id=gcurr.currency_id
#             WHERE
#                 (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) AND
#                 (TPM.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '') AND
#                 (GC.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')
#             GROUP BY
#                 TTDD.product_id,
#                 TPM.product_code,
#                 TPM.product_name,
#                 TPM.description,
#                 GC.company_code,
#                 GC.description,
#                 TPS.in_qty,
#                 TPS.out_qty,
#                 TPS.bal_qty,
#                 gcurr.description
#             ORDER BY
#                 TTDD.product_id
#         """

#         result = cursor.execute(
#             query, start_date, end_date, product_search, warehouse_search
#         ).fetchall()

#         keys = [
#             "product_id",
#             "product_code",
#             "product_name",
#             "product_description",
#             "warehouse_code",
#             "warehouse_description",
#             "in_quantity",
#             "out_quantity",
#             "balance_quantity",
#             "last_change",
#             "overtime_sold_quantities",
#             "currency",
#             "total_sales_amount",
#             "max_sales_amount",
#             "min_sales_amount",
#         ]

#         result_dicts = []
#         for row in result:
#             row = [
#                 str(item) if isinstance(item, decimal.Decimal) else item for item in row
#             ]
#             result_dicts.append(dict(zip(keys, row)))

#         # Store result in cache with expiration time (in seconds)
#         memcache_client.set(
#             cache_key,
#             json.dumps({"result": result_dicts}, cls=CustomJSONEncoder),
#             expire=3600,
#         )  # Cache for 1 hour

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/sales_statistics_search_cache")
# async def get_sales_statistics(
#     category_filter: str = Query(
#         ...,
#         title="Category Filter",
#         description="Category filter (Compressor, Electrical, Rice)",
#     ),
#     city_filter: str = Query(
#         ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
#     ),
#     segment_filter: str = Query(
#         ..., title="Segment Filter", description="Segment filter"
#     ),
#     vendor_type: str = Query(
#         ..., title="Segment Filter", description="Vendor type (Sales,Purchase,All)"
#     ),
#     customer_search: str = Query(
#         "", title="customer_search", description="Unique identifier of the vendor."
#     ),
#     product_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
#     warehouse_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
# ):

#     category_filter_cl = category_filter.replace(" ", "None")
#     city_filter_cl = city_filter.replace(" ", "None")
#     segment_filter_cl = segment_filter.replace(" ", "None")
#     vendor_type_cl = vendor_type.replace(" ", "None")
#     customer_search_cl = customer_search.replace(" ", "None")
#     product_search_cl = product_search.replace(" ", "None")
#     warehouse_search_cl = warehouse_search.replace(" ", "None")

#     cache_key = f"sales_statistics_search_cache{category_filter_cl}-{city_filter_cl}-{segment_filter_cl}-{vendor_type_cl}-{customer_search_cl}-{product_search_cl}-{warehouse_search_cl}"

#     try:

#         # Try to get data from cache
#         cached_result = memcache_client.get(cache_key)
#         if cached_result:
#             return json.loads(cached_result)

#         # Execute SQL query to fetch data
#         query = """
# 			DECLARE @CategoryFilter NVARCHAR(50) = ?;
#             DECLARE @CityFilter NVARCHAR(50) = ?;
#             DECLARE @SegmentFilter NVARCHAR(50) = ?;
#             DECLARE @VendorType NVARCHAR(50) = ?;
#             DECLARE @customerSearch NVARCHAR(50) = ?;
#             DECLARE @productSearch NVARCHAR(50) = ?;
#             DECLARE @warehouseSearch NVARCHAR(50) = ?;
#             SELECT
#                 TTMD.vendor_id,
#                 TVM.description AS vendor_name,
#                 as2.description AS segment,
#                 TVM.vendor_type AS transaction_type,
#                 TTDD.product_id,
#                 TPM.product_name AS product_group,
#                 TTDD.qty AS qty,
#                 TTDD.rate AS unit_rate,
#                 gc.description  AS currency,
#                 COALESCE(SUM(TTDD.amount), 0) AS total_amount,
#                 TTMD.invoice_date
#             FROM
#                 Trade_Transaction_Master_D TTMD
#             INNER JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             INNER JOIN
#                 Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
#             INNER JOIN
#                 Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
#             INNER JOIN
#                 Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
#             INNER JOIN
#             	GEN_Currency gc on TTMD.currency_id=gc.currency_id
#             WHERE
#                 (@VendorType = '' OR TVM.vendor_type=@VendorType)
#                 AND (
#         (@CategoryFilter = '' AND @CityFilter = '' AND @SegmentFilter = '')
#         OR (@CategoryFilter = 'Compressor' AND @CityFilter = '' AND @SegmentFilter = '' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%'))
#         OR (@CategoryFilter = 'Electrical' AND @CityFilter = '' AND @SegmentFilter = '' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%'))
#         OR (@CategoryFilter = 'Rice' AND @CityFilter = '' AND @SegmentFilter = '' AND (as2.description LIKE 'RIC%'))
#         OR (@CityFilter IN ('KHI', 'LHR', 'ISL') AND @CategoryFilter = '' AND @SegmentFilter = '' AND as2.description LIKE '%' + @CityFilter)
#         OR (@CategoryFilter = 'Compressor' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND @SegmentFilter = '' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description LIKE '%' + @CityFilter)
#         OR (@CategoryFilter = 'Electrical' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND @SegmentFilter = '' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description LIKE '%' + @CityFilter)
#         OR (@CategoryFilter = 'Rice' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND @SegmentFilter = '' AND (as2.description LIKE 'RIC%') AND as2.description LIKE '%' + @CityFilter)
#         OR (@SegmentFilter <> '' AND @CategoryFilter = '' AND @CityFilter = '' AND as2.description = @SegmentFilter)
#         OR (@SegmentFilter <> '' AND @CategoryFilter = 'Compressor' AND @CityFilter = '' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description = @SegmentFilter)
#         OR (@SegmentFilter <> '' AND @CategoryFilter = 'Electrical' AND @CityFilter = '' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description = @SegmentFilter)
#         OR (@SegmentFilter <> '' AND @CategoryFilter = 'Rice' AND @CityFilter = '' AND (as2.description LIKE 'RIC%') AND as2.description = @SegmentFilter)
#                 ) AND
#             (TVM.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '') AND
#             (TPM.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '') AND
#             (as2.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')
#             GROUP BY
#                 TTMD.vendor_id,
#                 TVM.description,
#                 TVM.vendor_type,
#                 TTDD.product_id,
#                 as2.description,
#                 TPM.product_name,
#                 TTDD.qty,
#                 TTDD.rate,
#                 gc.description,
#                 TTMD.invoice_date
#             ORDER BY
#                 TTMD.vendor_id ASC
#         """

#         result = cursor.execute(
#             query,
#             (
#                 category_filter,
#                 city_filter,
#                 segment_filter,
#                 vendor_type,
#                 customer_search,
#                 product_search,
#                 warehouse_search,
#             ),
#         ).fetchall()

#         keys = [
#             "vendor_id",
#             "vendor_name",
#             "segment",
#             "transaction_type",
#             "product_id",
#             "product_group",
#             "qty",
#             "unit_rate",
#             "currency",
#             "total_amount",
#             "invoice_date",
#         ]
#         result_dicts = []
#         for row in result:
#             # Convert Decimal objects to strings
#             row = [
#                 str(item) if isinstance(item, decimal.Decimal) else item for item in row
#             ]
#             result_dicts.append(dict(zip(keys, row)))

#         # Store result in cache with expiration time (in seconds)
#         memcache_client.set(
#             cache_key,
#             json.dumps({"result": result_dicts}, cls=CustomJSONEncoder),
#             expire=3600,
#         )  # Cache for 1 hour
#         return {"result": result_dicts}
#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# # http://0.0.0.0:8000/sales_statistics_search_cache?category_filter=%20&city_filter=%20&segment_filter=%20&vendor_type=Sale&customer_search=arif


# @router.get("/stock_aging_search_cache")
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
#     product_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
#     warehouse_search: str = Query(
#         "", title="product_search", description="Unique identifier of the product."
#     ),
# ):

#     # Replace whitespace characters with underscores in product_search and warehouse_search
#     product_search_cleaned = product_search.replace(" ", "_")
#     warehouse_search_cleaned = warehouse_search.replace(" ", "_")

#     # Generate cache key based on cleaned inputs
#     cache_key = (
#         f"stock_aging_search_cache{product_search_cleaned}-{warehouse_search_cleaned}"
#     )

#     # Try to get data from cache
#     cached_result = memcache_client.get(cache_key)
#     if cached_result:
#         return json.loads(cached_result)

#     try:

#         query = """


#             DECLARE @StartDate DATE = ?;
#             DECLARE @EndDate DATE = ?;
#             DECLARE @productSearch NVARCHAR(50) = ?;
#             DECLARE @warehouseSearch NVARCHAR(50) = ?;

#             SELECT
#                 p.product_id,
#                 p.product_name,
#                 as2.segment_id,
#                 as2.description,
#                 MAX(TTMD.invoice_date) AS last_invoice_date,
#                 DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) AS days_since_last_transaction,
#                 t.in_qty AS total_in_qty,
#                 t.out_qty AS total_out_qty,
#                 t.bal_qty AS total_bal_qty
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
#             WHERE
#                 p.active = 1 AND p.is_working = 1 AND t.bal_qty > 0
#                 AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) AND
#                 (p.product_name LIKE '%' + @productSearch + '%' OR @productSearch = '') AND
#                 (as2.description LIKE '%' + @warehouseSearch + '%' OR @warehouseSearch = '')
#             GROUP BY
#                 p.product_id, p.product_name, as2.segment_id, as2.description, t.in_qty, t.out_qty, t.bal_qty
#             ORDER BY
#                 last_invoice_date DESC


#         """

#         result = cursor.execute(query,start_date, end_date, product_search, warehouse_search).fetchall()

#         keys = [
#             "product_id",
#             "product_name",
#             "segment_id",
#             "description",
#             "last_invoice_date",
#             "days_since_last_transaction",
#             "total_in_qty",
#             "total_out_qty",
#             "total_bal_qty",
#         ]
#         result_dicts = []
#         for row in result:
#             # Convert Decimal objects to strings
#             row = [
#                 str(item) if isinstance(item, decimal.Decimal) else item for item in row
#             ]
#             result_dicts.append(dict(zip(keys, row)))

#         # Store result in cache with expiration time (in seconds)
#         memcache_client.set(
#             cache_key,
#             json.dumps({"result": result_dicts}, cls=CustomJSONEncoder),
#             expire=3600,
#         )  # Cache for 1 hour

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)
