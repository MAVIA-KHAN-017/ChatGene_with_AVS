from fastapi import Query, Depends
from fastapi.responses import JSONResponse
import pyodbc
from database import get_database_connection, execute_query_with_retry
from fastapi import APIRouter
from decimal import Decimal
from datetime import date
from typing import Optional

router = APIRouter()
cursor = get_database_connection()["cursor"]
# connection = get_database_connection()["connection"]


@router.get("/invoice_statistics")
async def get_invoice_statistics_by_month(
    year: int,
    category_filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    city_filter: str = Query(
        ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
    ),
):
    try:
        query = """


        DECLARE @CategoryFilter NVARCHAR(50) = ?; 
        DECLARE @CityFilter NVARCHAR(50) = ?;   

        SELECT 
            COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Compressor' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN ttm.invoice_id END) AS Compressor_Invoices,
            COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Electrical' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN ttm.invoice_id END) AS Electrical_Invoices,
            COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Rice' AND (as2.description LIKE 'RIC%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN ttm.invoice_id END) AS Rice_Invoices,
            COUNT(DISTINCT CASE WHEN (@CityFilter = 'KHI' AND as2.description LIKE '%KHI') THEN ttm.invoice_id END) AS KHI_Invoices,
            COUNT(DISTINCT CASE WHEN (@CityFilter = 'LHR' AND as2.description LIKE '%LHR') THEN ttm.invoice_id END) AS LHR_Invoices,
            COUNT(DISTINCT CASE WHEN (@CityFilter = 'ISL' AND as2.description LIKE '%ISL') THEN ttm.invoice_id END) AS ISL_Invoices,
            COUNT(DISTINCT ttm.invoice_id) AS Total_Invoices,
            MONTH(ttm.invoice_date) AS transaction_month,
            YEAR(ttm.invoice_date) AS transaction_year
        FROM 
            Trade_Transaction_Master_D as ttm
        JOIN 
            Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
        JOIN 
            Accounts_Segment as2 ON ttd.segment_id = as2.segment_id 
        WHERE 
            YEAR(ttm.invoice_date)=?
            AND(
           (@CategoryFilter = '' OR @CityFilter = '')
        OR (@CategoryFilter = 'Compressor' AND @CityFilter = '')
        OR (@CategoryFilter = 'Electrical' AND @CityFilter = '')
        OR (@CategoryFilter = 'Rice' AND @CityFilter = '')
        OR (@CityFilter IN ('KHI', 'LHR', 'ISL') AND @CategoryFilter = '')
        OR (@CategoryFilter = 'Compressor' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description LIKE '%' + @CityFilter)
        OR (@CategoryFilter = 'Electrical' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description LIKE '%' + @CityFilter)
        OR (@CategoryFilter = 'Rice' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'RIC%') AND as2.description LIKE '%' + @CityFilter))
        AND ttm.invoice_code LIKE 'SI-%' AND ttm.active = 1
        GROUP BY
            MONTH(ttm.invoice_date),YEAR(ttm.invoice_date);
        """

        params = (
            category_filter,
            city_filter,
            year,
        )
        result = execute_query_with_retry(query, params)
        keys = [
            "Compressor_Invoices",
            "Electrical_Invoices",
            "Rice_Invoices",
            "KHI_Invoices",
            "LHR_Invoices",
            "ISL_Invoices",
            "Total_Invoices",
            "transaction_month",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)





@router.get("/invoice_KPI_statistics",tags=["Sales"])
async def get_invoice_statistics(
    category_filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    city_filter: str = Query(
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
):
    try:
        query = """
            DECLARE @CategoryFilter NVARCHAR(50) = ?; 
            DECLARE @CityFilter NVARCHAR(50) = ?;              
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;

            SELECT 
                COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Compressor' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN ttm.invoice_id END) AS Compressor_Invoices,
                COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Electrical' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN ttm.invoice_id END) AS Electrical_Invoices,
                COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Rice' AND (as2.description LIKE 'RIC%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN ttm.invoice_id END) AS Rice_Invoices,
                COUNT(DISTINCT CASE WHEN (@CityFilter = 'KHI' AND as2.description LIKE '%KHI') THEN ttm.invoice_id END) AS KHI_Invoices,
                COUNT(DISTINCT CASE WHEN (@CityFilter = 'LHR' AND as2.description LIKE '%LHR') THEN ttm.invoice_id END) AS LHR_Invoices,
                COUNT(DISTINCT CASE WHEN (@CityFilter = 'ISL' AND as2.description LIKE '%ISL') THEN ttm.invoice_id END) AS ISL_Invoices,
                COUNT(DISTINCT  ttm.invoice_id) AS Total_number_of_invoices
            FROM 
                Trade_Transaction_Master_D as ttm
            JOIN 
                Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
            JOIN 
                Accounts_Segment as2 ON ttd.segment_id = as2.segment_id 
            WHERE 
                (
                    (@CategoryFilter = '' OR @CityFilter = '')
                    OR (@CategoryFilter = 'Compressor' AND @CityFilter = '')
                    OR (@CategoryFilter = 'Electrical' AND @CityFilter = '')
                    OR (@CategoryFilter = 'Rice' AND @CityFilter = '')
                    OR (@CityFilter IN ('KHI', 'LHR', 'ISL') AND @CategoryFilter = '')
                    OR (@CategoryFilter = 'Compressor' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description LIKE '%' + @CityFilter)
                    OR (@CategoryFilter = 'Electrical' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description LIKE '%' + @CityFilter)
                    OR (@CategoryFilter = 'Rice' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'RIC%') AND as2.description LIKE '%' + @CityFilter)
                )
                AND (
                    (@StartDate ='' AND @EndDate ='')
                    OR (ttm.invoice_date BETWEEN ISNULL(@StartDate, ttm.invoice_date) AND ISNULL(@EndDate, ttm.invoice_date))
                ) AND   (ttm.invoice_code  LIKE 'SI-%' ) 
        """

        params = (
            category_filter,
            city_filter,
            start_date,
            end_date
      
        )
        result = execute_query_with_retry(query,params)

        keys = [
            "Compressor_Invoices",
            "Electrical_Invoices",
            "Rice_Invoices",
            "KHI_Invoices",
            "LHR_Invoices",
            "ISL_Invoices",
            "Total_number_of_Invoices",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)




@router.get("/today_total_invoices",tags=["Sales"])
async def get_today_sales_total():
    try:
        query = """
            SELECT 
            COUNT(DISTINCT ttmd.invoice_id) AS total_invoices
            FROM 
                Trade_Transaction_Master_D ttmd 
            where 
                CAST(ttmd.invoice_date AS DATE) = CAST(GETDATE() AS DATE)
                AND ttmd.invoice_code LIKE 'SI-%' AND ttmd.active = 1
        """

        result = execute_query_with_retry(query)

        keys = ["total_invoices"]
        result_dict = dict(zip(keys, result[0]))

        return {"result": result_dict}


    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/sales_statistics",tags=["Sales"])
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
            DECLARE @CityFilter NVARCHAR(50) = ?;
            DECLARE @SegmentFilter NVARCHAR(50) = ?;
    
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
           WITH CreditNoteAmount AS (
			    SELECT 
			        TTDD.reference_invoice__Detailid,
			        TTMD.invoice_id,
			        TTMD.invoice_code,
			        TTDD.invoice_detail_id,
			        SUM(amount) AS CreditNoteTotal
			    FROM
			        Trade_Transaction_Master_D TTMD
			    INNER JOIN
			        Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
			    WHERE 
			        TTMD.invoice_code LIKE 'CN-%' 
			    GROUP BY 
			        TTDD.reference_invoice__Detailid,
			        TTMD.invoice_id,
			        TTMD.invoice_code,
			        TTDD.invoice_detail_id
			)
            SELECT
                TTMD.vendor_id,
                TVM.description AS vendor_name,
                as2.description AS segment,
                TTMD.invoice_code,
                TVM.vendor_type AS transaction_type,
                TTDD.product_id,
                TPM.product_name AS product_group,
                TTDD.qty AS qty,
                TTDD.rate AS unit_rate,
                gc.description  AS currency,
--				COALESCE(CNA.CreditNoteTotal, 0) AS CreditNoteTotal,
--				TTDD.amount AS amount,
				TTDD.amount - COALESCE(CNA.CreditNoteTotal, 0) AS total_amount,
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
           LEFT JOIN 
    			CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid 
            WHERE
                TTMD.active = 1
                AND(
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
                ) AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))  AND (TTMD.invoice_code  LIKE 'SI-%') 
            ORDER BY
                TTMD.vendor_id ASC	
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
        """

        params = (
            category_filter,
            city_filter,
            segment_filter,
            start_date,
            end_date,
            offset,
            page_size,
        )
        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "vendor_name",
            "segment",
            "invoice_code",
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


@router.get("/sales_statistics_proc",tags=["Sales"])
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
    invoice_code: str = Query(
        ..., title="Invoice code Filter", description="invoice code"
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
        EXEC SalesStatisticsSearch 
            @CategoryFilter = ?, 
            @CityFilter = ?, 
            @SegmentFilter = ?, 
            @Invoicecode =?,
            @StartDate = ?, 
            @EndDate = ?, 
            @customerSearch = ?, 
            @productSearch = ?, 
            @warehouseSearch = ?
        """

        params = (
            category_filter,
            city_filter,
            segment_filter,
            invoice_code,
            start_date,
            end_date,
            customer_search,
            product_search,
            warehouse_search,
        )

        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "vendor_name",
            "segment",
            "invoice_code",
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


@router.get("/sales_amount_by_month_graph")
async def get_invoice_statistics_by_month(
    year: int,
    category_filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    city_filter: str = Query(
        ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
    ),
):
    try:
        query = """
        DECLARE @CategoryFilter NVARCHAR(50) = ?; 
        DECLARE @CityFilter NVARCHAR(50) = ?;              
        DECLARE @Year INT = ?;

        SELECT
            MONTH(ttm.invoice_date) AS sales_month,
            SUM(ttm.net_amount) AS total_sales
        FROM
            Trade_Transaction_Master_D ttm
        JOIN 
            Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
        JOIN 
            Accounts_Segment as2 ON ttd.segment_id = as2.segment_id 
        WHERE 
            (@CategoryFilter = '' OR (@CategoryFilter = 'Compressor' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%')) OR
            (@CategoryFilter = 'Electrical' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%')) OR
            (@CategoryFilter = 'Rice' AND as2.description LIKE 'RIC%'))
            AND
            (@CityFilter = '' OR (@CityFilter <> '' AND as2.description LIKE '%' + @CityFilter))
            AND
            (@Year = '' OR YEAR(ttm.invoice_date) = @Year) 
            AND ttm.invoice_code LIKE 'SI-%'
        GROUP BY
            MONTH(ttm.invoice_date)
        ORDER BY
            sales_month;
        """

        params = (
            category_filter,
            city_filter,
            year,
        )
        result = execute_query_with_retry(query, params)
        keys = [
            "sales_month",
            "total_sales",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/sales_statistics_by_invoice",tags=["Sales"])
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
            DECLARE @CityFilter NVARCHAR(50) = ?;
            DECLARE @SegmentFilter NVARCHAR(50) = ?;
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;
            WITH CreditNoteAmount AS (
                SELECT 
                    TTDD.reference_invoice__Detailid,
                    TTMD.invoice_id,
                    TTMD.invoice_code,
                    TTDD.invoice_detail_id,
                    SUM(amount) AS CreditNoteTotal
                FROM
                    Trade_Transaction_Master_D TTMD
                INNER JOIN
                    Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id  
                WHERE 
                    TTMD.invoice_code LIKE 'CN-%' 
                GROUP BY 
                    TTDD.reference_invoice__Detailid,
                    TTMD.invoice_id,
                    TTMD.invoice_code,
                    TTDD.invoice_detail_id
            )
            SELECT 
                TTMD.invoice_id,
                TTDD.invoice_detail_id,
                as2.description AS segment,
                TTMD.invoice_code, 
                TVM.vendor_type AS transaction_type,
                TPM.product_id,
                TPM.product_name AS product_group,
                TTDD.qty,
                TTDD.rate AS unit_rate,
                gc.description AS currency,
            --    COALESCE(CNA.CreditNoteTotal, 0) AS credit_note_amount,
            
                TTDD.amount - COALESCE(CNA.CreditNoteTotal, 0) AS total_amount,
                TTMD.invoice_date
            FROM 
                Trade_Transaction_Detail_D TTDD
            JOIN
                Trade_Transaction_Master_D TTMD ON TTMD.invoice_id = TTDD.invoice_id  
            JOIN
                    Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
            JOIN
                Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
            JOIN
                Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
            JOIN
                GEN_Currency gc ON TTMD.currency_id = gc.currency_id
            LEFT JOIN 
                CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid 
            WHERE 
                TTMD.active = 1
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
                )
                AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
                AND (TTMD.invoice_code LIKE 'SI-%')
            ORDER BY
                TTMD.invoice_date  DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
        """

        params = (
            category_filter,
            city_filter,
            segment_filter,
            start_date,
            end_date,
            offset,
            page_size,
        )
        result = execute_query_with_retry(query, params)
        keys = [
            "invoice_id",
            "invoice_detail_id",
            "segment",
            "invoice_code",
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


@router.get("/sales_statistics_by_invoice_proc",tags=["Sales"])
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
    invoice_id: str = Query(
        ..., title="Invoice code Filter", description="invoice code"
    ),
    invoice_code: str = Query(
        ..., title="Invoice code Filter", description="invoice code"
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
    product_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
    warehouse_search: str = Query(
        "", title="product_search", description="Unique identifier of the product."
    ),
):

    try:

        query = """
        EXEC SalesStatisticsSearchByInvoice 
            @CategoryFilter = ?, 
            @CityFilter = ?, 
            @SegmentFilter = ?, 
            @Invoiceid=?,
            @Invoicecode =?,
            @StartDate = ?, 
            @EndDate = ?, 
            @productSearch = ?, 
            @warehouseSearch = ?
        """

        params = (
            category_filter,
            city_filter,
            segment_filter,
            invoice_id,
            invoice_code,
            start_date,
            end_date,
            product_search,
            warehouse_search,
        )

        result = execute_query_with_retry(query, params)
        keys = [
            "invoice_id",
            "invoice_detail_id",
            "segment",
            "invoice_code",
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


@router.get("/sales_summary",tags=["Sales"])
async def get_invoice_summary(
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
    start_date: str = Query(
        None, title="Start Date", description="Start date for invoices"
    ),
    end_date: str = Query(None, title="End Date", description="End date for invoices"),
    # customer_search: str = Query(
    #     "", title="customer_search", description="Unique identifier of the vendor."
    # ),
    page: int = Query(1, title="Page", description="Page number"),
    page_size: int = Query(
        10, title="Page Size", description="Number of items per page"
    ),
):
    try:
        offset = (page - 1) * page_size
        query = """
DECLARE @CategoryFilter NVARCHAR(50) = ?;
            DECLARE @CityFilter NVARCHAR(50) = ?;
            DECLARE @SegmentFilter NVARCHAR(50) = ?;
            DECLARE @StartDate DATE = ?;
            DECLARE @EndDate DATE = ?;

            WITH CreditNoteAmount AS (
                            SELECT
                    TTDD.reference_invoice__Detailid,
                    TTMD.invoice_id,
                    TTMD.invoice_code,
                    TTDD.invoice_detail_id,
                    SUM(amount) AS CreditNoteTotal,
                    SUM(TTMD.net_amount) AS B_CN_net_amount,
                    SUM(TTMD.tax_amount) AS B_CN_total_tax
                FROM
                    Trade_Transaction_Master_D TTMD
                INNER JOIN
                    Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
                WHERE
                    TTMD.invoice_code LIKE 'CN-%'
                GROUP BY
                    TTDD.reference_invoice__Detailid,
                    TTMD.invoice_id,
                    TTMD.invoice_code,
                    TTDD.invoice_detail_id
            )
            SELECT
                TVM.vendor_id,
                TVM.description AS customer_name,
                count(DISTINCT TTMD.invoice_id) AS total_invoice,
                gc.description  AS currency,
--                SUM(TTMD.total_amount) AS B_CN_amount,
--                SUM(CNA.CreditNoteTotal) AS CreditNoteTotal,
                SUM(TTMD.total_amount) - COALESCE(SUM(CNA.CreditNoteTotal),0) AS total_amount,
--                SUM(TTMD.tax_amount) AS B_CN_total_tax,
--                SUM(DISTINCT CNA.B_CN_total_tax) AS CNA_tax_amount,
                SUM(TTMD.tax_amount) - COALESCE(SUM(DISTINCT CNA.B_CN_total_tax),0) AS total_tax,
--                SUM(TTMD.net_amount) AS B_CN_net_amount,
--                SUM(DISTINCT CNA.B_CN_net_amount) AS CreditNoteTotalAmount,
                SUM(TTMD.net_amount) - COALESCE(SUM(DISTINCT CNA.B_CN_net_amount),0) AS total_net_amount,
                SUM(TTDD.qty) AS total_quantity
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
                        LEFT JOIN
                            CreditNoteAmount CNA ON TTDD.invoice_detail_id = CNA.reference_invoice__Detailid
            WHERE
                TTMD.active = 1
                AND(
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
                )
                AND(@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate)) AND
                TTMD.active = 1 AND
                (TTMD.invoice_code  LIKE 'SI-%')
            GROUP BY
                TVM.vendor_id, TVM.description, gc.description
            ORDER BY
                TVM.vendor_id ASC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
        """
        params = (category_filter,
            city_filter,
            segment_filter, start_date, end_date, offset, page_size)
        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "customer_name",
            "total_invoice",
            "currency",
            "total_amount",
            "total_tax",
            "total_net_amount",
            "total_quantity",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)





@router.get("/Sale_Summary_Search_proc",tags=["Sales"])
async def get_Sale_Summary_Search(
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
    )
):

    try:

        query = """
        EXEC SaleSummarySearch 
            @CategoryFilter = ?, 
            @CityFilter = ?, 
            @SegmentFilter = ?, 
            @StartDate = ?, 
            @EndDate = ?, 
            @customerSearch = ? 
        """

        params = (
            category_filter,
            city_filter,
            segment_filter,
            start_date,
            end_date,
            customer_search
        )

        result = execute_query_with_retry(query, params)
        keys = [
            "vendor_id",
            "customer_name",
            "total_invoice",
            "currency",
            "total_amount",
            "total_tax",
            "total_net_amount",
            "total_quantity"
        ]

        result_dicts = [dict(zip(keys, row)) for row in result]
        return {"result": result_dicts}
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)













# @router.get("/product_sales_summary")
# async def get_product_sales_summary(
#     start_date: date = Query(
#         None, title="Start Date", description="Start date for invoices"
#     ),
#     end_date: date = Query(
#         None, title="End Date", description="End date for invoices"
#     ),
#     vendor_id: str = Query(None, title="Vendor ID", description="ID of the vendor"),
# ):
#     try:
#         query = """
#                     DECLARE @StartDate DATE = ?;
#             DECLARE @EndDate DATE = ?;
#             SELECT
#                 TPM.product_id,
#                 TPM.product_name,
#                 as2.description AS warehouse,
#                 TTMD.invoice_code,
#                 SUM(TTDD.qty) AS total_quantity_sold,
#                 SUM(TTDD.amount) AS total_sales_amount
#             FROM
#                 Trade_Transaction_Master_D TTMD
#             INNER JOIN
#                 Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#             INNER JOIN
#                 Trade_Product_Master TPM ON TTDD.product_id = TPM.product_id
#             INNER JOIN
#                 Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
#             INNER JOIN
#                 Trade_Vendor_Master TVM ON TTMD.vendor_id = TVM.vendor_id
#             WHERE
#                 TTMD.active = 1
#                 AND (TTMD.invoice_code LIKE 'SI-%')
#                 AND TVM.vendor_id = ?
#                 AND (@StartDate = '' OR TTMD.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
#             GROUP BY
#                 TPM.product_id, TPM.product_name, as2.description, TTMD.invoice_code
#             ORDER BY
#                 TPM.product_id ASC;
#         """
#         params = (start_date, end_date,vendor_id)
#         result = execute_query_with_retry(query, params)

#         keys = [
#             "product_id",
#             "product_name",
#             "warehouse",
#             "invoice_code",
#             "total_quantity_sold",
#             "total_sales_amount"
#         ]
#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)
