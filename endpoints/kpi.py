from fastapi import Depends, Query
from fastapi.responses import JSONResponse
import pyodbc
from database import get_database_connection,execute_query_with_retry
from fastapi import APIRouter

router = APIRouter()
cursor = get_database_connection()["cursor"]
# connection = get_database_connection()["connection"]


 


# @router.get("/vendor_KPI_statistics")
# async def get_vendor_statistics(
#     category_filter: str = Query(
#         ...,
#         title="Category Filter",
#         description="Category filter (Compressor, Electrical, Rice)",
#     ),
#     city_filter: str = Query(
#         ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
#     ),
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
# ):
#     try:
#         query = """
#             DECLARE @CategoryFilter NVARCHAR(50) = ?; 
#             DECLARE @CityFilter NVARCHAR(50) = ?;              
#             DECLARE @StartDate DATE = ?;
#             DECLARE @EndDate DATE = ?;

#             SELECT 
#                 COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Compressor' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN tvm.vendor_id END) AS Compressor_customers,
#                 COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Electrical' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN tvm.vendor_id END) AS Electrical_customers,
#                 COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Rice' AND (as2.description LIKE 'RIC%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN tvm.vendor_id END) AS Rice_customers,
#                 COUNT(DISTINCT CASE WHEN (@CityFilter = 'KHI' AND as2.description LIKE '%KHI') THEN tvm.vendor_id END) AS KHI_customers,
#                 COUNT(DISTINCT CASE WHEN (@CityFilter = 'LHR' AND as2.description LIKE '%LHR') THEN tvm.vendor_id END) AS LHR_customers,
#                 COUNT(DISTINCT CASE WHEN (@CityFilter = 'ISL' AND as2.description LIKE '%ISL') THEN tvm.vendor_id END) AS ISL_customers,
#                 COUNT(DISTINCT  tvm.vendor_id) AS Total_number_of_customers
#                         FROM 
#                             Trade_Transaction_Master_D ttm
#                         JOIN 
#                             Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
#                         JOIN 
#                             Accounts_Segment as2 ON ttd.segment_id = as2.segment_id 
#                         JOIN
#                             Trade_Vendor_Master tvm ON ttm.vendor_id = tvm.vendor_id 
#             WHERE 
#                     tvm.vendor_type='Sale' AND
#                 (
#                     (@CategoryFilter = '' OR @CityFilter = '')
#                     OR (@CategoryFilter = 'Compressor' AND @CityFilter = '')
#                     OR (@CategoryFilter = 'Electrical' AND @CityFilter = '')
#                     OR (@CategoryFilter = 'Rice' AND @CityFilter = '')
#                     OR (@CityFilter IN ('KHI', 'LHR', 'ISL') AND @CategoryFilter = '')
#                     OR (@CategoryFilter = 'Compressor' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description LIKE '%' + @CityFilter)
#                     OR (@CategoryFilter = 'Electrical' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description LIKE '%' + @CityFilter)
#                     OR (@CategoryFilter = 'Rice' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'RIC%') AND as2.description LIKE '%' + @CityFilter)
#                 )
#                 AND (
#                     (@StartDate ='' AND @EndDate ='')
#                     OR (ttm.invoice_date BETWEEN ISNULL(@StartDate, ttm.invoice_date) AND ISNULL(@EndDate, ttm.invoice_date))
#                 );



#         """

#         result = cursor.execute(
#             query, (category_filter, city_filter, start_date, end_date)
#         ).fetchall()

#         keys = [
#             "Compressor_customers",
#             "Electrical_customers",
#             "Rice_customers",
#             "KHI_customers",
#             "LHR_customers",
#             "ISL_customers",
#             "Total_number_of_customers",
#         ]

#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)
  


# @router.get("/product_KPI_statistics")
# async def get_product_statistics(
#     category_filter: str = Query(
#         ...,
#         title="Category Filter",
#         description="Category filter (Compressor, Electrical, Rice)",
#     ),
#     city_filter: str = Query(
#         ..., title="City Filter", description="City filter (KHI, LHR, ISL)"
#     ),
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
# ):
#     try:
#         query = """

   
#         DECLARE @CategoryFilter NVARCHAR(50) = ?; 
#         DECLARE @CityFilter NVARCHAR(50) = ?;              
#         DECLARE @StartDate DATE = ?;
#         DECLARE @EndDate DATE = ?;

#         SELECT 
#             COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Compressor' AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN tpm.product_id END) AS Compressor_products,
#             COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Electrical' AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN tpm.product_id END) AS Electrical_products,
#             COUNT(DISTINCT CASE WHEN (@CategoryFilter = 'Rice' AND (as2.description LIKE 'RIC%') AND (@CityFilter = '' OR as2.description LIKE '%' + @CityFilter)) THEN tpm.product_id END) AS Rice_products,
#             COUNT(DISTINCT CASE WHEN (@CityFilter = 'KHI' AND as2.description LIKE '%KHI') THEN tpm.product_id END) AS KHI_products,
#             COUNT(DISTINCT CASE WHEN (@CityFilter = 'LHR' AND as2.description LIKE '%LHR') THEN tpm.product_id END) AS LHR_products,
#             COUNT(DISTINCT CASE WHEN (@CityFilter = 'ISL' AND as2.description LIKE '%ISL') THEN tpm.product_id END) AS ISL_products,
#             COUNT(DISTINCT  tpm.product_id) AS Total_number_of_products
#                     FROM 
#                         Trade_Transaction_Master_D ttm
#                     JOIN 
#                         Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
#                     JOIN 
#                         Accounts_Segment as2 ON ttd.segment_id = as2.segment_id 
#                     JOIN
#                         Trade_Product_Master tpm ON ttd.product_id = tpm.product_id
#         WHERE 
#             (
#                 (@CategoryFilter = '' OR @CityFilter = '')
#                 OR (@CategoryFilter = 'Compressor' AND @CityFilter = '')
#                 OR (@CategoryFilter = 'Electrical' AND @CityFilter = '')
#                 OR (@CategoryFilter = 'Rice' AND @CityFilter = '')
#                 OR (@CityFilter IN ('KHI', 'LHR', 'ISL') AND @CategoryFilter = '')
#                 OR (@CategoryFilter = 'Compressor' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'AT%' OR as2.description LIKE 'PMP%') AND as2.description LIKE '%' + @CityFilter)
#                 OR (@CategoryFilter = 'Electrical' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'ABB%' OR as2.description LIKE 'CHT%' OR as2.description LIKE 'ABB-RTL%' OR as2.description LIKE 'GE%') AND as2.description LIKE '%' + @CityFilter)
#                 OR (@CategoryFilter = 'Rice' AND @CityFilter IN ('KHI', 'LHR', 'ISL') AND (as2.description LIKE 'RIC%') AND as2.description LIKE '%' + @CityFilter)
#             )
#             AND (
#                 (@StartDate ='' AND @EndDate ='')
#                 OR (ttm.invoice_date BETWEEN ISNULL(@StartDate, ttm.invoice_date) AND ISNULL(@EndDate, ttm.invoice_date))
#             );
#         """

#         result = cursor.execute(
#             query, (category_filter, city_filter, start_date, end_date)
#         ).fetchall()

#         keys = [
#             "Compressor_Products",
#             "Electrical_Products",
#             "Rice_Products",
#             "KHI_Products",
#             "LHR_Products",
#             "ISL_Products",
#             "Total_number_of_Products",
#         ]

#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)



@router.get("/sales_growth")
async def get_sales_growth(
    current_year: str = Query(
        ..., title="Current Year", description="Current sales year"
    ),
    previous_year: str = Query(
        ..., title="Previous Year", description="Previous sales year"
    ),
):
    try:
        query = """
            DECLARE @currentyear NVARCHAR(20) = ?;
            DECLARE @previousyear NVARCHAR(20) = ?;

            SELECT
                @currentyear AS sales_year,
                COUNT(DISTINCT CASE WHEN YEAR(ttm.invoice_date) = @currentyear THEN ttd.invoice_detail_id END) AS current_year_sales,
                COUNT(DISTINCT CASE WHEN YEAR(ttm.invoice_date) = @previousyear THEN ttd.invoice_detail_id END) AS previous_year_sales,
                CASE
                    WHEN COUNT(DISTINCT CASE WHEN YEAR(ttm.invoice_date) = @previousyear THEN ttd.invoice_detail_id END) = 0 THEN 100  
                    ELSE ((COUNT(DISTINCT CASE WHEN YEAR(ttm.invoice_date) = @currentyear THEN ttd.invoice_detail_id END) - COUNT(DISTINCT CASE WHEN YEAR(ttm.invoice_date) = @previousyear THEN ttd.invoice_detail_id END)) * 100.0) / NULLIF(COUNT(DISTINCT CASE WHEN YEAR(ttm.invoice_date) = @previousyear THEN ttd.invoice_detail_id END), 0)
                END AS sales_growth_percentage
            FROM 
                Trade_Transaction_Master_D ttm
            JOIN 
                Trade_Transaction_Detail_D ttd ON ttm.invoice_id = ttd.invoice_id
            WHERE YEAR(ttm.invoice_date) IN (@currentyear, @previousyear);

        """

        params = (
            current_year,
            previous_year,
          
        )
        result = execute_query_with_retry(query,params)
        keys = [
            "sales_year",
            "current_year_sales",
            "previous_year_sales",
            "sales_growth_percentage",
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts[0]}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
 

@router.get("/today_total_invoices",tags=["Sales"])
async def get_receivables_today_total():
    try:
        query = """
            SELECT 
            COUNT(DISTINCT ttmd.invoice_id) AS total_invoices
            FROM 
                Trade_Transaction_Master_D ttmd 
            where 
                CAST(ttmd.invoice_date AS DATE) = CAST(GETDATE() AS DATE)
                AND ttmd.invoice_code LIKE 'SI-%' 
        """

        result = execute_query_with_retry(query)

        keys = ["total_invoices"]
        result_dict = dict(zip(keys, result[0]))

        return {"result": result_dict}


    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
    
    
    

@router.get("/today_total_product_sales")
async def get_receivables_today_total():
    try:
        query = """
            SELECT 
            COUNT(DISTINCT  tpm.product_id) AS total_products
            FROM 
                Trade_Transaction_Master_D ttmd 
                    JOIN 
                        Trade_Transaction_Detail_D ttd ON ttmd.invoice_id = ttd.invoice_id
                    JOIN 
                        Accounts_Segment as2 ON ttd.segment_id = as2.segment_id 
                    JOIN
                        Trade_Product_Master tpm ON ttd.product_id = tpm.product_id
            where 
                CAST(ttmd.invoice_date AS DATE) = CAST(GETDATE() AS DATE)
                AND ttmd.invoice_code LIKE 'SI-%'
        """
   
        result = execute_query_with_retry(query)
        keys = ["total_products"]
        result_dict = dict(zip(keys, result[0]))

        return {"result": result_dict}


    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
