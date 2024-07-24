from fastapi import Depends, Path, HTTPException
from fastapi.responses import JSONResponse
import pyodbc
from datetime import datetime,date
from database import get_database_connection,redis_client
from fastapi import APIRouter
from fastapi import Query, Depends
import json
from decimal import Decimal

router = APIRouter()
cursor = get_database_connection()["cursor"]

def update_receivables_redis_data(start_date: date, end_date: date):
    try:
        redis_key = f"receivables_{start_date}_{end_date}"
        redis_data = redis_client.get(redis_key)
        if redis_data:
            return json.loads(redis_data)

        query = """
            DECLARE @StartDate DATE = ?
            DECLARE @EndDate DATE = ?

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
            GROUP BY 
                ttmd.invoice_id, ttmd.vendor_id, tvm.description,gc.description
            ORDER BY 
                MIN(CAST(ttmd.invoice_date AS DATETIME))
        """
        result = cursor.execute(query, start_date, end_date).fetchall()

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
        result_dicts = []
        
        for row in result:
            row_dict = dict(zip(keys, row))
            
            row_dict['invoice_date'] = row_dict['invoice_date'].isoformat()
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = str(value)
            result_dicts.append(row_dict)

        redis_client.set(redis_key, json.dumps({"result": result_dicts}))
        redis_client.expire(redis_key, 1800)

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/redis_receivables_start_end_date")
async def get_receivables_start_end_date(
    start_date: date = Query(
        ..., title="Start Date", description="Start date for receivables"
    ),
    end_date: date = Query(
        ..., title="End Date", description="End date for receivables"
    ),
):
    try:
        redis_data = redis_client.get(f"receivables_{start_date}_{end_date}")
        if redis_data:
            return json.loads(redis_data)
        else:
            print("Data not available in Redis yet. Please try again later.")
            result_dicts= update_receivables_redis_data(start_date, end_date)
            return result_dicts

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
    







def update_sales_statistics_redis_data(
    category_filter: str,
    city_filter: str,
    segment_filter: str,
    vendor_type: str,

):
    try:
        redis_key = "sales_statistics_redis_data"
        redis_data = redis_client.get(redis_key)
        if redis_data:
            return json.loads(redis_data)

    
        query = """
           
			DECLARE @CategoryFilter NVARCHAR(50) = ?;
            DECLARE @CityFilter NVARCHAR(50) = ?;
            DECLARE @SegmentFilter NVARCHAR(50) = ?;
            DECLARE @VendorType NVARCHAR(50) = ?;
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
                )
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
                vendor_type
           
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
        result_dicts = []
        
        for row in result:
            row_dict = dict(zip(keys, row))
          
            row_dict['invoice_date'] = row_dict['invoice_date'].isoformat()
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = str(value)
            result_dicts.append(row_dict)

        redis_client.set(redis_key, json.dumps({"result": result_dicts}))
        redis_client.expire(redis_key, 1800)

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/redis_sales_statistics")
async def get_sales_statistics(
    category_filter: str = Query(
        ...,
        title="Category Filter",
        description="Category filter (Compressor, Electrical, Rice)",
    ),
    city_filter: str = Query(
        ...,
        title="City Filter",
        description="City filter (KHI, LHR, ISL)",
    ),
    segment_filter: str = Query(
        ...,
        title="Segment Filter",
        description="Segment filter",
    ),
    vendor_type: str = Query(
        ...,
        title="Vendor Type",
        description="Vendor type (Sales, Purchase, All)",
    ),

):
    try:
        redis_data = redis_client.get('sales_statistics_redis_data')
        if redis_data:
            return json.loads(redis_data)
        else:
            print("Data not available in Redis yet. Please try again later.")
            result_dicts= update_sales_statistics_redis_data(
            category_filter,
            city_filter,
            segment_filter,
            vendor_type,
    
        )
            return result_dicts
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)
    


def update_warehouse_redis_data():
    try:
       
        redis_key = "warehouse_sales_statistics"
        redis_data = redis_client.get(redis_key)
        if redis_data:
            return json.loads(redis_data)

        query = """
            SELECT
                TTDD.product_id AS product_id,
                TPM.product_code AS product_code,
                TPM.product_name AS product_name,
                TPM.description AS product_description,
                GC.company_code AS warehouse_code,
                gc.description AS warehouse_description,
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

        result = cursor.execute(query).fetchall()

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
        result_dicts = []
        for row in result:
            row_dict = dict(zip(keys, row))
            row_dict['last_change'] = row_dict['last_change'].isoformat()
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = str(value)

            result_dicts.append(row_dict)

        redis_client.set(redis_key, json.dumps({"result": result_dicts}))
        redis_client.expire(redis_key, 1800)

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


@router.get("/redis_warehouse_sales_statistics")
async def get_product_sales_statistics():
    try:
        redis_data = redis_client.get('warehouse_sales_statistics')
        if redis_data:
            return json.loads(redis_data)
        else:
            print("Data not available in Redis yet. Please try again later.")
            result_dicts= update_warehouse_redis_data()
            return result_dicts
       
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



def update_redis_data():
    try:

        query = """
            SELECT
                p.product_id,
                p.product_name,
                as2.segment_id,
                as2.description,
                MAX(TTMD.invoice_date) AS last_invoice_date,
                DATEDIFF(DAY, MAX(TTMD.invoice_date), GETDATE()) AS days_since_last_transaction,
                CAST(t.in_qty AS DECIMAL(18, 2)) AS total_in_qty,
                CAST(t.out_qty AS DECIMAL(18, 2)) AS total_out_qty,
                CAST(t.bal_qty AS DECIMAL(18, 2)) AS total_bal_qty
            
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
            GROUP BY
                p.product_id, p.product_name, as2.segment_id, as2.description, t.in_qty, t.out_qty, t.bal_qty
            ORDER BY
                last_invoice_date DESC;
        """

        result = cursor.execute(query).fetchall()

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
        for row in result:
            row_dict = dict(zip(keys, row))
            row_dict['last_invoice_date'] = row_dict['last_invoice_date'].isoformat()
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = str(value)
            result_dicts.append(row_dict)

        redis_client.set('whole_stock_aging', json.dumps({"result": result_dicts}))
        redis_client.expire('whole_stock_aging',1800)
        return {"result": result_dicts}
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/redis_stock_aging")
async def get_whole_stock_aging():
    try:
        redis_data = redis_client.get('whole_stock_aging')
        if redis_data:
            return json.loads(redis_data)
        else:
            print("Data not available in Redis yet. Please try again later.")
            result_dicts= update_redis_data()
            return result_dicts
       
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)


def update_redis_product_data(
    start_date: date,
    end_date: date,
    vendor_type: str
):
    try:

        query = """
            DECLARE @StartDate DATE = ?
            DECLARE @EndDate DATE = ?
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
                tvm.vendor_type = ?
            GROUP BY 
                tpd.product_id, tpm.product_name, tvm.vendor_type
            ORDER BY 
                total_sold_quantity DESC;
        """

        result = cursor.execute(query, (start_date, end_date, vendor_type)).fetchall()


        keys = [
            "product_id",
            "product_name",
            "transaction_type",
            "total_sold_quantity",
            "last_sale_date"

        ]

        result_dicts = []
        for row in result:
            row_dict = dict(zip(keys, row))
            row_dict['last_sale_date'] = row_dict['last_sale_date'].isoformat()
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = str(value)
            result_dicts.append(row_dict)

        redis_client.set('redis_Top_Sale_or_purchase_product_by_start_end_date', json.dumps({"result": result_dicts}))
        redis_client.expire('redis_Top_Sale_or_purchase_product_by_start_end_date',1800)
        return {"result": result_dicts}
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)



@router.get("/redis_Top_Sale_or_purchase_product_by_start_end_date")
async def get_whole_stock_aging(
    start_date: date = Query(
        ...,
        title="Start Date",
        description="Start date for receivables"
    ),
    end_date: date = Query(
        ...,
        title="End Date",
        description="End date for receivables"
    ),
    vendor_type: str = Query(
        ...,
        title="Type of Vendor",
        description="Type of vendor to retrieve sales data for."
    )
):
    try:
        redis_data = redis_client.get('Top_Sale_or_purchase_product_by_start_end_date')
        if redis_data:
            return json.loads(redis_data)
        else:
            print("Data not available in Redis yet. Please try again later.")
            result_dicts = update_redis_product_data(start_date, end_date, vendor_type)
            return result_dicts
    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)

