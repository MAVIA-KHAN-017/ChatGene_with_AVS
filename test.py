# api



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
        
                  
            DECLARE @StartDate DATE ='';
            DECLARE @EndDate DATE ='';
	   		
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
                fr.qty,
            --    fr.invoice_date,
            --    fr.running_qty,
            --    fr.amount,
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
                (@StartDate = '' OR fr.invoice_date BETWEEN @StartDate AND DATEADD(DAY, 1, @EndDate))
            
            ORDER BY 
                fr.last_invoice_date DESC;

            
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
                """

        params = (start_date, end_date, offset, page_size)
        result = execute_query_with_retry(query, params)

        keys = [
            "product_id",
            "product_name",
            "bal_qty",
            "qty",
            # "invoice_date",
            # "running_qty",
            # "amount",
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










# REDIS


# def update_receivables_redis_data():
#     try:
#         redis_key = "receivables_start_date_end_date"
#         redis_data = redis_client.get(redis_key)
#         if redis_data:
#             return json.loads(redis_data)

#         query = """
#             SELECT 
#                 ttmd.invoice_id,
#                 ttmd.vendor_id,
#                 tvm.description,
#                 gc.description AS currency,
#                 COALESCE(SUM(ttmd.net_amount), 0) AS total_amount,
#                 COALESCE(SUM(ttmd.paid_amount), 0) AS total_paid_amount,
#                 COALESCE(SUM(ttmd.net_amount - ttmd.paid_amount), 0) AS total_receivables,
#                 MIN(CAST(ttmd.invoice_date AS DATETIME)) AS invoice_date 
#             FROM 
#                 Trade_Transaction_Master_D ttmd 
#             JOIN
#                 GEN_Currency gc ON ttmd.currency_id = gc.currency_id
#             JOIN
#                 Trade_Vendor_Master tvm ON ttmd.vendor_id = tvm.vendor_id
#             WHERE 
#                 ttmd.active = 1
#             GROUP BY 
#                 ttmd.invoice_id, ttmd.vendor_id, tvm.description, gc.description
#         """
#         result = execute_query_with_retry(query)

#         keys = [
#             "invoice_id",
#             "vendor_id",
#             "description",
#             "currency",
#             "total_amount",
#             "total_paid_amount",
#             "total_receivables",
#             "invoice_date",
#         ]
#         result_dicts = []

#         for row in result:
#             row_dict = dict(zip(keys, row))
#             row_dict["invoice_date"] = row_dict["invoice_date"].isoformat()
#             for key, value in row_dict.items():
#                 if isinstance(value, Decimal):
#                     row_dict[key] = str(value)
#             result_dicts.append(row_dict)

#         redis_client.set(redis_key, json.dumps({"result": result_dicts}))
#         redis_client.expire(redis_key, 1800)

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


# @router.get("/redis_receivables_start_end_date") # type: ignore
# async def get_receivables_start_end_date():
#     try:
#         redis_key = "receivables_start_date_end_date"
#         redis_data = redis_client.get(redis_key) # type: ignore
#         if redis_data:
#             return json.loads(redis_data) # type: ignore
#         else:
#             result_dicts = update_receivables_redis_data()
#             return result_dicts

#     except pyodbc.Error as e: # type: ignore
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500) # type: ignore
#     except Exception as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500) # type: ignore




@router.get("/receivables_with_filter",tags=["Stock"])
async def receivables_with_filter(
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
    customer_Search: str = Query(
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
                DECLARE @customerSearch NVARCHAR(50) = ?;
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
                        AND  (@CategoryFilter = '' OR (@CategoryFilter = 'Compressor' AND (gttm.description LIKE '%AT%' OR gttm.description LIKE '%PMP%')) OR
                        (@CategoryFilter = 'Electrical' AND (gttm.description LIKE '%ABB%' OR gttm.description LIKE '%CHT%' OR gttm.description LIKE '%ABB-RTL%' OR gttm.description LIKE '%GE%')) OR
                        (@CategoryFilter = 'Rice' AND gttm.description LIKE '%RIC%'))
                        AND (@Segment = '' OR gttm.description LIKE '%' + @Segment + '%')
                        AND (@CityFilter = '' OR gttm.description LIKE '%' + @CityFilter)
                        AND (TVM.description LIKE '%' + @customerSearch + '%' OR @customerSearch = '')
                        AND (TTMD.invoice_code  LIKE 'SI-%')
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
                    vendor_id;

        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
                """


        params = (Category_Filter, Segment, City_Filter, customer_Search, start_date, end_date, offset, page_size)
        result = execute_query_with_retry(query, params)




        keys = [
            "invoice_id",
            "invoice_detail_id",
            "vendor_id",
            "vendor_description",
            "total_amount",
            "tax_amount",
            "paid_amount",
            "currency",
            "CreditNoteTotal",
            "CNA_tax_amount",
            "CreditNoteTotalAmount",
            "CNA_paid_amount"
        ]
        result_dicts = [dict(zip(keys, row)) for row in result]

        return {"result": result_dicts}

    except pyodbc.Error as e:
        error_message = {"error": str(e)}
        return JSONResponse(content=error_message, status_code=500)













@router.get("/total_receivables_with_filter",tags=["receivables"])
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
    customer_Search: str = Query(
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
    ),


):
    try:

        query = """


            DECLARE @CategoryFilter NVARCHAR(50) = ?;
            DECLARE @Segment NVARCHAR(50) = ?;
            DECLARE @CityFilter NVARCHAR(50) = ?;
            DECLARE @customerSearch NVARCHAR(50) = ?;
                DECLARE @StartDate DATE = '?';
                        DECLARE @EndDate DATE = '?'
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
                    AND  (@CategoryFilter = '' OR (@CategoryFilter = 'Compressor' AND (gttm.description LIKE '%AT%' OR gttm.description LIKE '%PMP%')) OR
                    (@CategoryFilter = 'Electrical' AND (gttm.description LIKE '%ABB%' OR gttm.description LIKE '%CHT%' OR gttm.description LIKE '%ABB-RTL%' OR gttm.description LIKE '%GE%')) OR
                    (@CategoryFilter = 'Rice' AND gttm.description LIKE '%RIC%'))
                    AND (@Segment = '' OR gttm.description LIKE '%' + @Segment + '%')
                    AND (@CityFilter = '' OR gttm.description LIKE '%' + @CityFilter)
                    AND (TTMD.invoice_code  LIKE 'SI-%')
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


         OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
                """


        params = (Category_Filter, Segment, City_Filter, customer_Search, start_date, end_date, offset, page_size)
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