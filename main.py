from fastapi import FastAPI


from fastapi.middleware.cors import CORSMiddleware
from endpoints import cache_apis, kpi, product, statistics_invoice_sales, stock, util, vendor,receivables,proc
import os


app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(kpi.router)
app.include_router(vendor.router)
app.include_router(product.router)
app.include_router(statistics_invoice_sales.router)
app.include_router(stock.router)
app.include_router(receivables.router)
# app.include_router(cache_apis.router)
# app.include_router(proc.router)
# app.include_router(redis_enpoints.router)

# @app.get("/sales_statistics_by_calender")
# async def get_sales_statistics(
#     start_date: str = Query(
#         ..., title="Start Date", description="Start date of the period"
#     ),
#     end_date: str = Query(..., title="End Date", description="End date of the period"),
#     cursor: pyodbc.Cursor = Depends(get_database_connection),
# ):
#     try:
#         query = f"""

#         SELECT
#             TTDD.invoice_id,
#             TTDD.invoice_detail_id,
#             FORMAT(TTMD.invoice_date, 'dddd') AS day_of_week,
#             TTMD.invoice_date AS invoice_date,
#             as2.description AS segment_name,
#             TTDD.product_id AS product_id,
#             TTDD.qty AS quantity,
#             TTDD.net_amount AS total_amount,
#             SUM(TTDD.net_amount) AS total_sales_amount
#         FROM
#             Trade_Transaction_Master_D TTMD
#         JOIN
#             Trade_Transaction_Detail_D TTDD ON TTMD.invoice_id = TTDD.invoice_id
#         JOIN
#             Accounts_Segment as2 ON TTDD.segment_id = as2.segment_id
#         WHERE
#             TTMD.invoice_date >= ?
#             AND TTMD.invoice_date < ?
#         GROUP BY
#             TTDD.invoice_id,
#             TTDD.invoice_detail_id,
#             FORMAT(TTMD.invoice_date, 'dddd'),
#             TTMD.invoice_date,
#             as2.description,
#             TTDD.product_id,
#             TTDD.qty,
#             TTDD.net_amount
#         ORDER BY
#             TTMD.invoice_date, TTDD.product_id;
#         """

#         result = cursor.execute(query, start_date, end_date).fetchall()

#         keys = [
#             "invoice_id",
#             "invoice_detail_id",
#             "day_of_week",
#             "invoice_date",
#             "product_id",
#             "quantity",
#             "total_amount",
#             "total_invoices",
#             "total_sales_amount",
#         ]
#         result_dicts = [dict(zip(keys, row)) for row in result]

#         return {"result": result_dicts}

#     except pyodbc.Error as e:
#         error_message = {"error": str(e)}
#         return JSONResponse(content=error_message, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
