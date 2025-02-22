-- --1.Total Pendapatan per ChannelGrouping di 5 Negara dengan Pendapatan Tertinggi--
WITH TopCountries AS (
    SELECT country, SUM(productRevenue) AS total_revenue
    FROM sales_data  
    GROUP BY country
    ORDER BY total_revenue DESC
    LIMIT 5
)
SELECT 
    sd.channelGrouping, 
    sd.country, 
    SUM(sd.productRevenue) AS total_revenue
FROM sales_data sd
JOIN TopCountries tc ON sd.country = tc.country
GROUP BY sd.channelGrouping, sd.country
ORDER BY total_revenue DESC;
-- CTE (WITH TopCountries AS (...)) digunakan untuk mendapatkan 5 negara dengan total pendapatan tertinggi.
-- Query utama kemudian mengambil total pendapatan berdasarkan channelGrouping hanya untuk negara-negara tersebut.
-- Data diurutkan berdasarkan pendapatan tertinggi.


-- 2.Analisis Perilaku Pengguna
-- (a) Menghitung rata-rata timeOnSite, pageviews, dan sessionQualityDim
SELECT 
    AVG(timeOnSite) AS avg_time_on_site,
    AVG(pageviews) AS avg_pageviews,
    AVG(sessionQualityDim) AS avg_session_quality
FROM sales_data sd ;

-- Query ini menghitung rata-rata waktu di situs, jumlah halaman yang dikunjungi, dan kualitas sesi pengguna.

-- (b) Menghitung nilai rata-rata timeOnSite dan pageviews terlebih dahulu
SELECT 
    sd.fullVisitorId,
    AVG(sd.timeOnSite) AS user_avg_time_on_site,
    AVG(sd.pageviews) AS user_avg_pageviews
FROM sales_data sd
CROSS JOIN (
    SELECT 
        AVG(timeOnSite) AS avg_time_on_site,
        AVG(pageviews) AS avg_pageviews
    FROM sales_data
) AS AvgMetrics
GROUP BY sd.fullVisitorId
HAVING 
    AVG(sd.timeOnSite) > (SELECT AVG(timeOnSite) FROM sales_data) 
    AND AVG(sd.pageviews) < (SELECT AVG(pageviews) FROM sales_data);


-- 3.Analisis Kinerja Produk
(a) Menghitung total pendapatan, total kuantitas terjual, dan total refund per produk
SELECT 
    v2ProductName,
    SUM(productRevenue) AS total_revenue,
    SUM(productQuantity) AS total_quantity_sold,
    SUM(productRefundAmount) AS total_refund
FROM sales_data
GROUP BY v2ProductName;
-- Query ini menjumlahkan pendapatan, jumlah produk terjual, dan total refund untuk setiap produk (v2ProductName).

-- (b) Mengurutkan produk berdasarkan pendapatan bersih dan menandai produk dengan refund > 10% dari total revenue
SELECT 
    v2ProductName,
    SUM(productRevenue) AS total_revenue,
    SUM(productRefundAmount) AS total_refund,
    (SUM(productRevenue) - SUM(productRefundAmount)) AS net_revenue,
    CASE 
        WHEN SUM(productRefundAmount) > 0.1 * SUM(productRevenue) THEN 'FLAGGED'
        ELSE 'OK'
    END AS refund_status
FROM sales_data
GROUP BY v2ProductName
ORDER BY net_revenue DESC;
-- Query Menghitung pendapatan bersih (total_revenue - total_refund).
-- Menandai produk dengan FLAGGED jika refund lebih dari 10% dari pendapatan total.
-- Mengurutkan berdasarkan pendapatan bersih tertinggi.







