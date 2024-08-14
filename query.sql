------------------------------------------------
-- Delete all records in table
DELETE FROM url_data;
------------------------------------------------

------------------------------------------------
-- Get count of records
SELECT 
    COUNT(*) 
FROM url_data;
------------------------------------------------

------------------------------------------------
-- Get count of crawled url's per second
SELECT 
    ROUND(COUNT(*) / 
    EXTRACT(EPOCH FROM MAX(CAST(data->>'last_crawl_time' AS TIMESTAMP)) - MIN(CAST(data->>'last_crawl_time' AS TIMESTAMP))), 2) AS records_crawled_per_second 
FROM url_data;
------------------------------------------------

------------------------------------------------
-- Get count of successfully crawled url's per second
SELECT 
    ROUND(COUNT(*) /  
    EXTRACT(EPOCH FROM MAX(CAST(data->>'last_crawl_time' AS TIMESTAMP)) - MIN(CAST(data->>'last_crawl_time' AS TIMESTAMP))), 2) AS records_crawled_per_second 
FROM url_data
WHERE data->>'crawl_success'='true';
------------------------------------------------

------------------------------------------------
-- Get success rate of crawl attempts
SELECT 
    ROUND(
        (SUM(CASE WHEN data->>'crawl_success' = 'true' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*))::NUMERIC, 
    2) AS crawl_success_rate 
FROM url_data;SELECT 
    ROUND(
        (SUM(CASE WHEN data->>'crawl_success' = 'true' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*))::NUMERIC, 
    2) AS crawl_success_rate 
FROM url_data;
------------------------------------------------
