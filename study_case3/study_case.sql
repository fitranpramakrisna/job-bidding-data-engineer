-- 1.  Tulis query untuk menghasilkan nama lokasi, kuartal (Q1, Q2, Q3, Q4), dan total kapasitas kontainer
SELECT 
    pd.location, 
    CASE 
    WHEN pcc.month IN (1, 2, 3) THEN 'Q1'
    WHEN pcc.month IN (4, 5, 6) THEN 'Q2'
    WHEN pcc.month IN (7, 8, 9) THEN 'Q3'
    WHEN pcc.month IN (10, 11, 12) THEN 'Q4'
END AS quarter,
    SUM(pcc.capacity) AS total_capacity
FROM port_container_capacity pcc
JOIN port_details pd ON pcc.port_id = pd.port_id
GROUP BY pd.location, quarter
ORDER BY pd.location, quarter;


-- 2.Tulis query untuk mendapatkan lokasi, rata-rata kapasitas tahunan, dan perbedaan rata-rata kapasitas antara kedua lokasi.
WITH avg_capacity AS (
    SELECT 
    pd.location, 
    pcc.year, 
    AVG(pcc.capacity) AS avg_capacity
    FROM port_container_capacity pcc
    JOIN port_details pd ON pcc.port_id = pd.port_id
    GROUP BY pd.location, pcc.year
)
SELECT 
    ac1.location AS location1, 
    ac2.location AS location2, 
    ac1.year, 
    ac1.avg_capacity AS avg_capacity_location1, 
    ac2.avg_capacity AS avg_capacity_location2, 
    (ac1.avg_capacity - ac2.avg_capacity) AS capacity_difference
FROM avg_capacity ac1
JOIN avg_capacity ac2 
    ON ac1.year = ac2.year AND ac1.location <> ac2.location
ORDER BY ac1.year, ac1.location;


-- 3. Tulis query untuk mendapatkan nama pelabuhan, lokasi, dan bulan-bulan di mana kapasitas meningkat dibandingkan bulan sebelumnya.
WITH capacity_prev_month AS (
    SELECT 
    pd.port_name, 
    pd.location, 
    pcc.year, 
    pcc.month, 
    pcc.capacity, 
    LAG(pcc.capacity) OVER (PARTITION BY pcc.port_id, pcc.year ORDER BY pcc.month) AS prev_capacity
    FROM port_container_capacity pcc
    JOIN port_details pd ON pcc.port_id = pd.port_id
)
SELECT * 
FROM capacity_prev_month
WHERE capacity > prev_capacity
ORDER BY port_name, year, month;
