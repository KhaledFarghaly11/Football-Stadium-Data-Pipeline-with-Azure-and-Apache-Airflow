-- Top 5 stadiums by capacity --
SELECT top 10 rank, stadium, capacity
FROM stadiums
ORDER BY capacity DESC;

-- Average capacity by region --
SELECT region, AVG(capacity) as average_capacity
FROM stadiums
GROUP BY region
ORDER BY average_capacity DESC;

-- Count of stadiums in each country --
SELECT country, count(country) stadium_count
FROM stadiums
GROUP BY country
ORDER BY stadium_count DESC, country ASC;

-- Stadium ranking within each region --
SELECT stadium, region, rank as global_rank,
    RANK() OVER(PARTITION BY region ORDER BY capacity DESC) as region_rank
FROM stadiums;

-- Top 3 stadium ranking within each region --
SELECT stadium, region, capacity, rank as global_rank, region_rank
FROM (
    SELECT rank, stadium, region, capacity,
           RANK() OVER (PARTITION BY region ORDER BY capacity DESC) as region_rank
    FROM stadiums
) ranked_stadiums
WHERE region_rank <= 3;

-- Stadiums with capacity above average --
SELECT stadium, t2.region, capacity, avg_capacity
FROM stadiums, (SELECT region, AVG(capacity) avg_capacity FROM stadiums GROUP BY region) t2
WHERE t2.region = stadiums.region
AND capacity > avg_capacity
ORDER BY region;

-- Stadiums with the closest capacity to regional median --
WITH MedianCTE AS (
    SELECT
        region, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacity) OVER (PARTITION BY region) AS median_capacity
    FROM stadiums
)
SELECT rank, stadium, region, capacity, ranked_stadiums.median_rank
FROM (
    SELECT
        s.rank, s.stadium, s.region, s.capacity,
        ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY ABS(s.capacity - m.median_capacity)) AS median_rank
    FROM stadiums s JOIN MedianCTE m ON s.region = m.region
) ranked_stadiums
WHERE median_rank = 1;