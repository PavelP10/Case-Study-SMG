/* Please find below scripts related to each challenge and its given task. For challenges A and B I am using SQL Server syntax. Moreover, after I ran the codes
in my environment, I replaced tables names in my environment with those provided in readme documents so you can run it right away.
I will also attach snips that my code works.

For the challenge C I created an environment in BigQuery as required in the readme document*/

------------------------------------------------------------------------------------------
-- Challenge A - Task 1
------------------------------------------------------------------------------------------

INSERT INTO ‘general_marketplaces.fct_listings’ (listing_id, price, valid_from, valid_to, listing_date_key, platform_id, product_type_id, status_id, user_id)
SELECT listing_id, price, last_update_date, NULL, last_update_date, platform_id, product_type_id, status_id, user_id
FROM (
		SELECT C.listing_id, C.price, C.listing_date_key, C.platform_id, C.product_type_id, C.status_id, C.user_id, C.creation_date, C.last_update_date
		FROM ‘general_marketplaces.fct_listings’   F
		RIGHT JOIN ‘general_marketplaces.cln_listings’   C
		ON F.listing_id = C.listing_id
		AND F.price = C.price
		AND F.listing_date_key = ISNULL(C.last_update_date,C.listing_date_key)
		AND F.platform_id = C.platform_id
		AND F.product_type_id = C.product_type_id
		AND F.status_id = C.status_id
		AND F.user_id = C.user_id
		WHERE F.valid_from < F.valid_to) AS DIM

------------------------------------------------------------------------------------------
-- Challenge A - Task 2
------------------------------------------------------------------------------------------

SELECT * FROM (
		SELECT L.*, 
		COUNT(*) OVER (PARTITION BY listing_id, L.platform_id ORDER BY L.platform_id) AS Listing_Change,
		CASE WHEN [platform] = 'Anibis.ch' THEN 'Anibis'
			 WHEN [platform] = 'Tutti.ch' THEN 'Tutti' ELSE [platform] END AS [platform],
		CASE WHEN L.valid_from <= '2021-12-31' THEN 'Before Renaming' ELSE 'After Renaming' END AS Platform_Renaming
		FROM ‘general_marketplaces.fct_listings’ L
		LEFT JOIN (SELECT DISTINCT platform_ID, 
									CASE WHEN [platform] = 'Anibis.ch' THEN 'Anibis'
										 WHEN [platform] = 'Tutti.ch' THEN 'Tutti' ELSE [platform] END AS [platform]
				   FROM ‘general_marketplaces.dim_platform’) P
		ON L.platform_id = P.platform_id
		WHERE listing_date_key BETWEEN '2021-12-01' AND '2022-01-31') A
WHERE Listing_change > 1
ORDER BY platform, listing_Id, listing_date_key

------------------------------------------------------------------------------------------
-- Challenge B - Task 1
------------------------------------------------------------------------------------------
/* I created a temp table as an input for the subtasks. */

DROP TABLE IF EXISTS #prepared_dataset

SELECT L.*, P.platform, PT.Product_type, S.Status, U.Location_city, D.weekday ,
CASE WHEN D.weekday IN ( 'Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day,
CASE WHEN L.valid_from <= '2021-12-31' THEN 'Before Renaming' ELSE 'After Renaming' END AS Platform_Renaming
INTO #prepared_dataset
FROM ‘general_marketplaces.fct_listings’ L
LEFT JOIN (SELECT DISTINCT platform_ID, CASE WHEN [platform] = 'Anibis.ch' THEN 'Anibis'
											 WHEN [platform] = 'Tutti.ch' THEN 'Tutti' ELSE [platform] END AS [platform]
			FROM‘general_marketplaces.dim_platform’) P
ON L.platform_id = P.platform_id
LEFT JOIN ‘general_marketplaces.dim_product_type’ PT
ON L.product_type_id = PT.product_type_id
LEFT JOIN ‘general_marketplaces.dim_status’ S
ON L.status_id = S.status_id
LEFT JOIN ‘general_marketplaces.dim_user’ U
ON L.User_id = U.user_id
LEFT JOIN ‘general_marketplaces.dim_date’ D
ON L.listing_date_key = D.date_key
WHERE listing_date_key BETWEEN '2021-12-01' AND '2022-01-31'

/* Task 1a */
SELECT Product_type, platform, ranking FROM (
		SELECT Product_type, platform, count(*) as cnt,row_number() over (partition by platform order by count(*) DESC) as ranking
		FROM #prepared_dataset
		WHERE Status = 'Sold'
		GROUP BY product_type, platform) Ranking
WHERE ranking <= 3

/* Task 1b */
SELECT Product_type, platform, ranking FROM (
		SELECT Product_type, platform, count(*) as cnt,row_number() over (partition by platform order by count(*)) as ranking
		FROM #prepared_dataset
		WHERE Status = 'Sold'
		GROUP BY product_type, platform) Ranking
WHERE ranking <= 3

/* Task 1c  -> Assuming the idle product types/listings are those labeled "Inactive" */
SELECT TOP 3 Product_type, SUM (Day_Amount) AS Day_Amount 
FROM (
		SELECT Product_type, platform, DATEDIFF (day, Valid_from,ISNULL(Valid_to,getdate())) AS Day_Amount
		FROM #prepared_dataset
		WHERE Status = 'Inactive') A
GROUP BY Product_type

/* Task 1d  */
SELECT Product_type, ROUND(SUM(price),2) as Amount_Sold
FROM #prepared_dataset
WHERE Status = 'Sold'
GROUP BY product_type
ORDER BY ROUND(SUM(price),2) DESC

/* Task 1e  */

/* The total amount sold by product type -> split over a week/weekend */
SELECT Product_type, ROUND(SUM(price),2) as Amount_Sold, Day
FROM #prepared_dataset
WHERE Status = 'Sold'
GROUP BY product_type, Day
ORDER BY ROUND(SUM(price),2) DESC

/* The total amount sold by product type -> split over a location -> could be also only a split The total amount sold by location */
SELECT Product_type, ROUND(SUM(price),2) as Amount_Sold, Location_city
FROM #prepared_dataset
WHERE Status = 'Sold'
GROUP BY product_type, Location_city
ORDER BY Location_city, ROUND(SUM(price),2) DESC

------------------------------------------------------------------------------------------
-- Challenge C - Task 1
------------------------------------------------------------------------------------------
/* Because the color tag field in ‘general_marketplaces.dim_product_type’  was not populated I created a function
that randomly assignes numbers to each row between 1 and 3. To each number I assigned a color. I created a new dataset that 
I subsequntly used in the BigQuery environment*/

SELECT *, CASE WHEN random_number = 1 THEN 'blue'
			   WHEN random_number = 2 THEN 'black'
			   WHEN random_number = 3 THEN 'red' ELSE 'no color' END AS color 
INTO ‘general_marketplaces.dim_product_type_random’
FROM(
		SELECT *
			, CAST(ROUND((RAND(CHECKSUM(NEWID()))*(1-3)+3),0) AS INT) as Random_Number
		FROM ‘general_marketplaces.dim_product_type’) AS RND

/* BigQuery*/

SELECT location_country, COUNT(*) AS Number_Of_Listings 
FROM (
    SELECT L.listing_id,L.listing_date_key, P.product_type, P.color, U.location_city
    , CASE U.Location_city WHEN 'Basel' THEN 'Switzerland'
                        WHEN 'Geneva' THEN 'Switzerland'
                        WHEN 'Lugano' THEN 'Switzerland'
                        WHEN 'Luzern' THEN 'Switzerland'
                        WHEN 'Zurich' THEN 'Switzerland' ELSE U.location_country END AS location_country
    FROM ‘general_marketplaces.fct_listings’` L
    LEFT JOIN ‘general_marketplaces.dim_product_type_random’ P
    ON L.product_type_id = P.product_type_id
    LEFT JOIN ‘general_marketplaces.dim_user’ U
    ON L.user_id = U.user_id)
WHERE color = 'black'
GROUP BY location_country
ORDER BY COUNT(*) DESC
LIMIT 3 


