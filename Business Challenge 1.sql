-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- The script below explains various different queries for our business challenge project, different queries might go im
-- different directions or might not have been used for the end result at all. This document serves as a accumulated overview of queries tested.
-- ---------------------------------------------------------------------------------------------------------------------------------------------
-- This file is for Team 8 Business Challenge
-- The entire team contributed different approaches, ideas,financial and analytical insights and finally developed the codes together
-- Team members are Adejumo Ebunoluwa, Heesu Kim, Jasper Van Westing, and Sreekanth Kanna


USE invest;

SELECT a.date, a.ticker, 
		a.value, 																					-- current value
		a.lagged_price_12, a.lagged_price_18,  a.lagged_price_24,									-- value xx months ago
		(a.value - a.lagged_price_12) / a.lagged_price_12 as returns_12,							-- return
        (a.value - a.lagged_price_18) / a.lagged_price_18 as returns_18,
        (a.value - a.lagged_price_24) / a.lagged_price_24 as returns_24
        
FROM
        (SELECT  *, EXTRACT(YEAR from date) AS year, EXTRACT(MONTH from date) AS month,					
				LAG(value, 12) OVER ( PARTITION BY ticker ORDER BY date) AS lagged_price_12,			-- value of 12 month ago
				LAG(value, 18) OVER ( PARTITION BY ticker ORDER BY date) AS lagged_price_18,			-- value of 18 month ago
                LAG(value, 24) OVER ( PARTITION BY ticker ORDER BY date) AS lagged_price_24				-- value of 24 month ago

		FROM pricing_daily_new
        
		WHERE price_type = 'Adjusted' 
                AND EXTRACT(YEAR from date) > 2018														-- Limit to recent data
              
		GROUP BY year, month,ticker		
		ORDER BY ticker, date DESC																		-- order by lastest information
		) AS a
WHERE EXTRACT(YEAR from date) = 2022 AND EXTRACT(month from date) =9									-- most recent information per tickers
;

-- ---------------------------------------------------------------------------------------------------------------
-- Q1. What is the most recent 12M*, 18M, and 24M (months) return for each of the securities (and for the entire portfolio)? 
-- most recent 12M**, 18M, 24M (months) return for each of the securities
-- For entire portfolio return for random customer
-- ---------------------------------------------------------------------------------------------------------------



SELECT 	b.customer_id,
		SUM(b.returns_12*b.allocation) AS return_12m,
		SUM(b.returns_18*b.allocation) AS return_18m,
        SUM(b.returns_24*b.allocation) AS return_24m

FROM(
			SELECT 	a.date, a.ticker, a.customer_id,
					a.value, 																					-- current value
					a.lagged_price_12, a.lagged_price_18,  a.lagged_price_24,									-- value xx months ago
					IFNULL((a.value - a.lagged_price_12) / a.lagged_price_12,0) as returns_12,							-- return
					IFNULL((a.value - a.lagged_price_18) / a.lagged_price_18,0) as returns_18,
					IFNULL((a.value - a.lagged_price_24) / a.lagged_price_24,0) as returns_24,
					a.quantity/@total_security AS allocation
					
			FROM
					(SELECT  pdn.date, pdn.ticker, pdn.value, hc.quantity, cd.customer_id,
							EXTRACT(YEAR from pdn.date) AS year, EXTRACT(MONTH from pdn.date) AS month,					
							LAG(pdn.value, 12) OVER ( PARTITION BY pdn.ticker ORDER BY pdn.date) AS lagged_price_12,			-- value of 12 month ago
							LAG(pdn.value, 18) OVER ( PARTITION BY pdn.ticker ORDER BY pdn.date) AS lagged_price_18,			-- value of 18 month ago
							LAG(pdn.value, 24) OVER ( PARTITION BY pdn.ticker ORDER BY pdn.date) AS lagged_price_24				-- value of 24 month ago

					FROM customer_details AS cd
					INNER JOIN account_dim AS ad
					ON cd.customer_id = ad.client_id
					INNER JOIN holdings_current AS hc
					ON ad.account_id= hc.account_id
					INNER JOIN security_masterlist AS sm
					ON hc.ticker = sm.ticker
					INNER JOIN pricing_daily_new AS pdn
					ON sm.ticker = pdn.ticker
					
					WHERE 	hc.price_type = 'Adjusted' 
							AND EXTRACT(YEAR from pdn.date) > 2019														-- Limit to recent data
							AND cd.customer_id IN(	32,19,77)															-- SELECT RANDOM CUSTOMER
					GROUP BY year, month,ticker		
					ORDER BY ticker, date DESC																		-- order by lastest information
					) AS a
			WHERE EXTRACT(YEAR from date) = 2022 AND EXTRACT(month from date) =9									-- most recent information per tickers
			
            
		) AS b
        
GROUP BY b.customer_id;


-- query below creates a view that will calculate the returns per day per ticker where price type is adjusted and from 3 years untill now
DROP VIEW IF EXISTS invest.jt_returns;

CREATE VIEW invest.jt_return AS

SELECT a.date, a.ticker, a.value, a.lagged_price, a.price_type,
	(a.value-a.lagged_price) / a.lagged_price AS `returns`
FROM 
	(SELECT *, LAG(value, 1) OVER (
									PARTITION BY ticker
                                    ORDER BY `date`
                                    ) AS lagged_price
FROM pricing_daily_new
WHERE price_type = 'Adjusted' 
AND `date` > '2019-09-09') AS a;

-- Query below returns the mean, standard deviation and risk adjusted returns per ticker 
SELECT ticker,
AVG(returns) AS mu,
STD(returns) AS sigma,
AVG(returns)/STD(returns) AS risk_adj_returns
FROM invest.jt_return
GROUP BY ticker
ORDER BY risk_adj_returns ASC;

-- 5 BEST tickers are CEG, BIL, CTA, SHV, TSLA
-- 5 WORST tickers are UPAR, THCX, POTX, BNDX, EOPS

-- Query below joins all tables together and returns total value and risk adjusted returns of portfolios.
SELECT (hc.value * hc.quantity) AS total_portfolio_value, customer_id,
AVG(returns) AS mu,
STD(returns) AS sigma,
AVG(returns)/STD(returns) AS risk_adj_returns
FROM customer_details AS cd
INNER JOIN account_dim AS ad
ON cd.customer_id = ad.client_id
INNER JOIN holdings_current AS hc
USING (account_id)
INNER JOIN jt_return AS jtr
USING (ticker)

GROUP BY customer_id
ORDER BY risk_adj_returns DESC
LIMIT 100;

-- BEST performing portfolios customer_id over the last 3 years (128, 193, 999, 186, 99)
-- WORST performing portfolios customer_id over the last 3 years (497, 554, 29, 539, 25)

-- 1. check the worst (sell)
-- 2. link to clients (joins)
-- 3. link to asset classes (spread the portfolio, diversify)
-- 4. Check clients' portfolio

-- join tables with returns (weighted average of individual stock returns)




----------------------------------------------------------------------
----------------------------------------------------------------------

-- its also important to get the best/worst portfolio over the last 2 years
----------------------------------------------------------------------
CREATE VIEW invest.jt_return2 AS

SELECT a.date, a.ticker, a.value, a.lagged_price, a.price_type,
	(a.value-a.lagged_price) / a.lagged_price AS `returns`
FROM 
	(SELECT *, LAG(value, 1) OVER (
									PARTITION BY ticker
                                    ORDER BY `date`
                                    ) AS lagged_price
FROM pricing_daily_new
WHERE price_type = 'Adjusted' 
AND `date` > '2020-09-09') AS a;

SELECT (hc.value * hc.quantity) AS total_portfolio_value, customer_id,
AVG(returns) AS mu,
STD(returns) AS sigma,
AVG(returns)/STD(returns) AS risk_adj_returns
FROM customer_details AS cd
INNER JOIN account_dim AS ad
ON cd.customer_id = ad.client_id
INNER JOIN holdings_current AS hc
USING (account_id)
INNER JOIN jt_return2 AS jtr2
USING (ticker)

GROUP BY customer_id
ORDER BY risk_adj_returns DESC
LIMIT 100;


----------------------------------------------------------------------
----------------------------------------------------------------------

-- -- its also important to get the best/worst portfolio over the last year
-- with the best/worst portfolio over the last couple of years, we can systematically identify portfolios that have consistently performed good or bad
-- and from those insights we can choose the portfolios of which we want to learn, to those which we want to improve
-- for our analysis we chose to improve customer_id 497 and 29 as they were consistently bad
-- and we would take the portfolios of customer_id 128 and 99 as examples as they performed consistently good

----------------------------------------------------------------------

CREATE VIEW invest.jt_return4 AS

SELECT a.date, a.ticker, a.value, a.lagged_price, a.price_type,
	(a.value-a.lagged_price) / a.lagged_price AS `returns`
FROM 
	(SELECT *, LAG(value, 1) OVER (
									PARTITION BY ticker
                                    ORDER BY `date`
                                    ) AS lagged_price
FROM pricing_daily_new
WHERE price_type = 'Adjusted' 
AND `date` > '2021-09-09') AS a;

SELECT (hc.value * hc.quantity) AS total_portfolio_value, customer_id,
AVG(returns) AS mu,
STD(returns) AS sigma,
AVG(returns)/STD(returns) AS risk_adj_returns
FROM customer_details AS cd
INNER JOIN account_dim AS ad
ON cd.customer_id = ad.client_id
INNER JOIN holdings_current AS hc
USING (account_id)
INNER JOIN jt_return2 AS jtr2
USING (ticker)

GROUP BY customer_id
ORDER BY risk_adj_returns DESC
LIMIT 100;


-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- Below query to get all the asset classes of the tickers, so we could make an analysis of what asset classes might be more risky than others
-- Further queries around major asset classes were done in Excel as the server would not run the queries anymore due to overage (losing connection)
SELECT ticker, major_asset_class, minor_asset_class
FROM security_masterlist
GROUP BY ticker;