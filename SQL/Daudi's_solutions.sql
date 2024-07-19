--------------------------------TASK 1----------------------------------
CREATE DATABASE lil_analysis; -- creating the database

-- creating the table 
CREATE TABLE llin_distirbution(
ID INT,
`Number distributed` INT,
location VARCHAR(50),
country VARCHAR(50),
year INT,
by_whom VARCHAR(50),
country_code VARCHAR(50)
);


-------------------------------TASK 2 ----------------------------------
SELECT 
		DISTINCT
		SUM(distributions.`Number distributed`) OVER(PARTITION BY distributions.country) AS LLNS, -- total no of llns by country
		distributions.Country
FROM 
		lin_analysis.llin_distribution AS distributions;
	
	
SELECT 
		DISTINCT
		AVG(distributions.`Number distributed`) OVER(PARTITION BY distributions.country) AS LLNS, -- average no of llns by country
		distributions.Country
FROM 
		lin_analysis.llin_distribution AS distributions;
	
	
	
SELECT 
		MAX(distributions.year) AS Latest_distribution_date, 
		MIN(distributions.year) AS Earliest_distribution_date
FROM 
		lin_analysis.llin_distribution AS distributions;	
	
	
----------------------------------TASK 3---------------------------------
SELECT 
		SUM(`Number distributed`) AS tot_no_of_llms, -- total number of llns
		distributions.by_whom 
		distributions.year
FROM
		lin_analysis.llin_distribution AS distributions
GROUP BY 
		distributions.by_whom , -- grouping by organization
year;



SELECT 
		SUM(distributions.`Number distributed`) AS tot_no_of_llms, -- total no of llns
		distributions.year
FROM
		lin_analysis.llin_distribution AS distributions
GROUP BY 
		distributions.year; -- grouping by year
	
-----------------------------------TASK 4-------------------------------
SELECT DISTINCT
		location,
		MAX(`Number distributed`)AS highest_no_of_llns, -- finding the maximum number of llns
		MIN(`Number distributed`) AS lowest_no_of_llns  -- finding the minimum number of llns
FROM
		lin_analysis.llin_distribution AS distributions
GROUP BY
		location; -- grouping by location


	
WITH llns AS(	
SELECT  DISTINCT
		by_whom ,
		MAX(`Number distributed`)AS highest_no_of_llns, -- finding the maximum number of llns
		MIN(`Number distributed`) AS lowest_no_of_llns  -- finding the minimum number of llns
FROM
		lin_analysis.llin_distribution AS distributions
GROUP BY
		by_whom)   -- grouping by organization
SELECT 
		*,
		(highest_no_of_llns -  lowest_no_of_llns) AS difference -- calculated column for the significant differences
FROM
		llns;
	
	

		
--------------------------------TASK 5----------------------------------
-- Outlier Investigation
SELECT 
		`Number distributed`, 
		 location ,
		 year
FROM
		lin_analysis.llin_distribution AS distributions  -- my datasource
WHERE 
		`Number distributed` > 4000000;  -- Max number is  around 3M overally , so any value so far from that is an outlier
