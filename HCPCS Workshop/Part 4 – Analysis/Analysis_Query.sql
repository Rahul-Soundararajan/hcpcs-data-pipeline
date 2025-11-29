
# 1. Write a query to count the number of codes in each group.

SELECT  group_code, count(*) as Count   
  FROM HCPCS.hcpcs_codes
  GROUP BY group_code;


# 2. Show the top 5 categories with the highest number of codes.

SELECT category_name 
  FROM 
    (SELECT  group_code,
      category_name,
      COUNT(*) AS Count   
      FROM HCPCS.hcpcs_codes
	  GROUP BY group_code, category_name
	  ORDER BY Count DESC
	) AS Sub
  LIMIT 5;


# 3. Find all codes that have changed descriptions over time (same hcpcs_code, different long_description).

SELECT hcpcs_code 
  FROM
    (SELECT hcpcs_code,
       COUNT(DISTINCT long_description) AS versions
	   FROM hcpcs_codes
	   GROUP BY hcpcs_code
       HAVING COUNT(DISTINCT long_description) > 1
	   AND MAX(is_current) = 'Y'
    ) AS Sub;
 
 
# 4. Show codes that were active in 2022 but expired before 2024.                   
SELECT *
FROM hcpcs_codes
WHERE effective_date <= '2022-12-31'   
  AND end_date IS NOT NULL
  AND end_date < '2024-01-01';
		
		