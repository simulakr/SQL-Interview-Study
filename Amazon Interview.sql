--1. Identify customers who made purchases on exactly three different days in the last month. 

WITH purchases_last_month as (
  SELECT customer_id, COUNT(DISTINCT purchase_date) as purchases
from purchases 
GROUP by customer_id
  WHERE purchase_date >= dateadd(month, -1, current_date) 

SELECT customer_id FROM purchases_last_month 
  where purchases =3 
  
--2.Find the top 2 highest-selling products for each category. 

WITH sales_rank as (
SELECT s.product_id, SUM(s.sale_amount) as total_sales , p.category,
  RANK() OVER(PARTITION BY category 
              ORDER BY SUM(s.sale_amount) DESC) as rnk
  from sales s 
  JOIN products p on s.product_id = p.product_id
  GROUP by p.category, s.product_id)
  
  SELECT p.category, s.product_id, total_sales
  from sales_rank 
  where rnk <=2 
  
--3. Find the top 1 highest-selling products for toys category. 
  
  SELECT TOP 1 category, product_id, sum(sale_amount) as total_sales 
  from sales 
  WHERE category = 'toys'
  GROUP by category, product_id 
  order by 3 desc 
    
--4. Detect anomalies where sales for a product are %50 lower than the average for that product. 
  
WITH product_stats as(
SELECT product_id, AVG(sale_amount) as avg_sales 
  from sales 
  GROUP by product_id) 

SELECT s.product_id, s.sale_amount
  from sales s join product_stats p 
  on s.product_id = p.product_id 
  WHERE sale_amount < 0.5 * avg_sales
  
--5. Find employess who have never been a manager and have worked in more than one department.   
    
WITH managers as(
  SELECT employee_id,name,  manager_id 
  from employees 
  WHERE manager_id is not null
GROUP by employee_id),
  
  more_depts as(
SELECT employee_id, COUNT(DISTINCT department_id) as work_dept_count 
  from employees 
    GROUP by employee_id)

SELECT m.employee_id, m.name, m.manager_id, md.work_dept_count   
  from managers m inner JOIN more_depts md 
  on m.employee_id  = md.employee_id

--6.Calculate the median salary each department. 
  
WITH ranked_salaries as(
  SELECT department_id, salary, 
  ROW_NUMBER() OVER(PARTITION BY department_id 
                   Order by salary) as row_num,
  COUNT(*) OVER(PARTITION BY department_id ) as total_rows 
  from employees) 
 
SELECT department_id, AVG(salary) as median_salary 
  from ranked_salaries 
  WHERE row_num in ((FLOOR(total_rows+1)/2 ),CEIL((total_rows+1)/2))
  GROUP by department_id

--7. Find how many employees each manager. 
  
SELECT manager_id, count(*) as employee_count 
  from employees 
  GROUP by manager_id 
  ORDER by 2 desc
  
--8. Divide the employees into 4 groups based on their salaries.

SELECT employee_id, salary, 
  NTILE(4) OVER(ORDER BY salary desc) as group_salary 
  from employees 
  order by salary desc

--9. Bring in the highest paid employee in each department.

 SELECT employee_id, name, department_id, salary
FROM (
    SELECT employee_id, name, department_id, salary, 
           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num
    FROM employees
) t
WHERE row_num = 1

--10. Find Average salary and number of total personel in each department.
  
SELECT department_id, avg(salary) as avg_salary, count(*) as personal_count
  from employees 
  GROUP by department_id 
  order by personal_count desc
   
--11. Identify customers who purchased products from all available categories. 
  
WITH categories_per_customer as (
  SELECT p.customer_id, count(DISTINCT pr.category) as cat_count 
  from purchases as p 
  join products pr on p.product_id = pr.product_id 
  GROUP by customer_id),
  
  total_categories as(
  SELECT DISTINCT category as category_counts
  from products)
  
  SELECT customer_id 
  from categories_per_customer, total_categories
  where cat_count= category_counts
  
--12. Find Average unit price and number of total products in each department.
  
  SELECT category, COUNT(*) as product_count, AVG(unit_price) as avg_price 
  from products 
  GROUP by category 
  order by COUNT(*) DESC
  
--13. Top 3 highest price product in each category.
  
  WITH price_category as(
  SELECT product_id, unit_price,
  RANK() OVER(PARTITION BY category 
              ORDER BY unit_price DESC) AS price_rank
  from products)
  
  SELECT product_id, unit_price
  from price_category 
  WHERE price_rank <=3 
  
  