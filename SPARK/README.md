# Apache Spark

Ниже полезные материалы, которые помогут в освоении Spark:
- [Теория по Spark в PDF](../files/spark.pdf)
- [Онлайн расчет ресурсов для Spark](https://sparkconfigoptimizer.com)
- [Как пользоваться Spark UI?](https://habr.com/ru/companies/avito/articles/764996/)
- [Leetcode по PySpark](https://platform.stratascratch.com/coding?code_type=6)


Одна из хороших практик для обучения - переписывать запросы с SQL на Spark и наоборот.
Результаты, очевидно, должны быть одинаковыми.

**Вот пример двух одинаковых запросов на SQL и на PySpark**

```sql
SELECT
    d.department_name,
    AVG(s.salary) AS average_salary
FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    JOIN salaries s ON e.employee_id = s.employee_id
WHERE s.salary >= 3000
GROUP BY d.department_name
ORDER BY average_salary DESC;
```


```python
result_df = employees_df\
                .join(departments_df, "department_id")\
                .join(salaries_df, "employee_id")
                .filter(salaries_df.salary >= 3000)
                .groupBy("department_name")
                .agg(F.avg("salary").alias("average_salary"))
                .orderBy(F.desc("average_salary"))
result_df.show()
```