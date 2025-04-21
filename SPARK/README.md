# Apache Spark



# Основы Apache Spark и RDD

## Введение

**Apache Spark** — это масштабируемая платформа для распределённой обработки данных, которая позволяет выполнять вычисления в памяти, обеспечивая высокую производительность и гибкость. Она подходит как для пакетной (batch), так и для потоковой (streaming) обработки данных.

---

##  Ключевые идеи Apache Spark

-  **Эффективная DAG-модель вычислений**  
  Spark строит направленный ациклический граф (DAG), отображающий зависимости между этапами обработки. Это обеспечивает более гибкое и оптимизированное планирование по сравнению с классическим MapReduce.

- **Ленивая модель исполнения**  
  Преобразования не выполняются сразу. Spark откладывает их выполнение до вызова операции-действия, что позволяет эффективно планировать и объединять задачи.

- **Гибкое управление памятью**  
  - Предпочтение хранения данных в памяти
  - Сброс данных на диск при нехватке ресурсов
  - Возможность комбинированного хранения
  - Поддержка различных форматов сериализации

- **Широкая поддержка языков**  
  Поддерживаются API для **Scala**, **Java**, **Python** и **R**

- **Единый API для batch и streaming обработки**

---

## RDD (Resilient Distributed Dataset)

**RDD** — основная абстракция данных в Spark, представляющая собой неизменяемую, распределённую коллекцию объектов.

### Основные свойства RDD:

- **Неизменяемость и отказоустойчивость (fault tolerance)**  
  Все операции над RDD являются детерминированными и безопасными к сбоям.

- **Два типа операций:**
  - **Transformations** — возвращают новый RDD, операции ленивые  
    Примеры: `map`, `filter`, `join`
  - **Actions** — инициируют выполнение вычислений  
    Примеры: `count`, `collect`, `save`

- **Партиционирование**  
  Данные разбиты на независимые части (partition), которые обрабатываются параллельно.

- **Кэширование данных**  
  Поддерживаются различные уровни хранения:
  - `memory`
  - `disk`
  - `memory &


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
