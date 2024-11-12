
Статью подготовил: Шустиков Владимир. (https://t.me/Shust_DE)

## Напутственные слова перед изучением материала

**!!!Сюда стоит лезть, после изучения курсов по Python!!!**

Данная статья охватывает основы работы с оркестратором AirFlow. Рассмотрим кратко теорию которую спрашивают достаточно часто, посмотрим как локально развернуть AirFlow с помощью docker-compose и рассмотрим простой пример одного из тестовых заданий, который охватывает достаточно большие возможности AirFlow.

Как говорил мой дед: "Я твой дед!". Вы можете спросить, "А к чему ты это написал?", а я вам отвечу "Хз. Живите теперь с этим!"

Приятного погружения.

-----------------
<h1 style="text-align: center;">AIRFLOW</h1>

Возьмём определение с [официального репозитория AirFlow](https://github.com/apache/airflow).

**AirFlow** — это платформа для программирования, планирования и мониторинга рабочих процессов.

А в общем и целом нужно запомнить, что AirFlow это **оркестратор** (не ELT-инстремент), в котором есть возможность прописывать ETL процессы, на языке Python. Каждый такой процес представляет собой DAG, состоящий из определённых задач, на сленге правильно говорить тасок.

<p align="center">
    <img src="./../png/airflow_logo.png" alt="AirFlow"/>
</p>

## DAG

DAG(Directed Acyclic Graph, направленный ациклический граф) представлет из себя набор такос идущих последовательно друг за дружкой либо параллельно и которые нельзя зациклить по кругу, т.е. своего рода строится прямолинейный конвеер обработки данных.

<p align="center">
    <img src="./../png/af_dag.png" alt="AirFlow" />
</p>

## Архитектура AirFlow

AirFlow состоит из трёх основных, взаимосвязанных компонентов:

* Планировщик AirFlow(scheduler)
* Исполнитель(Executor)
* Воркеры (workers)
* Веб-сервер AirFlow

<p align="center">
    <img src="./../png/af_arfitecture.png" alt="AirFlow" />
</p>

### Задачи планировщика

- Анализ графа;
- Проверка параметра **scheduler_interval**, который определяет частоту выполнения DAG;
- Планирование очереди графов.

### Задачи Исполнителя

- Запуск задач; 
- Распеделение задач между воркерами.

### Задачи воркеров

- Получает задачи от исполнителя;
- Отвечает за полное выполнение задач.

### Задачи Веб-сервера AirFlow

- Визуализация DAG'а, который проанализировал планировщик;
- Предоставлять интерфейс пользователю для отслеживания работы DAG.

## Инициализация DAG и основные параметры





## Операторы AirFlow

Как вам уже известно конвеер обработки является направленныйм ацикличным графом, в свою очередь DAG состит из определённых задач, которые определяются **операторами**. 

В AirFlow существует множество операторов, с из полным списком можно ознакомиться [здесь](https://airflow.apache.org/docs/apache-airflow-providers/operators-and-hooks-ref/index.html)

Вот примеры, часто встречаемых операторов, с которыми мы познакомимся в примере ниже:

* PythonOperator
* BashOperator
* PostgresOperator

## Передача данных между задачами.
 
Существует 2 метода передачи данных между тасками в AirFlow:

1. Механизм XCom;
2. сохраниение данных в хранилищах.

### Механизм XCom

XCom - позволяется обмениться сообщениями между задачами. Предназначен он исключительно для **небольших данных**. 

Согласно документации в замисимости от используемой базой данных метаинформации:

* SQLLite - до 2х Гб.
* PostreSQL - до 1 Гб.
* MySQL - до 64 Кб.

**Запомните** XCom можно использовать для передачи небольших объемов данных, например значение агрегации, колличества строк в файле, даже можно небольшой файл передать, но в остальных случаях используйте внешние решения для хранения данных, как пример, сохраняйте все в каnалог tmp и потом забирайте данные от туда.

Определяется Xcom 2мя способами:

* с помощью команд **xcom_push** и **xcom_pull**
* с помощью **Taskflow API** (декоратор **@task**)


---------
## Пример тестового задания

Необходимо написать DAG который будет выполнять следующие задачи:
 
1. С помощью PythonOperator необходимо сгенерировать тестовые данные и записать их в файл в каталог /tmp/data.csv ( для простоты можно взять 2 колонки id, value )
2. С помощью BashOperator переместить файл в каталог /tmp/processed_data
3. C помощью PythonOperator нужно загрузить данные из файла в таблицу в Postgres ( таблицу можно предварительно создать )
4. После записи данных в таблицу последним таском выведите в логах сообщение о количестве загруженных данных.
 
С помощью XCom необходимо:
 
Передать путь до файла из п.1 в оператор в п.2.
Передать количество записей из п.3 в п.4

### Запуск DAG'а

Для начала развернём с помощью Docker compose AirFlow. 

Жми и копируй [docker-compose.yaml](./task/docker-compose.yaml) в свой каталог.

С помощью WSL или любой командной строки, где ты работаешь с Docker, переходишь в каталог со скаченым или скопированным файлом и выполняешь команду:

```
docker compose up -d
```

Пока у тебя качаются и поднимаются контейнеры скачай следующий файл [test_task.py](./task/test_task.py)

После того как контейнеры поднимутся, в каталоге с **docker-compose.yaml** появятся 4 каталога config, dags, logs, plugins.

Необходимо скаченый файл **test_task.py** переместить в каталог **dags**.

Теперь заходим в браузер и в URL-строке прописываем:

```
localhost:8080
```

тем самым мы переходим на стартовую страницу.

<p align="center">
    <img src="./../png/af_start_page.png" alt="AirFlow" />
</p>

```
Логин: airflow.
Пароль: airflow.
```

После входа нас перебрасывает на главную страницу, где мы увидим DAG, который мы скинули в каталог(если его там нет подождите минут 5 и обносите страницу, в крайнем случае рестартаните Docker compose)

Перед тем как запустить DAG, необходимо прописать коннект к БД PostgreSQL, которая присутствует в docker compose файле. 

Для это в верхней строке меню наводим курсор на **Admin** и выбираетм пункт **Connections**. Нажимаем синий плюсик в открвшимся окне подключения прописываем следующие параметры:

|Параметр| Значение|
|---|---|
|Connection Id|psql_connection|
|Connection Type|Postgres|
|Host|host.docker.internal|
|Database|user|
|Login|user|
|Password|user|
|Port|5431|
|Extra|могут быть ковычки, необходимо оставить пустое поле, иначе будет ошибка подключения.|


<p align="center">
    <img src="./../png/af_connection.png" alt="AirFlow" />
</p>

Возвращаемся на главную страницу и заходим в наш DAG жамкув по его имени. 

Далее запускаем DAG, нажав на **Trigger GAD**(треугольник) в правом верхнем углу страницы.

Всё поздавляю, вы запустили свой первый DAG, теперь разберёмся с кодом написанным в DAG'е.

### Поясняю за код

Для начала необходимо инициализировать DAG с помощью класса DAG, для этого импортируем класс из библиотеки airflow.

```
from airflow import DAG
```
Инициализация DAG спроисходит следующим образом:

```
dag = DAG(
    dag_id="load_file_to_psql",
    default_args=default_args
)
```
Для того чтобы не захламлять сам класс DAG, аргументы можно передать через параментр **default_args** и вывести словать с параметрами отдельно.

```
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2024, 11, 13),
  'email': ['airflow@example.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'schedule_interval': None,
}
```

Далее берём первый пунт задания. Для генерации файла я использую библиотеку pandas. Следующий код генерирует данные и сохраняет их в файл.

```
 path_filename = '/tmp/data.csv'
 table = [(i, md5(int(i).to_bytes(8, 'big', signed=True)).hexdigest()) for i in range(1, 100)]
  table = pd.DataFrame(table, columns=['id', 'md5_id'])
  table.to_csv(path_filename, index=False)
```

Помещаем данный фрагмент в функцию **_generate_file**. Но не забываем, что путь к файлу нам необходимо передать через XCom. Поэтому необходимо в функцию передать все аргументы DAG'а, т.е. переменную **kwargs**. Из данного словаря нам нужен ключ **ti**(или task_instans, это одно и тоже, ссылка на один и тот же объект). Поэтому приравниваем его к переменной **ti**, чтобы не запутаться.

```
ti = kwargs['ti']
```

Далее необходимо передать у объекста вызвать метод **xcom_push** и передать ключ и значение XCom переменной.

```
ti.xcom_push(key='path_file', value=path_filename)
```

Объединяем и получаем полноценную функцию **_generate_file**:

```
def _generate_file(**kwargs):
  ti = kwargs['ti']
  path_filename = '/tmp/data.csv'
  table = [(i, md5(int(i).to_bytes(8, 'big', signed=True)).hexdigest()) for i in range(1, 100)]
  table = pd.DataFrame(table, columns=['id', 'md5_id'])
  table.to_csv(path_filename, index=False)
  ti.xcom_push(key='path_file', value=path_filename)
```
В PythonOperator нам просто нужно передать данную функцию следующим образом:

```
generate_file = PythonOperator(
    task_id='generate_file',
    python_callable=_generate_file,
    dag=dag,
)
```


За второй пункт отвечает следующая конструкция:

```
move_data_file = BashOperator(
  task_id="move_data_file",
  bash_command=("mkdir -p /tmp/processed_data/ && "
          "mv {{ ti.xcom_pull(task_ids='generate_file', key='path_file') }} /tmp/processed_data/"),
  dag=dag,
)
```

Здесь применяется команда, создания каталога и проверки его на существование

```
mkdir -p /tmp/processed_data/
```

и соотвественно перемещение самого файла

```
mv {{ ti.xcom_pull(task_ids='generate_file', key='path_file') }} /tmp/processed_data/
```

Как можно заметить, так как нам нужно достать файл из XCom в Bash-конструкцию, мы воспользовалить Jinja-шаблонизатором для языка Python.

Для пункта 3 изначально создается таблица с помощью оператора PostgresOperator в конструкции:

```
create_table_psql = PostgresOperator(
	task_id='create_table',
    postgres_conn_id='psql_connection',
    sql=""" DROP TABLE IF EXISTS table_name;
            CREATE TABLE table_name ( id int,
                                      md5_id text);
        """
)
```

Думаю тут и так все понятно и комментарии излишни. А если ты не знаешь, что делает SQL код, то какого лешего ты тут забыл, иди учи SQL!

Для загрузки данных используется следующий код:

```
  df = pd.read_csv("/tmp/processed_data/data.csv")
  engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
  df.to_sql('table_name', engine, if_exists='append', schema='public', index=False)
```

Но так как у нас есть дополнительное условие, а именно в XCom переменную положить колличество строк в файл, то опять обращается к **ti**.

```
ti = kwargs['ti']
ti.xcom_push(key='count_string', value=len(df))
```

Объединяем и получается следующая функция **_data_in_postgres**:

```
def _data_in_postgres(**kwargs):
  ti = kwargs['ti']
  df = pd.read_csv("/tmp/processed_data/data.csv")
  engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
  df.to_sql('table_name', engine, if_exists='append', schema='public', index=False)
  ti.xcom_push(key='count_string', value=len(df))
```

Теперь, необходимо вывести просто колличество строк, делаем все тоже самое как и при переносе файла. За это отвечается конструкция:

```
print_count_string_in_df = BashOperator(
  task_id="print_count_string_in_df",
  bash_command=('''echo "В таблице {{ ti.xcom_pull(task_ids='data_in_postgres', key='count_string') }} строк" '''),
  dag=dag,
)
```

Ну и наконец нужно построить граф, для этого последовательно выстраиваем операторы:

```
(
    generate_file >> 
    move_data_file >> 
    create_table_psql >> 
    data_in_postgres >> 
    print_count_string_in_df
)
```

Чтобы посмотреть результат последнего пункта, во вкладке **graph** выделить посленый зелёную таску (print_count_string_in_df) и появятся дополнительные вкладки. Перейдите во вкладку **Logs** и вы увидите запись представленную на скиншоте.

<p align="center">
    <img src="./../png/af_final_task.png" alt="AirFlow" />
</p>
