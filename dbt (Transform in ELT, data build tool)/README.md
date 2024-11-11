### Что такое

### Кому нужно, какие проблемы решает

### Нужно знать перед началом

### Запусти и попробуй!

### А теперь разберёмся, что это было
> дисклеймер: ограничения, только про core версию

### Терминология
- проект
- модель: .sql и .yml файлы
- материализация: view, table, inc table, а также: мат. вью, эфемерные модели
- seed
- источники
- зависимости 
- lineage
- макросы
- тесты 
- пакеты
- также полезно упомянуть: 
    - снепшоты 
    - метрики
    - analysis
- vars
- profile

### Основные команды
* compile
* deps
* run
* test
* build

### А как это может запускаться в продакшене
* airflow (cosmos)
* тесты https://docs.getdbt.com/blog/announcing-unit-testing#unit-tests-vs-model-contracts-vs-data-tests

### Как настроить окружение для дальнейшей удобной работы
* VS Code + dbt power user + linter (найти ссылку)

### Что ещё полезного почитать по dbt
* Поэтапное развитие проекта dbt на практическом примере (прям с кодом) https://docs.getdbt.com/blog/how-to-build-a-mature-dbt-project-from-scratch
* Советы на русском с дополнениями из моего опыта: https://t.me/rzv_de/117
* Про миграцию с хранимых процедур на dbt: https://docs.getdbt.com/blog/migrating-from-stored-procs
* Построение Кимбалл модели: https://docs.getdbt.com/blog/kimball-dimensional-model
* Отладка макросов: https://docs.getdbt.com/blog/guide-to-jinja-debug
* Семантический слой для BI: https://docs.getdbt.com/blog/semantic-layer-in-pieces