## Куратор раздела

<img align="left" width="200" src="../../png/shust.jpg" />

**Шустиков Владимир**, оставивший военную жизнь позади и ушедший в данные с головой. Работаю с данными более 2х лет и останавливаться не собираюсь! Веду:

   [Telegram канал](https://t.me/Shust_DE)
   
   [Youtube канал](https://www.youtube.com/@shust_de)

Если хочешь сменить текущую профессию на Дата Инженера — пиши не стесняйся, я сам проходил этот не легкий путь и тебе помогу https://t.me/ShustDE

Хочешь улучшить текущий раздел, внести недостающее или поправить формулировку? Предлагай PR и тегай [@ShustGF](https://github.com/ShustGF).

## Кейс 1: Уходи от OR в JOIN'ах

Есть запрос в котором втречается JOIN с 3мя условиями. Таблицы имеют большое кол-во строк.

```sql
SELECT ...
FROM _tmp_calc_cred AS cc
    INNER JOIN _postback_api  as pa 
        ON cc.request_external_id = pa.EXTERNAL_ID OR
           cc.calculation_id = pa.EXTERNAL_ID  OR
           cc.request_id = pa.EXTERNAL_ID
```

Во первых в данном случе стоит рапледелить таблицу `_tmp_calc_cred` по всем сегментам, так как в JOIN используется 3 разные колонки, а таблицу `_postback_api` по ключу `EXTERNAL_ID`, чтобы запрос выполнялся параллельно на разных сегментах.

Данный запрос стоит преобразовать к следующиму виду:

```sql
select ...
  FROM alexd._tmp_CALCULATION_CREDIT AS cc
      INNER JOIN alexd._postback_api  as pa
            ON cc.request_external_id = pa.EXTERNAL_ID 
union all
  select ...
  FROM alexd._tmp_CALCULATION_CREDIT AS cc
      INNER JOIN alexd._postback_api  as pa
           ON cc.calculation_id = pa.EXTERNAL_ID
union
  select ...
  FROM alexd._tmp_CALCULATION_CREDIT AS cc
      INNER JOIN alexd._postback_api  as pa
           ON  cc.request_id = pa.EXTERNAL_ID
```

В первом случае выберится физический вид JOIN'а - `Nested Loop`, 2й запрос хоть и больше, но выберится `Hash JOIN`. Здесь главное по отдельности запустить запросы и пприкинуть, какое максимальное кол-во строк может получиться в результате. Если строк в результате получается больше, чем в самих таблицах (произошел CROSS JOIN данных), то возможно 1й вариант будет выстрее!



