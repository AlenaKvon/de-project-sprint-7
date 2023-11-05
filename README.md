Шаг 1. Обновить структуру Data Lake.

Опишите в файле README.md структуру хранилища, которую вы планируете использовать для этого проекта: 
в какой директории данные будут тестироваться, 
какая директория будет предназначена для аналитиков, 
как часто обновляются данные, 
какой формат будет использоваться. 
Отталкивайтесь от уже построенного хранилища.

- /user/master/data/geo/events. Папка с данными с гео, уже партицирована по date. будем использовать ее в качестве stage.
Локальная папка с данными: /user/alenakvon/data/geo/events. Партицирована по date, event_type
- /user/alenakvon/analytics - папка для аналитиков и витрин
- /user/alenakvon/analytics/test - папка для тестов
- формат данных .parquet, партиции по дням

Справочники: /user/master/data/snapshots
drwxr-xr-x   - hdfs hadoop          0 2023-08-15 20:17 /user/master/data/snapshots/channel_admins
drwxr-xr-x   - hdfs hadoop          0 2023-08-15 20:17 /user/master/data/snapshots/channels
drwxr-xr-x   - hdfs hadoop          0 2023-08-15 20:17 /user/master/data/snapshots/groups
drwxr-xr-x   - hdfs hadoop          0 2023-08-15 20:17 /user/master/data/snapshots/tags_verified
drwxr-xr-x   - hdfs hadoop          0 2023-08-15 20:17 /user/master/data/snapshots/users

- /user/alenakvon/data/spr/geo - справочник городов


Шаг 2. Создать витрину в разрезе пользователей

- Я добавила координаты в витрину с данными пользователей (geo_city_info), т.к. они нужны далее.
Если критично, тогда нужно будет делать копию.
- условие на км исправил на 1 км

Шаг 3-4.

Шаг 5
даги оформила.





