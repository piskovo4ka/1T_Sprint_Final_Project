# 1T_Sprint_Final_Project

# В данном репозитории находятся файлы, относящиеся к Итоговому проекту курса "Инженер Данных" от 1T_Sprint.

# Проект "Анализ публикуемых новостей"

**Общая задача: создать ETL-процесс формирования витрин данных для анализа публикаций новостей.**

Подробное описание задачи:

Разработать скрипты загрузки данных в 2-х режимах:
- Инициализирующий – загрузка полного слепка данных источника
- Инкрементальный – загрузка дельты данных за прошедшие сутки

Организовать правильную структуру хранения данных:
-  Сырой слой данных
-  Промежуточный слой
- Слой витрин

В качестве результата работы программного продукта необходимо написать скрипт, который формирует витрину данных следующего содержания:

- Суррогатный ключ категории
- Название категории
- Общее количество новостей из всех источников по данной категории за все время
- Количество новостей данной категории для каждого из источников за все время
- Общее количество новостей из всех источников по данной категории за последние сутки
- Количество новостей данной категории для каждого из источников за последние сутки
- Среднее количество публикаций по данной категории в сутки
- День, в который было сделано максимальное количество публикаций по данной категории
- Количество публикаций новостей данной категории по дням недели

Дополнение:
Т.к. в разных источниках названия и разнообразие категорий могут отличаться, необходимо привести все к единому виду.

Источники:

https://lenta.ru/rss/

https://www.vedomosti.ru/rss/news

https://tass.ru/rss/v2.xml

https://www.fontanka.ru/fontanka.rss
