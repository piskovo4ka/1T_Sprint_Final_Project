I. 
script_1_parsing.py - питоновский код для парсинга, обработки и загрузки первичного слепка данных, 
используемый в DAG final_project.py  

В нем происходит парсинг данных и сохранение "грязной версии", сохранение словаря sites, содержащего тексты 
последних спарсенных новостей для каждого сайта. Это необходимо для последующего инкрементального режима.

Производится обработка данных: уменьшение кол-ва категорий, создание чистеньких датафреймов, 
для последующей загрузки в БД.

Создаются три таблицы: 
1. таблица categories со списком категорий новостей и суррогатными ключами (у нас их 13, 
было принято решение все непопулярные категории помещать в категорию "Всякое другое". 

<img width="357" alt="table_categories" src="https://user-images.githubusercontent.com/98237700/210048053-99f7e144-7ca1-4e4d-a03d-b0a5e65c13b5.png">


2. таблица sites со списком сайтов (у нас их 4) и суррогатными ключами

<img width="197" alt="table_sites" src="https://user-images.githubusercontent.com/98237700/210048075-7a8abd80-6056-4bde-9e54-5884332bc08d.png">


3. таблица ss_new с пронумерованными новостями (первичный ключ), номеро категории (вторичный ключ),
номером сайта (вторичный ключ), датой новости, днем недели, когда была опубликована.

<img width="392" alt="table_ss_news" src="https://user-images.githubusercontent.com/98237700/210048124-ebf0426e-611d-4fc6-b2db-a67ca60af1de.png">


ER-диаграмму, а также сами таблицы можно посмотреть в папке images. 

<img width="273" alt="ER_diagramm_1" src="https://user-images.githubusercontent.com/98237700/210048134-e5c877ff-a69e-4533-8cd6-8c01c90aaf77.png">


<img width="303" alt="ER_diagramm_2" src="https://user-images.githubusercontent.com/98237700/210048139-fa91b2e2-09aa-4d73-909f-da38a8fbb06f.png">


II.
parsing_cleaning_and_creating_tables.py - питоновский код для парсинга, 
обработки и загрузки дельты данных, для инкрементального режима используемый в DAG final_project_dag.py

Данный режим не сильно отличается от предыдущего. 
Основные отличия:
1. Загрузка не всего сайта, а только начиная с последней новости (список sites).
2. Загрузка новых данных только в таблицу SS_NEWS, тк сайты и категории у нас не изменяются.


III.
В sgl_sqripts.sql - можно посмотреть скрипты написания итоговых витрин. Получилось 4 витрины. 

Получившиеся витрины можно посмотреть в папке images. 

![vitrina_2_image](https://user-images.githubusercontent.com/98237700/210048270-7c9c3670-ff3e-4ca7-935a-eebb328bccf7.png)

![vitrina_1_image](https://user-images.githubusercontent.com/98237700/210048281-22878d6a-4271-40d0-ad0d-50ae2bf066f1.png)


<img width="519" alt="vitrina_3_image" src="https://user-images.githubusercontent.com/98237700/210048289-20a26431-8529-4dfd-9e44-f71276a36086.png">

![vitrina_4_image](https://user-images.githubusercontent.com/98237700/210048302-beb41e79-ff21-45b4-8f15-1197d925fabc.png)


IV.
Файлы df_init и data_dirty - то, что сохраняется в первозданном виде. 
По сути это что-то среднее между Mini-Data-Lake и DWH.



