Здесь лежат два DAG файла для инициализирующего режима (загрузка полного слепка данных источника)
 загрузки (парсинга) данных и инкрементального (загрузка дельты данных за прошедшие сутки) режима загрузки.

final_project.py - для инициализирующего режима (тянет за собой script_1_parsing.py из папки scripts)
final_project_dag.py - для инкрементального режима (тянет за собой parsing_cleaning_and_creating_tables 
из папки scripts)

<img width="921" alt="final_progect_image" src="https://user-images.githubusercontent.com/98237700/210047949-a10a72ab-9faa-47ba-9773-a9176ae62170.png">


В final_project_image и final_project_dag_image можно помотреть скрины "паровозов".

<img width="946" alt="final_progect_dag_image" src="https://user-images.githubusercontent.com/98237700/210047926-7c635b25-e65d-44ce-91a1-7a5e8c9b4f0b.png">


В папке scripts  лежат питонячие скрипты, sql запросы, а также сохраняемые файлы. Тебе сюда.
