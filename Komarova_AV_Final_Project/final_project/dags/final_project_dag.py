from datetime import datetime, timedelta
import psycopg2

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.sql_sensor import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts import parsing_cleaning_and_creating_tables

default_dag_args = {}

# Переменная с именем txt файла
file_path = Variable.get("final_file_path")
    
# ф-я для передачи креденшиалс    
def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    conn_to_airflow = BaseHook.get_connection(conn_id)
    return conn_to_airflow

def hello():
    print("Hello!")

def buy():
    print("buy!")  

def connect_to_psql(**kwargs):
    ti = kwargs['ti']

    file_path = Variable.get("final_file_path")
    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port,\
                                                             conn_to_airflow.login, conn_to_airflow.password,\
                                                                 conn_to_airflow.schema
    
    ti.xcom_push(value = [pg_hostname, pg_port, pg_username, pg_pass, pg_db], key='conn_to_airflow')
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS n_t (id serial PRIMARY KEY, x_1 integer, x_2 integer);")
    #cursor.execute("SELECT * FROM ss_news;")
         
    #cursor.fetchall()
    pg_conn.commit()

    cursor.close()
    pg_conn.close()      


with DAG(dag_id=
    "final_project_dag", start_date=datetime(2022, 12, 27),
    schedule_interval="23-25 6 30 12 *",
    default_args=default_dag_args,
) as dag:
    
    create_vitrines_1 = PostgresOperator(
        task_id="new_vitrine_1",
        sql="""  with nnum_4 as (
                with num_4 as (select category_num,
                                    round(avg(id)) as avg_per_last_day
                        from ss_news
                        where (cast(CURRENT_DATE as DATE)- cast(date_and_time as DATE)) < 1
                        group by category_num 
                        order by category_num ) 
                select id, category, avg_per_last_day
                from categories ac 
                join num_4 on num_4.category_num = ac.id
                ),
                nnum_1 as(
                with num_1 as (select category_num,
                        sum(id) as sum_all
                from ss_news
                group by category_num 
                order by category_num ) 
                select id, category, sum_all
                from categories ac 
                join num_1 on num_1.category_num = ac.id
                ),
                nnum_2 as (
                with num_2 as (select category_num,
                                    sum(id) as sum_last_day
                            from ss_news
                            where (cast(CURRENT_DATE as DATE)- cast(date_and_time as DATE)) < 2 
                            group by category_num) 
                select ac.id, 
                    category,
                    sum_last_day
                from categories ac 
                join num_2 on num_2.category_num = ac.id 
                ),
                nnum_3 as (
                with num_3 as (select category_num,
                                    round(avg(id)) as avg_all
                        from ss_news
                        group by category_num 
                        order by category_num ) 
                select id, category, avg_all
                from categories ac 
                join num_3 on num_3.category_num = ac.id
                )
                select nnum_2.id,
                    nnum_2.category,
                    nnum_2.sum_last_day,
                    nnum_1.sum_all,
                    nnum_3.avg_all,
                    nnum_4.avg_per_last_day
                from nnum_2 join nnum_1
                on nnum_1.id = nnum_2.id
                join nnum_3 
                on nnum_1.id = nnum_3.id
                join nnum_4 
                on nnum_1.id = nnum_4.id;
        
        """,
        postgres_conn_id=Variable.get("conn_id"),
        trigger_rule='one_success',
        dag=dag
    )

    create_vitrines_2 = PostgresOperator(
        task_id="new_vitrine_2",
        sql="""  with nn_1 as (
                    with n_1 as (select category_num, site_num,
                                    sum(id) as sum_per_site
                                from ss_news
                                group by category_num, site_num
                                order by category_num, site_num),
                        s as (select id, 
                                    site
                            from sites)
                    select c.id, 
                        c.category, 
                        s.site,
                        sum(n_1.sum_per_site) as sum_per_site_all
                    from categories as c
                    join n_1 on n_1.category_num = c.id
                    join s on n_1.site_num = s.id
                    group by c.id, s.site 
                    order by id 
                    ),
                    nn_2 as 
                    (with n_2 as (select category_num, 
                                    site_num,
                                    sum(id) as sum_per_site_last_day
                                from ss_news
                                where (cast(CURRENT_DATE as DATE)- cast(date_and_time as DATE)) < 2 
                                group by category_num, site_num
                                order by category_num, site_num),
                            s as (select id, 
                                    site
                            from sites)
                    select c.id, 
                        c.category, 
                        s.site,
                        sum(n_2.sum_per_site_last_day) as sum_per_site_last_day
                    from categories as c
                    join n_2 on n_2.category_num = c.id
                    join s on n_2.site_num = s.id
                    group by c.id, s.site 
                    order by id, site
                    )
                    select nn_1.id, 
                        nn_1.category, 
                        nn_1.site,
                        nn_1.sum_per_site_all,
                        nn_2.sum_per_site_last_day
                    from nn_1 join nn_2
                    on nn_1.id = nn_2.id
        
        """,
        postgres_conn_id=Variable.get("conn_id"),
        trigger_rule='one_success',
        dag=dag
    )

    create_vitrines_3 = PostgresOperator(
        task_id="new_vitrine_3",
        sql=""" with s as (
                select category_num,
                    (first_value(cast(date_and_time as DATE)) OVER (PARTITION BY category_num)) as max_day
                    --cast(date_and_time as DATE),
                    --max(sum(id)) OVER (PARTITION BY category_num),
                                    --sum(id) as sum_pub
                            from ss_news
                            group by category_num, cast(date_and_time as DATE)
                            order by category_num )
                select category_num,
                    max(max_day) as day_with_max_news
                from s 
                group by category_num;
        
        """,
        postgres_conn_id=Variable.get("conn_id"),
        trigger_rule='one_success',
        dag=dag
    )
    
    create_vitrines_4 = PostgresOperator(
        task_id="new_vitrine_4",
        sql="""  with num as(
                select category_num,
                    date_of_week,
                    sum(id) as sum_pub
                from ss_news
                group by category_num, date_of_week
                order by category_num, date_of_week 
                )
                select ac.id, 
                    category,
                    date_of_week,
                    sum_pub
                from categories as ac
                join num on num.category_num = ac.id;

                SELECT * FROM ss_news;
        
        """,
        postgres_conn_id=Variable.get("conn_id"),
        trigger_rule='one_success',
        dag=dag
    )
    
    
    bash_hello = BashOperator(task_id="hello", bash_command="echo hello", do_xcom_push=False)
    parsing_1 = BashOperator(task_id="parsing_cleaning_creating", \
            bash_command="/opt/airflow/dags/scripts/parsing_cleaning_and_creating_tables.py", \
            do_xcom_push=False) 
    conn_to_psql_tsk = PythonOperator(task_id="connect_to_psql", python_callable = connect_to_psql)
    bash_buy = BashOperator(task_id="buy", bash_command="echo buy", do_xcom_push=False)


 # Set dependencies between tasks
bash_hello >> parsing_1 >> conn_to_psql_tsk >> create_vitrines_1 >> create_vitrines_2 \
            >> create_vitrines_3 >> create_vitrines_4 >> bash_buy 