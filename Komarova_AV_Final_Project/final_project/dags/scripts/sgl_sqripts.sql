--Общее количество новостей из всех источников по данной категории за все время
--Общее количество новостей из всех источников по данной категории за последние сутки
--Среднее количество публикаций по данной категории за все время
--Среднее количество публикаций по данной категории за последние сутки
with nnum_4 as (
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
on nnum_1.id = nnum_4.id
	   


CREATE TABLE IF NOT EXISTS Vitrine_1 (
    ID INT PRIMARY KEY,
    category CHARACTER VARYING(100) NOT NULL 
)



--Количество новостей данной категории для каждого из источников за последние сутки
--Количество новостей данной категории для каждого из источников за все время
with nn_1 as (
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
	   
	   


--День, в который было сделано максимальное количество публикаций по данной категории
with s as (
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
group by category_num


--Количество публикаций новостей данной категории по дням недели
with num as(
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
join num on num.category_num = ac.id

 

