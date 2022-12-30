#!/usr/bin/env python
# coding: utf-8

# In[11]:


import requests as req 
from bs4 import BeautifulSoup   
import time
import json

import numpy as np
import pandas as pd

# создадим пустой словарь, куда будем записывать данные

data = {

        "data" : []
    }

data_dirty = {

        "data" : []
    }


sites = { 'Фонтонка.ру' : ['https://www.fontanka.ru/fontanka.rss', ''],
                  'LENTA.RU' : ['https://lenta.ru/rss', ''],
                  'TASS' : ['https://tass.ru/rss/v2.xml', ''],
                  'ВЕДОМОСТИ' : ['https://www.vedomosti.ru/rss/news', '']
           }


df = pd.DataFrame(data['data'])
df_dirty = pd.DataFrame(data_dirty['data'])


def parsing(df, df_dirty):        
        
    for site in range(len(sites)):
        
        exitFlag=False
        
        data = {"data" : [] 
               }
        data_dirty = {"data" : []  }
       
        url = list(sites.values())[site][0]
                
        resp = req.get(url)
        soup  = BeautifulSoup(resp.content, features="lxml")

        tag_item = soup.find_all('item')
        
        for tag in tag_item:
            
            title = tag.find('title')
            
            if title.text != list(sites.values())[site][1]: 
                exitFlag=True 
                
                category = tag.find('category')

                date = tag.find('pubdate')

                data['data'].append({"title" : title.text, "category": category.text,
                                     "pubdate" : date.text, "site" : list(sites.keys())[site]
                                    })
                data_dirty['data'].append({"site" : list(sites.keys())[site], "item" : tag.text})


                with open(f"/opt/airflow/dags/scripts/data/data_dirty_{pd.Timestamp('today'):%d-%m-%Y-%H-%M}.json", "w", encoding="utf-8") as file:
                      json.dump(data_dirty, file, ensure_ascii = False)                        
            else:
                break                                
                
        if(exitFlag):
            df = pd.concat([pd.DataFrame(data['data']), df], ignore_index=True)
            df_dirty = pd.concat([pd.DataFrame(data_dirty['data']), df_dirty], ignore_index=True)        
            sites[list(sites.keys())[site]][1] = df.iloc[0]['title']   
          
    return df, df_dirty


df, df_dirty  = parsing(df, df_dirty)
df.sample(8)

df_init = df.copy()


with open("/opt/airflow/dags/scripts/df_init.json", "w", encoding="utf-8") as file:
                  json.dump(df_init.to_dict(), file, ensure_ascii = False)


with open("/opt/airflow/dags/scripts/data/sites.json", "w", encoding="utf-8") as file:
                  json.dump(sites, file, ensure_ascii = False) 

# ## Обаботка и подготовка данных

# ### Работа с различными категориями

# Выделим основные категории, переименуем их. В случае появления новых категорий, будет отрпавлять в категорию "Всякое другое".


df['category'] = np.where(((df['category']=='Экономика') | (df['category']=='Бизнес') 
                           | (df['category']=='Финансы') | (df['category']=='Бизнес / Транспорт')),
                          'Экономика и бизнес', df['category']) 

df['category'] = np.where(((df['category']=='Моя страна') | (df['category']=='Москва')
                           | (df['category']=='Город') | (df['category']=='Доктор Питер')
                           | (df['category']=='Среда обитания')),'Россия', df['category'])

df['category'] = np.where(((df['category']=='Международная панорама') | (df['category']=='Бывший СССР')),
                          'Мир', df['category'])

df['category'] = np.where(((df['category']=='Силовые структуры') | (df['category']=='Космос')),
                          'Армия и ОПК', df['category']) 

df['category'] = np.where((df['category']=='Технологии'),'Наука и техника', df['category'])

df['category'] = np.where(((df['category']=='Общество') | (df['category']=='Новости партнеров')
                          | (df['category']=='Власть')),'Политика', df['category']) 

df['category'] = np.where(((df['category']=='Из жизни') 
                           | (df['category']=='Биографии и справки') | (df['category']=='Особое мнение')),
                          'Происшествия', df['category'])

df['category'] = np.where((df['category']=='Туризм и отдых') | (df['category']=='Туризм'),
                          'Путешествия', df['category']) 

df['category'] = np.where((df['category']=='Афиша Plus'),'Культура', df['category'])

df['category'] = np.where((df['category']=='Забота о себе'),'Здоровье', df['category'])

df['category'] = np.where((df['category']=='Ценности'),'Интернет и СМИ', df['category'])

df['category'] = np.where(((df['category']=='Авто') 
                           | (df['category']=='Недвижимость')),'Недвижимость и Авто', df['category'])

new_category = ['Политика',
 'Экономика и бизнес',
 'Мир',
 'Россия',
 'Происшествия',
 'Наука и техника',
 'Интернет и СМИ',
 'Армия и ОПК',
 'Спорт',
 'Путешествия',
 'Культура',
 'Здоровье'  ]


for cat in list(df['category'].value_counts().index):
     if cat not in new_category:
        df['category'] = np.where((df['category'] == cat),'Всякое другое', df['category'])


### Создадим DataFrame с нумерованным списком категорий

df_category = pd.DataFrame(df['category'].unique())
df_category = df_category.rename_axis('id').reset_index()
df_category = df_category.rename(columns={0:'category'})
neworder = ['id','category'] 
df_category = df_category.reindex(columns=neworder)

### Создадим DataFrame с нумерованным списком сайтов-источников

df_sites  = pd.DataFrame(df['site'].unique())
df_sites  = df_sites.rename_axis('id').reset_index()
df_sites  = df_sites.rename(columns={0:'site'})
neworder = ['id','site']  
df_sites  = df_sites.reindex(columns=neworder)

### Создадим DataFrame с нумерованным списком новостей: id, номер категории, номер сайта и дата

df_news = df.copy()
df_news['date_of_week'] = 1
df_news['date_and_time'] = 1

#Разделим даты на день недели и все остальное.

for index, row in enumerate(df_news['pubdate'].str.split(',')):
    #print(row)
    df_news['date_of_week'][index] = row[0]
    df_news['date_and_time'][index] = " ".join(row[1].split()[:-1])

#Пронумеруем наши новости

df_news = df_news.drop(columns = ['pubdate'])

#Кодируем категории новостей и названия сайтов.

df_news['category_num'] = 0
for ind, row in enumerate(df_news['category']):
    for i, cat in enumerate(df_category['category']):
        if row == cat:
            df_news['category_num'][ind] = i

df_news['site_num'] = 0
for ind, row in enumerate(df_news['site']):
    for i, cat in enumerate(df_sites['site']):
        if row == cat:
            df_news['site_num'][ind] = i

#Все супер! Удалим избыточные столбцы 'site', 'category'.

df_news = df_news.drop(columns = ['site', 'category'])
neworder = ['title', 'category_num', 'site_num', 'date_and_time', 'date_of_week']  
df_news = df_news.reindex(columns=neworder)

#Кажется, что для наших витрин сами названия новостей не нужны. Тем более у нас есть сохраненная резервная копи данных. 
#Удалим названия новостей. 

df_news = df_news.drop(columns = ['title'])


## Заполнение таблиц БД PostgreSQL

#Подключение к БД PostgreSQL

import psycopg2

conn = psycopg2.connect(
    database="employees",
    user="pdn",
    password="admin",
    host="host.docker.internal",
    port="25432"
)

cur = conn.cursor()

create_posts_table = """
CREATE TABLE IF NOT EXISTS CATEGORIES (
    ID INT PRIMARY KEY,
    category CHARACTER VARYING(100) NOT NULL 
)
"""

conn.autocommit = True
cur.execute(create_posts_table)

create_posts_table = """
CREATE TABLE IF NOT EXISTS SITES (
    ID INT PRIMARY KEY,
    SITE CHARACTER VARYING(80) NOT NULL 
)
"""

conn.autocommit = True
cur.execute(create_posts_table)

create_posts_table = """
CREATE TABLE IF NOT EXISTS ss_NEWS (
    ID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_num INTEGER NOT NULL, 
    site_num INTEGER NOT NULL,
    date_and_time TIMESTAMP NOT NULL, 
    date_of_week CHARACTER VARYING(30) NOT NULL,
    CONSTRAINT fk_category_num
      FOREIGN KEY(category_num)
          REFERENCES CATEGORIES(ID)
          ON DELETE CASCADE,
    CONSTRAINT fk_site_num
      FOREIGN KEY(site_num)
          REFERENCES SITES(ID)
          ON DELETE CASCADE  
    )
"""

conn.autocommit = True
cur.execute(create_posts_table)

df_shema_category = list(df_category.itertuples(index=False, name=None))
df_shema_site = list(df_sites.itertuples(index=False, name=None))
df_shema_news= list(df_news.itertuples(index=False, name=None))

data_category = ", ".join(["%s"] * len(df_category))

insert_query = (f"INSERT INTO CATEGORIES (ID, category) VALUES {data_category}")

conn.autocommit = True
cur = conn.cursor()
cur.execute(insert_query, df_shema_category)

data_site = ", ".join(["%s"] * len(df_sites))

insert_query = (
    f"INSERT INTO SITES (ID, site) VALUES {data_site}"
)

conn.autocommit = True
cur = conn.cursor()
cur.execute(insert_query, df_shema_site)

data_news = ", ".join(["%s"] * len(df_news))

insert_query = (
    f"INSERT INTO ss_NEWS (category_num, site_num, date_and_time, date_of_week) VALUES {data_news}"
)

conn.autocommit = True
cur = conn.cursor()
cur.execute(insert_query, df_shema_news)

#  Посмотрим все на данные в таблице NEWS
cur.execute("SELECT * FROM ss_NEWS LIMIT 10 ")
dirs = cur.fetchall()
for row in dirs[-5:]:
    
    print(row)                                 




