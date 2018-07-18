from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import json
import  pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import  relativedelta
from bs4 import BeautifulSoup
import urllib.request as rq
import urllib
import time
import re

args = {
    'owner': 'airflow',
    'provide_context': True
}

def parse_date(fecha,date_map):

    day_only = fecha[:-6]
    only_months = re.sub("\d*","",day_only)
    only_months_replaced = date_map[only_months]
    scrape_year = date_map["Hoy"][:4]
    if scrape_year in only_months_replaced:
        return pd.to_datetime(only_months_replaced)
    else:
        fecha_num = day_only[:2]
        nueva_fecha = pd.to_datetime("{}{}-{}".format(scrape_year,only_months_replaced,fecha_num))
        return  nueva_fecha

def scrape_basic_info(date_limit,date_map):
    prices = []
    urls = []
    nombres = []
    categorias = []
    municipios = []
    fechas = []
    base_url = 'http://www.corotos.com.do/santo_domingo/bienes_ra%C3%ADces-en_venta?f=c'
    new_url = base_url
    limit_reached = False
    i = 0
    today_dt = pd.to_datetime(date_map["Hoy"])
    while not limit_reached:
        print("Scraping page {}".format(i))
        req = rq.Request(new_url)
        req.add_header('User-Agent', 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0')
        a = rq.urlopen(req).read()
        parser = BeautifulSoup(a, 'lxml')
        for item in parser.find_all("div", attrs={'class': 'item relative '}):
            # print(item)
            fecha =  item.find("div", attrs={"class": "time"}).get_text().strip()
            fecha_iso = parse_date(fecha,date_map)
            if fecha_iso > today_dt:
                fecha_iso = fecha_iso - relativedelta(years=1)

            if fecha_iso < date_limit:
                print("Date limit reached.")
                limit_reached = True
                break
            else:
                fechas.append(fecha_iso.isoformat())
                link = item.find("a", attrs={'class': "history", "href": True})
                urls.append(link["href"])
                nombres.append(link.get_text())
                try:
                    prices.append(item.find("span", attrs={'class': "price"}).get_text().strip())
                except:
                    prices.append("")
                otra_info = (item.find("div", attrs={"class": "item-cat-region"}).get_text().strip().split(','))
                categorias.append(otra_info[0])
                municipios.append(otra_info[1])

        new_url = base_url + "&o={}".format(i + 2)
        i+=1
    bienes_raices_df = pd.DataFrame(
        {"Nombre": nombres, "URLs": urls, "Categoria": categorias, "Municipio": municipios, "Fecha": fechas,
         "Precio": prices})
    bienes_raices_df = bienes_raices_df.loc[bienes_raices_df.Precio != ""]
    return bienes_raices_df

def print_scrape(**kwargs):
    months_dict = {' dic.': '-12', ' nov.': '-11', ' oct.': '-10', ' sept.': '-09', ' agosto': '-08', ' jul.': '-07',
                   ' jun.': '-06',  ' mayo': '-05', ' abr.': '-04', ' marzo': '-03', ' feb.': '-02', ' enero': '-01'}
    scraping_date = datetime.date.today()
    months_dict['Hoy'] = scraping_date.isoformat()
    months_dict['Ayer'] = (scraping_date - relativedelta(days=1)).isoformat()
    bas = scrape_basic_info(pd.to_datetime("2018-05-02"), months_dict)
    return bas


def scrape_detailed_info(bienes_raices_df,characteristics):

    detailed_df = bienes_raices_df.copy()


    characteristics_data = {}
    for key in characteristics:
        characteristics_data[key] = ["Ninguno"] * detailed_df.shape[0]

    print("Scraping {} URLs".format(detailed_df.shape[0]))
    for i, url in enumerate(detailed_df.URLs):
        spl = url.split("://")
        full_url = spl[0] + "://" + urllib.parse.quote(spl[1])
        req = rq.Request(full_url)
        req.add_header('User-Agent', 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0')
        skipped_details = set(['Tipo de anuncio','Tipo de Uso','Número de Oficinas'])
        try:
            a = rq.urlopen(req).read()
            parser = BeautifulSoup(a, 'lxml')
            characteristics_data["Vendedor"][i] = parser.find("h3", attrs={"class": "shops-name"}).get_text()
            characteristics_data["Descripción"][i] = parser.find("div", attrs={"class": "fs16"}).get_text()
            try:
                table = parser.find("div", attrs={"class": "param-table"})
                for column in table.find_all("div", attrs={"class": "param-table-column"}):
                    detail = column.find("span", attrs={"class": "adparam_label float-left prm"}).get_text()
                    if detail not in skipped_details:
                        characteristics_data[detail][i] = column.find("strong").get_text()
            except:
                print(detail)
                print("Could not find params for {}".format(i))

        except:
            print("Failed to open {}".format(i))

    for key in characteristics_data.keys():
        try:
            detailed_df[key] = characteristics_data[key]
        except:
            continue
    return detailed_df

def get_detailed_info(**kwargs):
    characteristics = ["Construcción", "Número de Baños", "Número de Habitaciones", "Sector",
                       "Solar", "Tipo",

                       "Vendedor", "Descripción"]

    bienes_raices_df = kwargs['ti'].xcom_pull(task_ids='hello_task')
    bienes_raices_df_detailed = scrape_detailed_info(bienes_raices_df,characteristics)
    return bienes_raices_df_detailed


def puller(**kwargs):
    ti = kwargs['ti']

    v2 = ti.xcom_pull(task_ids='extract_task')
    v3 = ti.xcom_pull(task_ids='extract_details_task')
    return v2


dag = DAG('hello_world', description='Simple scraping DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime.datetime(2017, 3, 20), catchup=False,
          default_args=args)

push1 = PythonOperator(task_id='extract_task', python_callable=print_scrape, dag=dag)
push2 = PythonOperator(task_id='extract_details_task', python_callable=get_detailed_info, dag=dag)
pull = PythonOperator(
    task_id='puller', dag=dag, python_callable=puller)

pull.set_upstream([push1,push2])