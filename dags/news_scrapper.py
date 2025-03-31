from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import random


# Define default arguments for Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email": [
            "manishitnp@gmail.com"
        ],
    'start_date': datetime(2025, 3, 30),
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "sla": timedelta(minutes=3),
}


def fetch_news():
    sources = {
        "YourStory": "https://yourstory.com/search?q=",
        "Finshots": "https://finshots.in/search?q="
    }
    companies = ["HDFC", "Tata Motors"]
    articles = []

    for company in companies:
        for source, url in sources.items():
            search_url = url + company.replace(" ", "%20")
            response = requests.get(search_url)
            if response.status_code == 200:
                articles.append({"source": source, "company": company, "content": response.text})
            else:
                print(f"Failed to fetch news from {source} for {company}")

    return articles


def process_news(ti):
    raw_articles = ti.xcom_pull(task_ids='fetch_news')
    if not raw_articles:
        print("No articles fetched!")
        return

    # Basic text processing
    processed_articles = []
    for article in raw_articles:
        if not article.get("content"):
            continue  # Skip if content is null
        processed_articles.append({
            "source": article["source"],
            "company": article["company"],
            "content": article["content"].lower()[:500]  # Truncate for demo
        })

    return processed_articles

def get_sentiment_score(ti):
    processed_articles = ti.xcom_pull(task_ids='process_news')
    sentiment_scores = []

    for article in processed_articles:
        #api_key = ""
        #url = "https://api.meaningcloud.com/sentiment-2.1"
        #payload = {'key': api_key, 'txt': article["content"], 'lang': 'en'}
        #response = requests.post(url, data=payload)

        #if response.status_code == 200:
            #score = response.json().get("score_tag", 0)
        num = random.random()
        sentiment_scores.append({"company": article["company"], "score": num})

    return sentiment_scores


def store_results(ti):
    sentiment_data = ti.xcom_pull(task_ids='get_sentiment_score')
    df = pd.DataFrame(sentiment_data)
    df.to_csv('sentiment_scores.csv', index=False)


with DAG(
        dag_id='company_sentiment_analysis',
        default_args=default_args,
        schedule_interval='0 19 * * 1-5'  # 7 PM on working days
) as dag:
    fetch_news_task = PythonOperator(
        task_id='fetch_news',
        python_callable=fetch_news,
    )

    process_news_task = PythonOperator(
        task_id='process_news',
        python_callable=process_news,
    )

    get_sentiment_score_task = PythonOperator(
        task_id='get_sentiment_score',
        python_callable=get_sentiment_score,
    )

    store_results_task = PythonOperator(
        task_id='store_results',
        python_callable=store_results,
    )

    fetch_news_task >> process_news_task >> get_sentiment_score_task >> store_results_task
