from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta
import requests
import pandas as pd
import zipfile
import os

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



DATA_DIR = "/opt/airflow/data/movielens"
URL = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"
ZIP_FILE = f"{DATA_DIR}/ml-100k.zip"
EXTRACTED_FOLDER = f"{DATA_DIR}/ml-100k"


# Task 1: Download the dataset
def download_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    response = requests.get(URL)
    with open(ZIP_FILE, "wb") as file:
        file.write(response.content)


# Task 2: Extract dataset
def extract_data():
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(DATA_DIR)


# Task 3.1: Compute mean age by occupation
def compute_mean_age():
    users = pd.read_csv(f"{EXTRACTED_FOLDER}/u.user", sep='|', header=None,
                        names=["user_id", "age", "gender", "occupation", "zip"])
    mean_age = users.groupby("occupation")["age"].mean()
    print(mean_age)


# Task 3.2: Find top 20 highest-rated movies (min 35 ratings)
def top_rated_movies():
    ratings = pd.read_csv(f"{EXTRACTED_FOLDER}/u.data", sep='\t', header=None,
                          names=["user_id", "item_id", "rating", "timestamp"])
    movies = pd.read_csv(f"{EXTRACTED_FOLDER}/u.item", sep='|', header=None, encoding='latin-1',
                         names=["movie_id", "title"] + [f"genre_{i}" for i in range(19)])
    rating_counts = ratings.groupby("item_id")["rating"].agg(['count', 'mean'])
    top_movies = rating_counts[rating_counts['count'] >= 1].nlargest(20, 'mean').merge(movies, left_index=True,
                                                                                        right_on='movie_id')
    print(top_movies[["title", "mean"]])


# Task 3.3: Find top genres rated by age-group and occupation
def top_genres_by_group():
    users = pd.read_csv(f"{EXTRACTED_FOLDER}/u.user", sep='|', header=None,
                        names=["user_id", "age", "gender", "occupation", "zip"])
    ratings = pd.read_csv(f"{EXTRACTED_FOLDER}/u.data", sep='\t', header=None,
                          names=["user_id", "item_id", "rating", "timestamp"])
    movies = pd.read_csv(f"{EXTRACTED_FOLDER}/u.item", sep='|', header=None, encoding='latin-1',
                         names=["movie_id", "title"] + [f"genre_{i}" for i in range(19)])

    merged = ratings.merge(users, on='user_id').merge(movies, left_on='item_id', right_on='movie_id')
    merged['age_group'] = pd.cut(merged['age'], bins=[20, 25, 35, 45, 100], labels=["20-25", "25-35", "35-45", "45+"])
    genre_columns = [f"genre_{i}" for i in range(19)]
    genre_counts = merged.groupby(['age_group', 'occupation'])[genre_columns].sum()
    print(genre_counts)


# Task 3.4: Find top 10 similar movies based on ratings
def find_similar_movies(movie_title="Star Wars (1977)"):
    ratings = pd.read_csv(f"{EXTRACTED_FOLDER}/u.data", sep='\t', header=None,
                          names=["user_id", "item_id", "rating", "timestamp"])
    movies = pd.read_csv(f"{EXTRACTED_FOLDER}/u.item", sep='|', header=None, encoding='latin-1',
                         names=["movie_id", "title"] + [f"genre_{i}" for i in range(19)])

    movie_ratings = ratings.pivot(index='user_id', columns='item_id', values='rating')
    target_movie_id = movies[movies["title"] == movie_title]["movie_id"].values[0]
    similarity = movie_ratings.corrwith(movie_ratings[target_movie_id])
    similarity = similarity.dropna().sort_values(ascending=False).head(11)
    similar_movies = movies[movies["movie_id"].isin(similarity.index)]
    print(similar_movies[["title"]])

# Define the DAG
with DAG(
        dag_id='movielens_pipeline',
        default_args=default_args,
        schedule_interval='00 09 * * *',
        default_view='tree'
) as dag:
    wait_for_dag1 = ExternalTaskSensor(
        task_id="wait_for_dag1",
        external_dag_id="company_sentiment_analysis",
        external_task_id="store_results",  # Must match task_id in DAG-1
        mode="poke",
        timeout=600,
        poke_interval=30
    )
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data
    )
    extract_task = PythonOperator(
        provide_context=True,
        op_kwargs={'key': 'val'},
        task_id='extract_data',
        python_callable=extract_data
    )

    mean_age_task = PythonOperator(
        task_id='compute_mean_age',
        python_callable=compute_mean_age
    )

    top_rated_task = PythonOperator(
        task_id='top_rated_movies',
        python_callable=top_rated_movies
    )

    top_genres_task = PythonOperator(
        task_id='top_genres_by_group',
        python_callable=top_genres_by_group
    )

    similar_movies_task = PythonOperator(
        task_id='find_similar_movies',
        python_callable=find_similar_movies
    )

    wait_for_dag1 >> download_task >> extract_task >> [mean_age_task, top_rated_task, top_genres_task, similar_movies_task]

