from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, engine
import pandas as pd
import json
import os
#Etapas

path = '/opt/airflow/data/'
#criar função tratamento de df
def tweet_to_df(tweet):
    try:
        columns_to_drop = ['quote_count', 'reply_count', 'retweet_count', 'favorite_count',
                          'favorited', 'retweeted', 'user', 'entities', 'retweeted_status']
        df_tratado = pd.DataFrame(tweet).reset_index(drop=True).iloc[:1]
        df_tratado.drop(columns=columns_to_drop)
        df_tratado['user_id'] = tweet['user']['id']
        df_tratado['user_id_str'] = tweet['user']['id_str']
        df_tratado['user_screen_name'] = tweet['user']['screen_name']
        df_tratado['user_location'] = tweet['user']['location']
        df_tratado['user_description'] = tweet['user']['description']
        df_tratado['user_protected'] = tweet['user']['protected']
        df_tratado['user_verified'] = tweet['user']['verified']
        df_tratado['user_followers_count'] = tweet['user']['followers_count']
        df_tratado['user_friends_count'] = tweet['user']['friends_count']
        df_tratado['user_created_at'] = tweet['user']['created_at']

        user_mentions = []

        columns_rename = {"screen_name": "entities_screen_name",
                         "name": "entities_name",
                         "id": "entitites_id",
                         "id_str": "entities_id_str"}
        for i in range(len(tweet['entities']['user_mentions'])):
            dicionariobase = tweet['entities']['user_mentions'][i].copy()
            dicionariobase.pop('indices', None)
            df = pd.DataFrame(dicionariobase, index=[0])
            df = df.rename(columns=columns_rename)
            user_mentions.append(df)
        dfs = []
        for i in user_mentions:
            dfs.append(pd.concat([df_tratado.copy(), i], axis=1))

        df_final = pd.concat(dfs, ignore_index=True)
    except:
        return None
    return df_final

#criar DAG
DEFAULT_ARGS = {"owner": "Cristian",
    'depends_on_past': False,
    'start_date': datetime(2021,7,21)
}

@dag(default_args=DEFAULT_ARGS, schedule_interval=None, description="ETL de dados do Twitter para o Postgres"    , tags=['twitter','postgres'])
def etl_twitter_postgres():
#criar task inicio
    @task
    def start():
        print("Start!")
        return True

#criar task ler txt - json
    @task
    def read_data_export_json(retorno):
        with open(path + 'collected_tweets20210719204547.txt', 'r') as f:
            tweets = f.readlines()
        total_tweets = len(tweets)
        for i in range(total_tweets):
            with open(f"{path}tweet_{i}.json", 'w') as f:
                json.dump(
                    json.loads(json.loads(tweets[i]))
                    , f)
        return len(tweets) - 1

    @task
    def read_json_export_pandas(retorno):
        arquivos = [file for file in os.listdir(path) if file.startswith("tweet_")]
        print(arquivos)
        for arquivo in arquivos:
            with open(path + arquivo) as f:
                tweet = f.readlines()
            parsedtweet = json.loads(tweet[0])
            processado = tweet_to_df(parsedtweet)
            if processado  is None:
                pass
            else:
                processado.to_csv(path + arquivo [:-4] + "csv", sep=";", index=False)
        return True
    @task
    def concatenate_all_csv(retorno):
        arquivos = [file for file in os.listdir(path) if file.endswith(".csv")]
        dataframes = [pd.read_csv(path + arquivo, sep=';',) for arquivo in arquivos]
        unico = pd.concat(dataframes, ignore_index=True)
        unico.to_csv(f"{path}tweets_data_frame_unico.csv", sep=";", index=False)
        unico.to_csv("/tmp/tweets_data_frame_unico.csv", sep=";", index=False)
        return True

    @task
    def write_table_to_postgres(retorno):
        df = pd.read_csv(f"{path}tweets_data_frame_unico.csv", sep=";")
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/postgres")
        df.to_sql("tweets", con=engine, index=False, if_exists="replace", chunksize=1000)
        return True

    st=start()
    ntweets = read_data_export_json(st)
    check_file = FileSensor(task_id="check_file", filepath=f"{path}tweet_{ntweets}.json", poke_interval=10)
    res = read_json_export_pandas(ntweets)

    ntweets >> check_file >> res

    list_files = BashOperator(
        task_id="list_files",
        bash_command = f"ls {path}"
    )
    res >> list_files

    concatenado = concatenate_all_csv(res)
    escrita = write_table_to_postgres(concatenado)

    query_table = PostgresOperator(
        task_id = "consulta_tabela",
        postgres_conn_id = "postgres",
        sql = """
            CREATE OR REPLACE VIEW tweets_lang AS
            SELECT lang, count(lang) as contagem FROM tweets
            GROUP BY lang
            ORDER BY contagem desc
        """,
    )
    escrita >> query_table

#criar task ler json - pandas (csv)
#concatenate csv
#escrever para postgres
#executar dag
#
execucao = etl_twitter_postgres()