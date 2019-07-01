import pandas as pd
import os
import subprocess
import logging
import time


current_path = os.getcwd()
MOVIE_PATH = current_path + "/imdb/basic.tsv"
CREW_PATH = current_path + "/imdb/cast.tsv"
DIRECTOR_PATH = current_path + "/imdb/crew.tsv"
RATING_PATH = current_path + "/imdb/rating.tsv"
JSON_FILE = "movie.json"
FORMAT = '%(asctime)-15s %(user)-8s %(message)s'
rootLogger = logging.getLogger()
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(FORMAT)
rootLogger.addHandler(consoleHandler)


def create_movie_df(filepath):
    try:
        logging.info("Start creating and cleaning basic movie dataframe\n")
        df = pd.read_csv(filepath, sep='\t', low_memory=False)
        df = df[df["titleType"] == "movie"]
        #df = df[(df["titleType"] == "short") | (df["titleType"] == "movie")]
        df = df.reset_index()
        df.drop('index',axis=1,inplace=True)
        df["genres"] = df["genres"].apply(lambda x: "" if x == "\\N" else x)
        df["genres"] = df["genres"].apply(lambda x: x.split(",") if not x == "" else x)
        df["startYear"] = df["startYear"].apply(lambda x: "" if x == "\\N" else x)
        df["endYear"] = df["endYear"].apply(lambda x: "" if x == "\\N" else x)
        df["runtimeMinutes"] = df["runtimeMinutes"].apply(lambda x: "" if x == "\\N" else str(int(x) * 60))
        df = df.rename(columns={"runtimeMinutes": "runtimeSeconds"})
        df = df.reset_index()
        logging.info("Basic movie dataframe created..\n")
        return df
    except Exception:
        logging.error("There is something going wrong in movie frame")
        raise


def create_director_writer_df(filepath):
    try:
        logging.info("Start creating and cleaning director dataframe\n")
        df = pd.read_csv(filepath, sep='\t', low_memory=False)
        df["directors"] = df["directors"].apply(lambda x: "" if x == "\\N" else x)
        df["writers"] = df["writers"].apply(lambda x: "" if x == "\\N" else x)
        logging.info("Director dataframe created..\n")
        return df
    except Exception:
        logging.error("There is something going wrong in director frame")
        raise


def create_rating_df(filepath):
    try:
        rating_df = pd.read_csv(filepath,sep='\t', low_memory=True)
        return rating_df
    except Exception:
        logging.error("There is something went wrong in rating frame")
        raise


def merge_cast_movie_director_df(movie_df, director_df,rating_df, filepath):
    try:
        logging.info("Start creating and merging all  director, movie,crew dataframes\n")
        cast_df = pd.read_csv(filepath, sep='\t', low_memory=False)
        cast_df.drop('job', axis=1, inplace=True)
        cast_df.drop('characters', axis=1, inplace=True)
        cast_df.drop('ordering', axis=1, inplace=True)
        cast_df = cast_df.groupby('tconst').agg({'nconst':', '.join,'category':', '.join}).reset_index()
        movie_with_cast = pd.merge(movie_df, cast_df, on="tconst",how='left')
        movie_with_cast_director = pd.merge(movie_with_cast, director_df, on="tconst",how='left')
        movie_with_cast_director_rating = pd.merge(movie_with_cast_director, rating_df, on="tconst",how='left')
        movie_with_cast_director_rating = movie_with_cast_director_rating[movie_with_cast_director_rating["titleType"] == "movie"]
        logging.info("Final frame created.\n")
        return movie_with_cast_director_rating
    except Exception:
        logging.error("There is something going wrong in final frame")
        raise


def create_movie_json(final_df):
    logging.info("Creating json file from final df..\n")
    movie_json = final_df.to_json(JSON_FILE, orient='records')
    logging.info("Movie JSON file created: {}...\n".format(JSON_FILE))


def main():
    t1 = time.time()
    logging.info("Movie data creation starts...")
    movie_df = create_movie_df(MOVIE_PATH)
    director_df = create_director_writer_df(DIRECTOR_PATH)
    rating_df = create_rating_df(RATING_PATH)
    final_df = merge_cast_movie_director_df(movie_df, director_df, rating_df, CREW_PATH)
    create_movie_json(final_df)
    cmd = "mongoimport --jsonArray --db movie --collection movies --file {}".format(JSON_FILE)
    proc = subprocess.call(cmd, shell=True)
    if proc != 0:
        logging.error("Mongo command execution failed\n")
        raise Exception
    logging.info("Movie data created and inserted into db...")
    t2 = time.time()
    logging.info("Total time taken by process is: {} seconds".format(t2-t1))

main()

