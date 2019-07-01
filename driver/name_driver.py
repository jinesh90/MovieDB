import pandas as pd
import os
import subprocess
import logging
from os import path


current_path = os.getcwd()
NAMEFILE_PATH = current_path + "/imdb/names.tsv"
JSON_FILE = "name.json"
FORMAT = '%(asctime)-15s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)

def create_name_json(filepath):
    df = pd.read_csv(filepath, sep='\t', low_memory=False)
    df["birthYear"] = df["birthYear"].apply(lambda x: "" if x == "\\N" else x)
    df["deathYear"] = df["deathYear"].apply(lambda x: "" if x == "\\N" else x)
    df["knownForTitles"] = df["knownForTitles"].apply(lambda x: x.split(","))
    df["primaryProfession"] = df["primaryProfession"].apply(lambda x: x.split(",") if not isinstance(x, float) else "")
    name_json = df.to_json(JSON_FILE, orient='records')
    logging.info("Basic JSON file created: {}...\n".format(JSON_FILE))


def check_json_created(filename):
    if path.exists(current_path + '/{}'.format(filename)):
        return True
    else:
        logging.warning("JSON file is not exist\n")
        return False


def json_insert(filename):
    cmd = "mongoimport --jsonArray --db movie --collection person --file {}".format(filename)
    proc = subprocess.call(cmd,shell=True)
    if proc != 0:
        logging.error("Mongo command execution failed\n")
        raise Exception


def main():
    try:
        create_name_json(NAMEFILE_PATH)
        if check_json_created(JSON_FILE):
            logging.info("JSON file created successfully, start inserting to DB..\n")
            json_insert(JSON_FILE)
    except Exception:
        logging.error("There is something going wrong")

main()
