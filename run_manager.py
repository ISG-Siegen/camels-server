import sqlite3
from typing import Any
import pandas as pd
import json
from camels.server.server_context import get_db
from camels.server.server_database_identifier import Algorithm, Task, Metric


# check if runs exist for the given metadata on algorithms, tasks and metrics
def check_data_status(l_hash: str, algo_names: str, task_name: str, metric_names: str) -> Any:
    con = get_db()

    with con:
        remote_runs = pd.read_sql_query(f"SELECT * FROM Runs", con)

    # filter runs by parameters
    data_runs = remote_runs.loc[
        (remote_runs["Hash"] == int(l_hash)) &
        (remote_runs["Algorithm"].isin([Algorithm[algo].name for algo in json.loads(algo_names)])) &
        (remote_runs["Task"] == Task[task_name].name) &
        (remote_runs["Metric"].isin([Metric[metric].name for metric in json.loads(metric_names)]))]

    return data_runs[["Algorithm", "Metric"]].values.tolist()


# write completed runs to database
def save_runs(evaluations):
    con = get_db()

    with con:
        # transform the evaluations into dataframes
        evaluations = json.loads(evaluations)
        for evaluation in evaluations:
            try:
                pd.DataFrame(evaluation).to_sql("Runs", con, if_exists="append", index=False)
            except sqlite3.IntegrityError:
                print("Tried to write a duplicate configuration.")
                continue

    return "Runs were saved."
