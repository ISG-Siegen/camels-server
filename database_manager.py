from typing import Tuple
import pandas as pd
import time
from camels.server.server_context import get_db
from camels.server.server_database_identifier import Metadata, Algorithm, Metric, Task


# generates required tables
def generate_tables() -> None:
    con = get_db()

    with con:
        cur = con.cursor()

        cur.execute(
            "CREATE TABLE IF NOT EXISTS Config("
            "VersionID integer,"
            "PRIMARY KEY (VersionID))"
        )

        cur.execute(
            "CREATE TABLE IF NOT EXISTS Runs("
            "Hash string,"
            "Algorithm string,"
            "Task string,"
            "Metric string,"
            "Score real,"
            "PRIMARY KEY (Hash, Algorithm, Task, Metric))"
        )

        sql_string = "CREATE TABLE IF NOT EXISTS Metadata(" \
                     "Hash integer,"
        for metadata in Metadata:
            sql_string += f"{metadata.name} real,"
        sql_string += "FOREIGN KEY(Hash) REFERENCES Runs(Hash)," \
                      "PRIMARY KEY (Hash))"

        cur.execute(sql_string)

        cur.execute(
            "CREATE TABLE IF NOT EXISTS Algorithms("
            "Algorithm string,"
            "FOREIGN KEY (Algorithm) REFERENCES Runs(Algorithm),"
            "PRIMARY KEY (Algorithm))"
        )

        cur.execute(
            "CREATE TABLE IF NOT EXISTS Tasks("
            "Task string,"
            "FOREIGN KEY (Task) REFERENCES Runs(Task),"
            "PRIMARY KEY (Task))"
        )

        cur.execute(
            "CREATE TABLE IF NOT EXISTS Metrics("
            "Metric string,"
            "FOREIGN KEY (Metric) REFERENCES Runs(Metric),"
            "PRIMARY KEY (Metric))"
        )

    return


# adds data to a given table
def add_data(data: pd.DataFrame, table_name: str) -> Tuple[str, bool]:
    con = get_db()

    if table_name == "Runs":
        print("Do not use this function to add run data.")
        raise Exception

    with con:
        remote_data = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        if len(remote_data) > 0:
            return f"The table {table_name} already contains data.", False

        data.to_sql(table_name, con, if_exists="append", index=False)

    return f"Data was written to table {table_name}.", True


# generates database entries based on identifier file
def generate_basic_data() -> str:
    config = pd.DataFrame([[time.time_ns()]], columns=["VersionID"])

    msg_cfg, wrt_cfg = add_data(config, "Config")
    print(msg_cfg)

    algorithms = pd.DataFrame([[algorithm.name] for algorithm in Algorithm],
                              columns=["Algorithm"])

    msg_alg, wrt_alg = add_data(algorithms, "Algorithms")
    print(msg_alg)

    tasks = pd.DataFrame([[task.name] for task in Task],
                         columns=["Task"])

    msg_tsk, wrt_tsk = add_data(tasks, "Tasks")
    print(msg_tsk)

    metrics = pd.DataFrame([[metric.name] for metric in Metric],
                           columns=["Metric"])

    msg_met, wrt_met = add_data(metrics, "Metrics")
    print(msg_met)

    if any([wrt_cfg, wrt_alg, wrt_tsk, wrt_met]):
        return "Populated database with basic data."
    else:
        return "Database already populated with basic data."


# shorthand to generate and fill tables
def populate_database() -> str:
    generate_tables()
    return generate_basic_data()
