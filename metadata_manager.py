import sqlite3
from typing import Tuple
import pandas as pd
import json
from camels.server.server_context import get_db


# checks if given hash exists in database
def check_hash(l_hash: str) -> Tuple[str, bool]:
    con = get_db()

    with con:
        remote_metadata = pd.read_sql_query(f"SELECT * FROM Metadata", con)

    # check if the metadata already exists and get a new id
    if len(remote_metadata) > 0:
        exist = remote_metadata[remote_metadata["Hash"] == int(l_hash)]
        if len(exist) > 0:
            return f"Metadata for data set hash {l_hash} already exists on server.", False

    return f"Metadata for data set hash {l_hash} does not exist on the server.", True


# writes the metadata object to the database
def write_metadata(meta_data: str) -> str:
    con = get_db()

    meta_data = json.loads(meta_data)

    with con:
        # write metadata
        try:
            pd.DataFrame(meta_data).to_sql("Metadata", con, if_exists="append", index=False)
        except sqlite3.IntegrityError:
            print("Exception: Tried to write a duplicate metadata set.")
            return "Tried to write a duplicate metadata set."

    return f"Saved metadata for data set hash {meta_data['Hash'][0]}."
