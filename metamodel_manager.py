import os
from pathlib import Path
from typing import List
import pandas as pd
import numpy as np
import json
from camels.server.server_context import get_db
from camels.server.server_database_identifier import Learner
from camels.meta_learner import meta_fit_and_predict, meta_train_best_model, meta_predict

file_path = Path(os.path.dirname(os.path.abspath(__file__)))


# trains and saves the meta learner
def train_meta_learner(metric_names: str, task_names: str, learner_names: str) -> List:
    from sklearn.model_selection import LeaveOneOut

    con = get_db()

    metric_names = json.loads(metric_names)
    task_names = json.loads(task_names)
    learner_names = json.loads(learner_names)

    with con:
        # load required tables
        runs = pd.read_sql_query(f"SELECT * FROM Runs", con)
        metadata = pd.read_sql_query(f"SELECT * FROM Metadata", con)
        config = pd.read_sql_query(f"SELECT * FROM Config", con)

    version_id = config["VersionID"][0]

    # saves the return messages for the client
    final_results = []

    # get the number of metadata features
    metadata_features = len(list(metadata.drop(columns=["Hash"])))

    # loop all tasks and metrics
    for task in task_names:
        for metric in metric_names:

            # arrange the metadata and algorithm score
            train_md = pd.DataFrame()
            for metadata_hash in runs["Hash"].unique():
                eval_runs = runs.loc[(runs["Hash"] == metadata_hash) &
                                     (runs["Task"] == task) &
                                     (runs["Metric"] == metric)].copy()
                metadata_run = metadata.loc[metadata["Hash"] == metadata_hash].copy()
                metadata_run.drop(columns=["Hash"], inplace=True)
                for run in eval_runs.loc[runs["Hash"] == metadata_hash].itertuples():
                    metadata_run[f"{run.Algorithm}"] = run.Score
                train_md = pd.concat([train_md, metadata_run])

            # get and fit the models
            for learner in learner_names:
                # validate the model performance
                print(f"Evaluating {learner} with n-repetition leave-one-out cross-validation.")
                eval_results = []
                top_n_results = []
                n_rep = 50
                top_n_max = 7
                for i in range(n_rep):
                    if (i + 1) % 1 == 0:
                        print(f"Evaluation step {i + 1}/{n_rep}...")
                    loo = LeaveOneOut()
                    results_per_model = {k: [[], []] for k in ["VBA", "SBA", "EPM"]}
                    top_n_hits = {k: [] for k in range(2, top_n_max + 1)}
                    for train_index, test_index in loo.split(train_md):
                        train_eval, test_eval = train_md.iloc[train_index], train_md.iloc[test_index]

                        x_train_eval = train_eval.iloc[:, :metadata_features].to_numpy()
                        y_train_eval = train_eval.iloc[:, metadata_features:].to_numpy()

                        x_test_eval = test_eval.iloc[:, :metadata_features].to_numpy()
                        y_test_eval = test_eval.iloc[:, metadata_features:].to_numpy().flatten()

                        # fit and predict with epm (meta-learner)
                        epm_selection = meta_fit_and_predict(x_train_eval, y_train_eval, x_test_eval, Learner[learner])

                        # write SBA, VBA and EPM to list
                        predicted_idx_list = [("SBA", np.argmin(y_train_eval.sum(axis=0))),
                                              ("VBA", np.argsort(y_test_eval)[0]),
                                              ("EPM", epm_selection)]

                        # get error for models
                        for model_name, idx in predicted_idx_list:
                            results_per_model[model_name][0].append(y_test_eval[idx])
                            results_per_model[model_name][1].append(idx)

                        for top in range(2, top_n_max + 1):
                            top_n_hits[top].append(True) if epm_selection in np.argsort(y_test_eval)[:top] \
                                else top_n_hits[top].append(False)

                    res_table = []
                    res_columns = ["Selection Method", "Average Error", "Selection Accuracy"]

                    vba_res = results_per_model.pop("VBA")
                    oracle_indices = vba_res[1]
                    res_table.append(("VBA", np.mean(vba_res[0]), 1))
                    for model_name, (scores, predicted_indices) in results_per_model.items():
                        sel_acc = sum(i == j for i, j in zip(predicted_indices, oracle_indices)) / len(oracle_indices)
                        res_table.append((model_name, np.mean(scores), sel_acc))

                    res_df = pd.DataFrame(res_table, columns=res_columns)
                    eval_results.append(res_df)

                    for key in top_n_hits:
                        top_n_hits[key] = np.sum(top_n_hits[key]) / len(top_n_hits[key])

                    top_n_results.append(top_n_hits)

                eval_df = pd.concat(eval_results, axis=1)
                mean_res = eval_df.drop(columns=["Selection Method"]).groupby(level=0, axis=1).mean()
                mean_res = pd.concat([mean_res, eval_df["Selection Method"].iloc[:, 0]], axis=1)
                print(mean_res)
                final_results.append(mean_res.to_json())

                top_n_agg = {k: [] for k in range(2, top_n_max + 1)}
                for result in top_n_results:
                    for key in result:
                        top_n_agg[key].append(result[key])

                for key, values in top_n_agg.items():
                    print(f"Top {key} hit percentage: {np.mean(values)}.")

                # train the final model
                x_t = train_md.iloc[:, :metadata_features]
                y_t = train_md.iloc[:, metadata_features:]

                print("Training and saving meta-learner.")
                meta_train_best_model(x_t, y_t, Learner[learner],
                                      Path(file_path / f"{version_id}_{learner}/"),
                                      Path(f"{task}_{metric}"))

    # return the result string list
    return final_results


def predict_with_meta_learner(meta_data: str, metric_name: str, task_name: str, learner_name: str):
    con = get_db()

    meta_data = json.loads(meta_data)
    meta_data["IDX"] = [0]
    meta_data = pd.DataFrame(meta_data).drop(columns=["IDX"])

    with con:
        config = pd.read_sql_query(f"SELECT * FROM Config", con)

    version_id = config["VersionID"][0]

    return meta_predict(meta_data, Path(file_path / f"{version_id}_{learner_name}/{task_name}_{metric_name}"))
