from flask import Flask, jsonify, request, g
import sqlite3
import warnings
from pathlib import Path
import os

warnings.simplefilter(action='ignore', category=FutureWarning)

app = Flask(__name__)

file_path = Path(os.path.dirname(os.path.abspath(__file__)))

db_path = Path(file_path / "camels.db")


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(db_path)
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.route("/validate_connection", methods=["GET"])
def s_validate_connection():
    from configuration_manager import validate_connection
    ret = validate_connection()
    return ret


@app.route("/populate_database", methods=["POST"])
def s_populate_database():
    from database_manager import populate_database
    ret = populate_database()
    return jsonify(ret)


@app.route("/check_hash", methods=["GET"])
def s_get_metadata():
    from metadata_manager import check_hash
    ret = check_hash(request.args['l_hash'])
    return jsonify(ret)


@app.route("/save_metadata", methods=["POST"])
def s_save_metadata():
    from metadata_manager import write_metadata
    ret = write_metadata(request.values['meta_data'])
    return jsonify(ret)


@app.route("/check_data_status", methods=["GET"])
def s_check_data_status():
    from run_manager import check_data_status
    ret = check_data_status(request.values['l_hash'], request.values['algo_names'], request.values['task_name'],
                            request.values['metric_names'])
    return jsonify(ret)


@app.route("/save_runs", methods=["POST"])
def s_save_runs():
    from run_manager import save_runs
    ret = save_runs(request.values['evaluations'])
    return jsonify(ret)


@app.route("/train_meta_learner", methods=["POST"])
def s_train_meta_learner():
    from metamodel_manager import train_meta_learner
    ret = train_meta_learner(request.values['metric_names'], request.values['task_names'],
                             request.values['learner_names'])
    return jsonify(ret)


@app.route("/predict_with_meta_learner", methods=["GET"])
def s_predict_with_meta_learner():
    from metamodel_manager import predict_with_meta_learner
    ret = predict_with_meta_learner(request.values['meta_data'], request.values['metric_name'],
                                    request.values['task_name'], request.values['learner_name'])
    return jsonify(ret)


if __name__ == '__main__':
    app.run(host="0.0.0.0", threaded=True, debug=False)
