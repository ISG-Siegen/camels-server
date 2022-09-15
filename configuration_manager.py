from flask import send_file, Response


def validate_connection() -> Response:
    return send_file("server_database_identifier.py")
