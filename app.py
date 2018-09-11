import os
import time
import traceback
from functools import wraps

import pymysql
from flask import *
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == 'csit' and password == 'react'


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Access Forbidden', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


def db_wrapper(f):
    @requires_auth
    def inner(*args, **kwargs):
        host = os.getenv("ENDPOINT")
        user = os.getenv("USER")
        password = os.getenv("PASSWORD")
        db = os.getenv("DATABASE")

        connection = pymysql.connect(host=host, user=user, password=password, db=db,
                                     cursorclass=pymysql.cursors.DictCursor)

        return f(connection=connection, *args, **kwargs)

    return inner


def db_get_wrapper(f):
    func_args = f.__code__.co_varnames

    @db_wrapper
    def inner(*args, **kwargs):
        since = request.args.get("since", 0, type=int)
        before = request.args.get("before", time.time(), type=int)
        page = request.args.get("page", 1, type=int)
        per_page = request.args.get("per_page", 10, type=int)
        keys = {"since": since, "before": before, "page": page, "per_page": per_page}
        for key in keys:
            if key not in func_args:
                keys.pop(key)
        return f(*args, **keys, **kwargs)

    return inner


def database(f=None, request_type=None):
    if f is not None:
        return db_wrapper(f)
    if request_type == "GET":
        return db_get_wrapper
    else:
        return db_wrapper


@app.route('/')
def index():
    return Response('Online', status=200)


@app.route('/users', methods=['GET'])
@database(request_type="GET")
def get_users(connection, page, per_page):
    try:
        with connection.cursor() as cursor:
            query = """SELECT `author_id` as USER FROM Users LIMIT %s, %s"""
            cursor.execute(query, (per_page, page))
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/channels', methods=['GET'])
@database(request_type="GET")
def get_channels(connection, page, per_page):
    try:
        with connection.cursor() as cursor:
            query = """SELECT channel_id, `COUNT(id)` as messages FROM Channels LIMIT %s, %s"""
            cursor.execute(query, (per_page, page))
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/users/<user_id>/messages', methods=['GET'])
@requires_auth
@database(request_type='GET')
def get_message_by_user(connection, since, before, page, per_page, user_id):
    try:
        with connection.cursor() as cursor:
            query = """SELECT id, channel_id, author_id, content FROM `MESSAGES` WHERE `author_id` = %s
            AND `Created_At` > from_unixtime(%s)
            AND `Created_At` < from_unixtime(%s)
            LIMIT %s, %s
            """
            cursor.execute(query, (user_id, since, before, per_page, page))
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/channels/<channel_id>/messages', methods=['GET'])
@requires_auth
@database(request_type='GET')
def get_messages(connection, since, before, per_page, page, channel_id):
    try:
        with connection.cursor() as cursor:
            query = """SELECT id, channel_id, author_id, content FROM `MESSAGES` WHERE `channel_id` = %s
            AND `Created_At` > from_unixtime(%s)
            AND `Created_At` < from_unixtime(%s)
            LIMIT %s, %s"""
            cursor.execute(query, (channel_id, since, before, per_page, page))
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/channels/<channel_id>/messages', methods=['POST', 'PUT'])
@requires_auth
@database
def create_message(connection, channel_id):
    if "content" not in request.form or "author_id" not in request.form:
        return Response("The request is incomplete/missing values", 403)

    content = request.form["content"]
    author = request.form["author_id"]

    try:
        with connection.cursor() as cursor:
            query = """INSERT INTO MESSAGES (`channel_id`, `author_id`, `content`, `Modified_By`)
             VALUES(%s, %s, %s, %s)"""
            cursor.execute(query, (channel_id, author, content, request.remote_addr))
            connection.commit()
            return jsonify({
                "result": "success",
                "msg": "record created",
            })
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0", port=80)
