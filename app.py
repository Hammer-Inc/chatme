import os
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


@app.route('/')
def index():
    return Response('Online', status=200)


@app.route('/users', methods=['GET'])
@requires_auth
def get_users():
    try:
        with connect().cursor() as cursor:
            query = """SELECT `author_id` as USER FROM Users"""
            cursor.execute(query)
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/channels', methods=['GET'])
@requires_auth
def get_channels():
    try:
        with connect().cursor() as cursor:
            query = """SELECT channel_id, `COUNT(id)` as messages FROM Channels"""
            cursor.execute(query)
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/users/<user_id>/messages', methods=['GET'])
@requires_auth
def get_message_by_user(user_id):
    try:
        with connect().cursor() as cursor:
            query = """SELECT * FROM MESSAGES WHERE `author_id` = %s"""
            cursor.execute(query, user_id)
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/channels/<channel_id>/messages', methods=['GET'])
@requires_auth
def get_messages(channel_id):
    try:
        with connect().cursor() as cursor:
            query = """SELECT * FROM MESSAGES WHERE `channel_id` = %s"""
            cursor.execute(query, channel_id)
            result = cursor.fetchall()
            return jsonify(result)
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


@app.route('/channels/<channel_id>/messages', methods=['POST', 'PUT'])
@requires_auth
def create_message(channel_id):
    if "content" not in request.form or "author_id" not in request.form:
        return Response("The request is incomplete/missing values", 403)

    content = request.form["content"]
    author = request.form["author_id"]
    try:
        connection = connect()
        with connection.cursor() as cursor:
            query = """INSERT INTO MESSAGES VALUES(NULL, %s, %s, %s)"""
            cursor.execute(query, (channel_id, author, content))
            connection.commit()
            return jsonify({
                "result": "success",
                "msg": "record created",
            })
    except:
        traceback.print_exc()
        return Response("A Database Error Occurred", 500)


def connect():
    host = os.getenv("ENDPOINT")
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    db = os.getenv("DATABASE")

    connection = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=pymysql.cursors.DictCursor)

    return connection


if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0", port=80)
