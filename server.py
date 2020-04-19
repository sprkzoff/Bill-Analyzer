import os
import json
import requests
import shutil
import io
import re
from flask import Flask, request, abort
import mysql.connector
from mysql.connector import Error


# Imports the Google Cloud client library
from google.cloud import vision
from google.cloud.vision import types

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
)


#############################
#                           #
#          setting          #
#                           #
#############################

from dotenv import load_dotenv
load_dotenv(verbose=True)


access_token = os.getenv('LINE_ACCESS_TOKEN')
secret = os.getenv('LINE_SECRET')

if access_token is None or secret is None:
    print("Please set environment variable either manually or in .env")
    exit(1)

image_path = './img.jpg'

#############################

app = Flask(__name__)

line_bot_api = LineBotApi(access_token)
handler = WebhookHandler(secret)


def detect_text(path):
    """Detects text in the file."""
    from google.cloud import vision
    import io
    client = vision.ImageAnnotatorClient()

    with io.open(path, 'rb') as image_file:
        content = image_file.read()

    image = vision.types.Image(content=content)

    response = client.text_detection(image=image)
    texts = response.text_annotations

    # print(texts)

    text_in_img = []

    # print('Texts:')

    for text in texts:
        # print('\n"{}"'.format(text.description))
        text_in_img.append(text.description)
        vertices = (['({},{})'.format(vertex.x, vertex.y)
                     for vertex in text.bounding_poly.vertices])

        # print('bounds: {}'.format(','.join(vertices)))

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))

    return text_in_img


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)
    print("\n")
    print(body)
    print("\n")
    # handle webhook body

    payload = json.loads(body)
    payload_message = payload["events"][0]["message"]
    replyToken = payload["events"][0]["replyToken"]
    user = payload["events"][0]["source"]
    print(replyToken)

    try:
        # handler.handle(body, signature)
        if payload_message["type"] == "text":  # text case

            # filter by case

            if payload_message["text"].strip().lower() == "all expense":
                # query find all and sum all cost
                all_expense = 0
                try:
                    connection = mysql.connector.connect(host=os.getenv('DB_HOST'),
                                                         database=os.getenv('DB_NAME'),
                                                         user=os.getenv('DB_USER'),
                                                         password=os.getenv('DB_PASSWORD'))
                    if connection.is_connected():
                        db_Info = connection.get_server_info()
                        print("Connected to MySQL Server version ", db_Info)
                        cursor = connection.cursor()
                        cursor.execute("select database();")
                        record = cursor.fetchone()
                        print("You're connected to database: ", record)

                        cursor = connection.cursor()
                        cursor.execute("select * from expense;")
                        record = cursor.fetchall()
                        all_expense = sum(
                            [e[2] for e in record if e[1] == user['userId']])

                except Error as e:
                    print("Error while connecting to MySQL", e)
                finally:
                    if (connection.is_connected()):
                        cursor.close()
                        connection.close()
                        print("MySQL connection is closed")

                line_bot_api.reply_message(
                    replyToken,
                    TextSendMessage(text="All of your expense : "+str("%.2f" %all_expense)+"$"))
            elif payload_message["text"].strip().lower() == "forecast expense":

                # use aws forecast
                line_bot_api.reply_message(
                    replyToken,
                    TextSendMessage(text="your next expense is ..."))

        elif payload_message["type"] == "image":  # img case
            print("image form")
            # get link of img and use cloud vision next

            img_link = "https://api-data.line.me/v2/bot/message/" + \
                payload_message['id'] + "/content"

            r = requests.get(img_link, headers={
                             "Authorization": "Bearer "+access_token}, stream=True)
            with open(image_path, 'wb') as out_file:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, out_file)
            del r

            price = ""
            texts = detect_text(image_path)
            print(texts[1:])
            check_total = False
            found_cost = False

            for i in range(1, len(texts)):
                # print(text)
                if not(check_total) and texts[i].lower() in {"total", "total:"} and texts[i-1].lower() != "sub":
                    print("Found total >> " + texts[i])
                    check_total = True
                if check_total:
                    if re.match(r'^-?\d+(?:\.\d+)?$', texts[i].strip("$")) is not None:
                        print("Found Cost >> "+texts[i].strip("$"))
                        price = texts[i].strip("$")
                        found_cost = True
                        break

            response_text = ""

            if not(found_cost):
                response_text = "Sorry,I cannot find your expense"
            else:
                response_text = "Your expense is " + price + "$"
                try:
                    connection = mysql.connector.connect(host=os.getenv('DB_HOST'),
                                                         database=os.getenv('DB_NAME'),
                                                         user=os.getenv('DB_USER'),
                                                         password=os.getenv('DB_PASSWORD'))
                    if connection.is_connected():
                        db_Info = connection.get_server_info()
                        print("Connected to MySQL Server version ", db_Info)
                        cursor = connection.cursor()
                        cursor.execute("select database();")
                        record = cursor.fetchone()
                        print("You're connected to database: ", record)

                        cursor = connection.cursor()
                        query = "INSERT INTO expense (username, expense) VALUES (\""+ str(user['userId']) +"\","+ price +");"
                        print("query >> ",query)
                        cursor.execute(query)
                        connection.commit()

                except Error as e:
                    print("Error while connecting to MySQL", e)
                finally:
                    if (connection.is_connected()):
                        cursor.close()
                        connection.close()
                        print("MySQL connection is closed")

            line_bot_api.reply_message(
                replyToken,
                TextSendMessage(text=response_text))

    except InvalidSignatureError:
        print("Invalid signature. Please check your channel access token/channel secret.")
        abort(400)

    return 'OK'


@app.route("/", methods=['GET'])
def landing():
    return 'landing page'


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=event.message.text))


if __name__ == "__main__":
    app.run(host='localhost', port=80)
