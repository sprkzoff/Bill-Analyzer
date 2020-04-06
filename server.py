import json
import requests
import shutil
from flask import Flask, request, abort

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


access_token = 'XMvjusLUYH8wAgmNGVHDuV90vau0s14kSoaNKQbrmQSFfHBSlm9M9uBGZ55deqbBlFzK8Xjm1tLQqkr52vbgESBJudNCjB8wo4bHM1LWJrMEt+KR3BYdB9rAQRODyNkbLWSFBoInK1nKGEG7HejfGQdB04t89/1O/w1cDnyilFU='
secret = '65b40138d95bc1b81265333cfcd2556a'


#############################

app = Flask(__name__)

line_bot_api = LineBotApi(access_token)
handler = WebhookHandler(secret)


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
    print(replyToken)

    try:
        # handler.handle(body, signature)
        if payload_message["type"] == "text": ####### text case
            print("message form")
            ### filter by case

            line_bot_api.reply_message(
                replyToken,
                TextSendMessage(text="text jaa"))
        elif payload_message["type"] == "image": ####### img case
            print("image form")
            ### get link of img and use cloud vision next

            img_link = "https://api-data.line.me/v2/bot/message/"+ payload_message['id'] +"/content"

            r=requests.get(img_link, headers={ "Authorization": "Bearer "+access_token  },stream=True)
            with open('./img.jpg', 'wb') as out_file:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, out_file)
            del r

            line_bot_api.reply_message(
                replyToken,
                TextSendMessage(text="img jaa"))
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
