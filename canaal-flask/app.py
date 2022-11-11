import os
from flask import Flask, render_template, abort, url_for, request, jsonify

import json
import requests

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def index():
    if request.method == "POST":
        address = request.form['address']
        if address:
            merkle = requests.get(f'https://xf6u3cmhe7.execute-api.us-east-2.amazonaws.com/proof/{address}')
            if merkle.text != '[]':
                    data = requests.get("https://api.kucoin.com/api/v1/market/stats?symbol=BTC-USDT")
                    response = [data]
                    response = [str(i.json()) for i in response]
                    return json.dumps(response)
            else:
                return json.dumps({'hi':'Unauthorized Account'})
        else:
            return json.dumps({'hi':'MetaMask not Connected'})
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)