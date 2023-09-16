import json
import os
from datetime import datetime, timezone

import requests as requests
from flask import Flask
from flask import request
from flask_cors import CORS, cross_origin
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

metrics = PrometheusMetrics(app, group_by='endpoint', default_labels={'application': 'CscTrackerRedirect'})
url_repository = 'http://127.0.0.1:5001' + '/'  # https://backend.csctracker.com/repository
# url_repository = os.environ['URL_REPOSITORY'] + '/'  # https://backend.csctracker.com/repository
url_prometeus = os.environ['URL_PROMETHEUS']  # https://prometheus.csctracker.com/api/v1/query_range


@app.route('/convert', methods=['GET'])
@cross_origin()
def convert():
    args = request.args
    args_ = {}
    for key in args.keys():
        args_[key] = args[key]

    if 'step' not in args_:
        args_['step'] = 60

    timestamp_ = int(datetime.now().timestamp() / int(args_['step'])) * int(args_['step'])

    if 'start' not in args_:
        args_['start'] = timestamp_ - (60 * 10)

    if 'end' not in args_:
        args_['end'] = timestamp_

    body = None
    response = requests.get(url_prometeus, json=body, params=args_)

    headers = {
        'Authorization': "Bearer " + os.environ['TOKEN_INTEGRACAO']
    }

    body = convert_response_to_metrics(response, headers)
    response = requests.post(f"{url_repository}metrics?metric&date", headers=headers, json=body)
    return response.json(), 200, {'Content-Type': 'application/json; charset=utf-8'}


def convert_response_to_metrics(response, headers=None):
    json_ = response.json()
    result = json_['data']['result']
    metrics = []
    count_ = 0
    for metric in result:
        metric_name = json.dumps(metric['metric'])
        values_ = metric['values']
        for value in values_:
            count_ += 1
            print(f"processing {count_} of {len(values_) * len(result)}")
            metric_ = {'metric': metric_name, 'value': str(value[1])}
            timestamp = int(value[0])
            dt_obj = datetime.fromtimestamp(timestamp)
            dt_obj_utc = dt_obj.astimezone(timezone.utc)
            metric_['date'] = dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            metrics.append(metric_)
    return metrics


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
