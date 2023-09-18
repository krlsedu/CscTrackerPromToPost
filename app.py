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
url_repository = os.environ['URL_REPOSITORY'] + '/'
url_prometeus = os.environ['URL_PROMETHEUS']


@app.route('/convert', methods=['GET'])
@cross_origin()
def convert():
    args = request.args
    args_ = {}
    args_to_prometheus = ['start', 'end', 'step', 'query', 'timeout', 'dedup', 'partial_response', 'silence']
    for key in args.keys():
        if key in args_to_prometheus:
            args_[key] = args[key]

    if 'step' not in args_:
        args_['step'] = 60

    timestamp_ = int(datetime.now().timestamp() / int(args_['step'])) * int(args_['step'])

    if 'date' in args:
        date_end = datetime.strptime(args['date'], '%Y-%m-%d %H:%M:%S.%f')
        timestamp_ = int(date_end.timestamp() / int(args_['step'])) * int(args_['step'])

    if 'end' not in args_:
        args_['end'] = timestamp_

    if 'range' in args:

        range_ = args['range']
        if range_.endswith('s'):
            args_['start'] = timestamp_ - (int(range_[:-1]) * 1)
        elif range_.endswith('m'):
            args_['start'] = timestamp_ - (int(range_[:-1]) * 60)
        elif range_.endswith('h'):
            args_['start'] = timestamp_ - (int(range_[:-1]) * 3600)
        elif range_.endswith('d'):
            args_['start'] = timestamp_ - (int(range_[:-1]) * 86400)
        else:
            args_['start'] = timestamp_ - (int(range_) * 60)

    if 'start' not in args_:
        args_['start'] = timestamp_ - (60 * 10)


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
        last_metric = None
        for value in values_:
            count_ += 1
            print(f"processing {count_} of {len(values_) * len(result)}")
            metric_ = {'metric': metric_name, 'value': str(value[1])}
            timestamp = int(value[0])
            dt_obj = datetime.fromtimestamp(timestamp)
            dt_obj_utc = dt_obj.astimezone(timezone.utc)
            metric_['date'] = dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            if last_metric is None or last_metric['value'] != metric_['value']:
                metrics.append(metric_)
                last_metric = metric_
            else:
                print(f"metric ignored -> {metric_}")
    return metrics


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
