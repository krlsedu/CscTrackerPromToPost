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
    headers = {
        'Authorization': "Bearer " + os.environ['TOKEN_INTEGRACAO']
    }
    args = request.args
    args_ = {}
    args_to_prometheus = ['start', 'end', 'step', 'query', 'timeout', 'dedup', 'partial_response', 'silence']
    for key in args.keys():
        if key in args_to_prometheus:
            args_[key] = args[key]

    if 'step' not in args_:
        args_['step'] = 60

    if 'step_dest' in args:
        step_dest = args['step_dest']
    else:
        step_dest = args_['step']

    timestamp_ = int(datetime.now().timestamp() / int(step_dest)) * int(step_dest)

    if 'date' in args:
        date_end = datetime.strptime(args['date'], '%Y-%m-%d %H:%M:%S.%f')
        timestamp_ = int(date_end.timestamp() / int(step_dest)) * int(step_dest)
    converted_metric_ = get_date_end(args_['query'], headers)
    print(converted_metric_)
    if 'auto' in args:
        if converted_metric_ is not None:
            timestamp_ = converted_metric_['timestamp_start']

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
        args_['start'] = timestamp_ - (11000 * int(args_['step']))

    body = None
    response = requests.get(url_prometeus, json=body, params=args_)

    body = convert_response_to_metrics(response, headers)
    response = requests.post(f"{url_repository}metrics?metric&date", headers=headers, json=body)
    if converted_metric_ is None:
        converted_metric_ = {'query': args_['query'], 'timestamp_start': args_['start'], 'timestamp_end': args_['end']}
    else:
        converted_metric_['timestamp_start'] = args_['start']
        converted_metric_['timestamp_end'] = args_['end']

    response_ = requests.post(f"{url_repository}converted_metrics", headers=headers, json=converted_metric_)
    print(response_.json())
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
        values_.sort(key=lambda x: x[0])
        for value in values_:
            count_ += 1
            metric_ = {'metric': metric_name, 'value': str(value[1])}
            timestamp = int(value[0])
            dt_obj = datetime.fromtimestamp(timestamp)
            dt_obj_utc = dt_obj.astimezone(timezone.utc)
            metric_['date'] = dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            if last_metric is None or last_metric['value'] != metric_['value']:
                metrics.append(metric_)
                last_metric = metric_
    return metrics


def get_date_end(query, headers=None):
    select_ = (f"select * from converted_metrics cm "
               f"where cm.query = '{query}' order by timestamp_start limit 1")
    body = {'command': select_}
    response = requests.post(f"{url_repository}command/select", headers=headers, json=body)
    json_ = response.json()
    if len(json_) > 0:
        return json_[0]


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
