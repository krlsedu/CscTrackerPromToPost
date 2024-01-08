import json
import logging
import os
from datetime import datetime, timezone

from csctracker_py_core.repository.http_repository import cross_origin
from csctracker_py_core.starter import Starter
from csctracker_queue_scheduler.services.scheduler_service import SchedulerService

starter = Starter()
app = starter.get_app()
http_repository = starter.get_http_repository()
remote_repository = starter.get_remote_repository()
url_prometeus = os.environ['URL_PROMETHEUS']

SchedulerService.init()


@app.route('/convert', methods=['GET'])
@cross_origin()
def convert():
    args = http_repository.get_args()
    SchedulerService.put_in_queue(conver_tr, args)
    return {'message': 'ok'}, 200, {'Content-Type': 'application/json; charset=utf-8'}


def conver_tr(args):
    ant_ = datetime.now()
    headers = {
        'Authorization': "Bearer " + os.environ['TOKEN_INTEGRACAO']
    }
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
    query_ = args_['query']
    if 'auto' in args:
        query_ += f"[auto = {args['auto']}]"

    converted_metric_ = get_date_end(query_, headers)
    logging.getLogger().info(converted_metric_)
    if 'auto' in args:
        if converted_metric_ is not None:
            timestamp_ = converted_metric_['timestamp_start']
        args_['start'] = timestamp_ - 11000

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
    response = http_repository.get(url_prometeus, json=body, params=args_)

    body = convert_response_to_metrics(response, headers)
    response = remote_repository.insert(f"metrics?metric&date", headers=headers, data=body)
    if converted_metric_ is None:
        converted_metric_ = {'query': query_, 'timestamp_start': args_['start'], 'timestamp_end': args_['end']}
    else:
        converted_metric_['timestamp_start'] = args_['start']
        converted_metric_['timestamp_end'] = args_['end']

    response_ = remote_repository.insert(f"converted_metrics", headers=headers, json=converted_metric_)
    logging.getLogger().info(response_.json())
    logging.getLogger().info(response.json())
    logging.getLogger().info(args_, args, datetime.now() - ant_)
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
    response = remote_repository.execute_select(select_, headers=headers)
    json_ = response
    if len(json_) > 0:
        return json_[0]


starter.start()
