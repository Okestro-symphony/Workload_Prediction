import os
import sys
import uuid
import datetime
from configparser import ConfigParser
from math import ceil
import ray

project_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_path)

from pipelines.prediction.decompose.decomposer import Decomposition
from utils.logs.log import standardLog
from utils.result import Reporting


standardLog = standardLog()
reporting = Reporting(job='Time Series Decomposition')

config = ConfigParser()
config.read(os.path.join(project_path, 'config.ini')) 

data_path = config.get('Preprocessing', 'DATA_DIR')

save_path = project_path + '/data/decompose_data'
if not os.path.exists(save_path):
    os.makedirs(save_path, exist_ok=True)


providers = ['pod', 'vm']
metrics = {"vm":[ "cpu", "diskio-write", "diskio-read","memory","network-in", "network-out","filesystem"],
            "pod":["cpu", "memory"]}

metric_col = {'cpu': 'mean_cpu_usage',
            'memory' : 'mean_memory_usage',
            'network-in' : 'mean_network_in_bytes',
            'network-out' : 'mean_network_out_bytes',
            'diskio-read' : 'mean_read_bytes',
            'diskio-write' : 'mean_write_bytes',
            'filesystem' : 'mean_filesystem_usage'
            }

def main(provider: str,
        metric: str,
        host_thr: int=70
        ):
    try:
        decomposition_id=str(datetime.datetime.now())[:10] + str(uuid.uuid1())
        print(f'Decomposition analysis ID : {decomposition_id}')
        print(f'===============Decompose PROVIDER : {provider} METRIC : {metric}===============')
        metric_to_read = metric.split("-")[0] if provider == 'vm' else 'pod'
        decomposer = Decomposition(
                                decomposition_id=decomposition_id,
                                provider=provider,
                                metric_to_read=metric_to_read,
                                metric=metric,
                                feature=metric_col[metric],
                                data_path=data_path,
                                save_path= save_path,
                                duration=3,
                                period='M',
                                agg_interval='5m',
                                duration_threshold=3,
                                seasonal_threshold=0.4
                                )
        try:
            df = decomposer.read_data()
            decomposer.analysis_ts()
        except Exception as e:
            standardLog.sending_log('error', e).error(f'Fail to load preprocessed {metric_to_read} csv data')
            reporting.report_result(result='fail', error='read')
        try:
            print(f'Host : {len(decomposer.hosts)}')
            for p in decomposer.periods:
                print(f'--------------------------------decompose by {p}--------------------------------')
                decomp_period = decomposer.period_dict[p] * decomposer.duration_threshold
                smoother_len = decomp_period + 1 if decomp_period % 2 == 0 else decomp_period
                start = 0
                for i in range(ceil(decomposer.n_hosts/host_thr)):
                    ray.init()
                    print(f'/////////////////////////////// Start ray session {i} ///////////////////////////////')
                    end = start + host_thr - 1 if (start+host_thr) <  decomposer.n_hosts else decomposer.n_hosts - 1
                    ray.get([decomposer.decompose_host.remote(decomposer, df, i, p, smoother_len) for i in range(start, end)])
                    start += host_thr
                    ray.shutdown()
                    print(f'/////////////////////////////// Shutdown ray session {i} ///////////////////////////////')
        except Exception as e:
            standardLog.sending_log('error', e).error(f'Fail to save data')
            reporting.report_result(result='fail', error='write')
    except Exception as e:
        standardLog.sending_log('error', e).error(f'Decomposition model Fail')
        reporting.report_result(result='fail', error='Decomposition model Fail')   
    standardLog.sending_log('success').info('Time Series Decomposition success')
    reporting.report_result(result='success')

if __name__ == '__main__':
    start_time = datetime.datetime.now(datetime.timezone.utc)
    for provider in providers:
        metric_list = metrics[provider]
        for metric in metric_list:
            main(provider, metric)
    end_time = datetime.datetime.now(datetime.timezone.utc)
    print(f'start : {start_time}, \n end: {end_time}')
