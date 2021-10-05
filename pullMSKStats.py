# -*- coding: utf-8 -*-

# input params
# path to the config file, see pullStatsConfig.json

from optparse import OptionParser

import boto3
import datetime
import os
import pandas as pd
import sys

# Metric Collection Period (in days)
METRIC_COLLECTION_PERIOD_DAYS = 7

# Aggregation Duration (in seconds)
AGGREGATION_DURATION_SECONDS = 3600

# Different versions of python 2.7 have renamed modules
try:
    from configparser import ConfigParser

except ImportError:
    from ConfigParser import ConfigParser


def get_average_metrics():
    metrics = [
        'BytesInPerSec',
        'BytesOutPerSec',
        'MessagesInPerSec',
        'CpuUser']
    return metrics


def get_peak_metrics():
    metrics = [
        'BytesInPerSec',
        'BytesOutPerSec',
        'MessagesInPerSec',
        'CpuUser',
        'ConnectionCount',
        'PartitionCount',
        'GlobalTopicCount',
        'EstimatedMaxTimeLag',
        'LeaderCount',
        'ReplicationBytesOutPerSec',
        'ReplicationBytesInPerSec',
        'MemoryFree',
        'MemoryUsed']
    return metrics


def calc_expiry_time(expiry):
    """Calculate the number of days until the reserved instance expires.
    Args:
        expiry (DateTime): A timezone-aware DateTime object of the date when
            the reserved instance will expire.
    Returns:
        The number of days between the expiration date and now.
    """
    return (expiry.replace(tzinfo=None) - datetime.datetime.utcnow()).days


def get_clusters_info(session):
    """Calculate the running/reserved instances in ElastiCache.
    Args:
        session (:boto3:session.Session): The authenticated boto3 session.
    Returns:
        A dictionary of the running/reserved instances for ElastiCache nodes.
    """
    conn = session.client('kafka')
    results = {
        'msk_running_instances': {}
    }

    paginator = conn.get_paginator('list_clusters')
    page_iterator = paginator.paginate()
    # Loop through running ElastiCache instance and record their engine,
    # type, and name.
    for page in page_iterator:
        for instance in page['ClusterInfoList']:
            if instance['State'] == 'ACTIVE':
                cluster_id = instance['ClusterName']
                results['msk_running_instances'][cluster_id] = instance

    return results


def get_metric(cloud_watch, cluster_id, node, metric, is_peak, time_period=METRIC_COLLECTION_PERIOD_DAYS):
    """Write node related metrics to file
    Args:
        ClusterId, node and metric to write
    Returns:
    The metric value
    """
    today = datetime.date.today() + datetime.timedelta(days=1)
    then = today - datetime.timedelta(days=time_period)

    period=AGGREGATION_DURATION_SECONDS
    if not is_peak:
        period=AGGREGATION_DURATION_SECONDS * 24 * time_period

    if metric != 'GlobalTopicCount':
        response = cloud_watch.get_metric_statistics(
            Namespace='AWS/Kafka',
            MetricName=metric,
            Dimensions=[
                {'Name': 'Cluster Name', 'Value': cluster_id},
                {'Name': 'Broker ID', 'Value': str(node)}
            ],
            StartTime=then.isoformat(),
            EndTime=today.isoformat(),
            Period=period,
            Statistics=['Average']
        )
    else:
        response = cloud_watch.get_metric_statistics(
            Namespace='AWS/Kafka',
            MetricName=metric,
            Dimensions=[
                {'Name': 'Cluster Name', 'Value': cluster_id}
            ],
            StartTime=then.isoformat(),
            EndTime=today.isoformat(),
            Period=period,
            Statistics=['Average']
        )

    rec_max = 0
    for rec in response['Datapoints']:
        if rec['Average'] > rec_max:
            rec_max = rec['Average']

    return rec_max


def create_data_frame():
    """Create an empty dataframe with headers
    Args:
    Returns:
    The newely created pandas dataframe
    """
    df_columns = ["Region", "ClusterName", "NodeId", "NodeType", "KafkaVersion"]
    for metric in get_average_metrics():
        df_columns.append(('%s (avg over last week)' % metric))
    for metric in get_peak_metrics():
        df_columns.append(('%s (max over last week)' % metric))
    df = pd.DataFrame(columns=df_columns)
    return df


def write_cluster_info(df, clusters_info, session, region, options):
    """Write all the data gathered to the file
    Args:
        The cluster information dictionary
    Returns:
    """
    cloud_watch = session.client('cloudwatch')
    row = []
    i = 0

    running_instances = clusters_info['msk_running_instances']
    for instanceId, instanceDetails in running_instances.items():
        for nodeId in range(1, instanceDetails['NumberOfBrokerNodes'] + 1):
            print("Getting cluster % s details" % instanceId)
            print(instanceDetails['BrokerNodeGroupInfo'])
            row.append("%s" % region)
            row.append("%s" % instanceId)
            row.append("%s" % nodeId)
            row.append("%s" % instanceDetails['BrokerNodeGroupInfo']['InstanceType'])
            row.append("%s" % instanceDetails['CurrentBrokerSoftwareInfo']['KafkaVersion'])

            # if instanceDetails['EnhancedMonitoring'] != 'PER_TOPIC_PER_PARTITION' and \
            #         instanceDetails['EnhancedMonitoring'] != 'PER_TOPIC' and \
            #         instanceDetails['EnhancedMonitoring'] != 'PER_BROKER':
            #     print("Cluster monitoring is off, cannot gather more information")
            #     return

            for metric in get_average_metrics():
                row.append(
                    get_metric(
                        cloud_watch,
                        instanceId,
                        nodeId,
                        metric,
                        False))

            for metric in get_peak_metrics():
                row.append(
                    get_metric(
                        cloud_watch,
                        instanceId,
                        nodeId,
                        metric,
                        True))
            df.loc[i] = row
            row = []
            i += 1

    df.sort_values(by=['ClusterName', 'NodeId'])


def get_reserved_instances(clusters_info):
    # create the dataframe
    df_columns = ["Instance Type", "Count", "Remaining Time (days)"]
    df = pd.DataFrame(columns=df_columns)

    row = []
    i = 0
    reserved_instances = clusters_info['elc_reserved_instances']
    for instanceId, instanceDetails in reserved_instances.items():
        row.append(("%s" % instanceId))
        row.append(("%s" % instanceDetails['count']))
        row.append(("%s," % instanceDetails['expiry_time']))
        df.loc[i] = row
        i = i + 1
        row = []

    return (df, i)


def get_costs(session):
    # query pricing
    pr = session.client('ce')
    now = datetime.datetime.now()
    start = (now - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')
    pricing_data = pr.get_cost_and_usage(TimePeriod={'Start': start,
                                                     'End': end},
                                         Granularity='MONTHLY',
                                         Filter={
        "And": [{"Dimensions": {'Key': 'REGION',
                                'Values': [session.region_name]}},
                {"Dimensions": {'Key': 'SERVICE',
                                'Values': ['Amazon Managed Streaming for Apache Kafka']}}]},
        Metrics=['UnblendedCost'])

    costs = 0
    for res in pricing_data['ResultsByTime']:
        costs = costs + float(res['Total']['UnblendedCost']['Amount'])

    return costs


def process_aws_account(config, section, options):
    print('Grab a coffee this script takes a while...')
    # connect to ElastiCache
    # aws key, secret and region
    region = config.get(section, 'region')
    access_key = config.get(section, 'aws_access_key_id')
    secret_key = config.get(section, 'aws_secret_access_key')

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region)

    output_file_path = "%s/%s-%s.xlsx" % (options.outDir, section, region)
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')

    print('Writing Headers')
    cluster_df = create_data_frame()

    print('Gathering data...')
    clusters_info = get_clusters_info(session)

    print('Writing data...')
    write_cluster_info(cluster_df, clusters_info, session, region, options)
    cluster_df.to_excel(writer, 'ClusterData')

    costs = get_costs(session)
    customer_costs = {'costs': [costs]}
    costs_df = pd.DataFrame(customer_costs)
    costs_df.to_excel(writer, 'Costs', index=False)

    writer.save()


def main():
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="configFile",
                      help="Location of configuration file", metavar="FILE")
    parser.add_option("-d", "--out-dir", dest="outDir", default=".",
                      help="directory to write the results in", metavar="PATH")

    (options, args) = parser.parse_args()
    if options.configFile is None:
        print("Please run with -h for help")
        sys.exit(1)

    config = ConfigParser()
    config.read(options.configFile)

    for section in config.sections():
        if not os.path.isdir(options.outDir):
            os.makedirs(options.outDir)

        process_aws_account(config, section, options)


if __name__ == "__main__":
    main()
