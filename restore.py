import boto3
import clickhouse_connect

s3_access_id = ""
s3_secret_key = ""
s3_url = ""
pods=['clickhouse-shard0-1'] #''
host = "localhost"
username = "default"
password = ""
pod_name = 'clickhouse-shard0-1'
backup_time = '2025_06_03-17_23_12'

client = clickhouse_connect.get_client(host=host, username=username, password=password)

s3 = boto3.client(
    "s3",
    aws_access_key_id=s3_access_id,
    aws_secret_access_key=s3_secret_key,
    endpoint_url=s3_url,
    region_name="msk"
)

backups = s3.list_objects_v2(
        Bucket='backup-test-clickhouse', Prefix=f"{pod_name}/{backup_time}/", Delimiter="/")

if backups.get("CommonPrefixes", False):
    for obj in backups["CommonPrefixes"]:
        path = obj['Prefix']
        dumps_list = path.split("/")[2]
        dumps_list_ = dumps_list.split("__", 1)
        dumps_list_.pop(0)
        _ = ''.join(dumps_list_)
        result = _.split("___")
        print(result)
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {result[0]} ON CLUSTER default")
        except Exception as ex:
            print(f"Ошибка: {ex}")

        try:
            client.command(
                f"RESTORE TABLE {result[0]}.{result[1]} ON CLUSTER default FROM S3('{s3_url}/backup-test-clickhouse/{pod_name}/{backup_time}/"
                f"{backup_time}__{result[0]}___{result[1]}',"
                f" '{s3_access_id}', '{s3_secret_key}')")
        except Exception as ex:
            print(f"Ошибка: {ex}")