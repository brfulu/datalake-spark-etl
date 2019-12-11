import boto3
import configparser
import json


def create_emr_cluster(emr_client, config):
    cluster_id = emr_client.run_job_flow(
        Name='spark-emr-cluster',
        ReleaseLabel='emr-5.28.0',
        LogUri='s3://aws-logs-086546006979-us-west-2',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 4,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'Setup - copy files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', 's3://' + config['S3']['CODE_BUCKET'], '/home/hadoop/',
                             '--recursive']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/etl.py',
                             config['DATALAKE']['INPUT_DATA'], config['DATALAKE']['OUTPUT_DATA']]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='MyEmrRole'
    )

    print('cluster created with the step...', cluster_id['JobFlowId'])


def create_bucket(s3_client, bucket_name):
    location = {'LocationConstraint': 'us-west-2'}
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


def upload_code(s3_client, file_name, bucket_name):
    s3_client.upload_file(file_name, bucket_name, 'etl.py')


def create_iam_role(iam_client):
    role = iam_client.create_role(
        RoleName='MyEmrRole',
        Description='Allows EMR to call AWS services on your behalf',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}
            }]
        })
    )

    iam_client.attach_role_policy(
        RoleName='MyEmrRole',
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
    )

    iam_client.attach_role_policy(
        RoleName='MyEmrRole',
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    )

    return role


def main():
    config = configparser.ConfigParser()
    config.read('../dl.cfg')

    iam_client = boto3.client('iam')
    # create_iam_role(iam_client)

    s3_client = boto3.client(
        's3',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    )

    # create_bucket(s3_client, config['S3']['OUTPUT_BUCKET'])
    # create_bucket(s3_client, config['S3']['CODE_BUCKET'])
    upload_code(s3_client, 'scripts/etl.py', config['S3']['CODE_BUCKET'])

    emr_client = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
    )

    create_emr_cluster(emr_client, config)


if __name__ == '__main__':
    main()
