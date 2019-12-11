# datalake-spark-etl

## Udacity Data Engineer Nanodegree project
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Requirements for running
- Python3 
- AWS account

## Project structure explanation
```
postgres-data-modeling
│   README.md             # Project description
|   dl.cfg                # Configuration file
|   requirements.txt      # Python dependencies
│   
└───src                   # Source code
|   |               
│   └───notebooks         # Jupyter notebooks
|   |   |  test.ipynb     # Run sql queries agains Redshift
|   |   |
|   └───scripts
│       │  etl.py         # ETL script
|       |  deploy.py      # Deploy everything on AWS (buckets, roles, clusters) and run etl
```

## Datalake schema

#### Fact Table
- songplays (records in log data associated with song plays): songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
- users (users in the app): user_id, first_name, last_name, gender, level
- songs (songs in music database): song_id, title, artist_id, year, duration
- artists (artists in music database): artist_id, name, location, lattitude, longitude
- time (timestamps of records in songplays broken down into specific units): start_time, hour, day, week, month, year, weekday

## Instructions for running locally

#### Clone repository to local machine
```
git clone https://github.com/brfulu/datalake-spark-etl.git
```

#### Change directory to local repository
```
cd datalake-spark-etl
```

#### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Go to source directory
```
cd src/
```

#### Run etl script in local mode
```
python -m scripts.etl
```

## Instructions for running on AWS
#### Edit dl.cfg file
Do NOT publish your AWS credentials publicly.
```
[AWS]
AWS_ACCESS_KEY_ID=<AWS CREDENTIALS HERE>
AWS_SECRET_ACCESS_KEY=<AWS CREDENTIALS HERE>

[S3]
CODE_BUCKET=<CODE BUCKET NAME>
OUTPUT_BUCKET=<OUTPUT BUCKET NAME>

[DATALAKE]
INPUT_DATA=s3a://udacity-dend/
OUTPUT_DATA=s3a://<OUTPUT_BUCKET NAME>/
```

#### Deploy to AWS
The script automatically creates two S3 buckets (code_bucket and output_bucket), an IAM Role for EMR to access S3, and finally initiates the creation of the EMR cluster.
```
python -m scripts.deploy
```

#### Check results
Go to the AWS management console and to the EMR service and check the cluster and job status. 
After confirming the job run successfully, then run steps below to query the datalake.
```
jupyter notebook  # launch jupyter notebook app

# The notebook interface will appear in a new browser window or tab.
# Navigate to src/notebooks/test.ipynb and run sql queries against the datalake
```

#### Shut down AWS resources
Go to the AWS management console (Oregon region) and terminate the EMR cluster to avoid further costs.