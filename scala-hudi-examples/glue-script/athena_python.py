'''
copy of https://gist.githubusercontent.com/quiver/77572d7d11be1c042ea3404bb465e047/raw/b2ef96fecfbf93f4e46132c724f179f80c5c1d76/athena.py
'''

#!/usr/bin/env python
# vim: set fileencoding=utf8 :
```
$ pip install -U boto3 retrying
$ export AWS_DEFAULT_PROFILE=test
$ cat foo.sql
select count(*)
from bar
$ python athena.py foo.sql
$ ls -1
athena.log  # program log
athena.py   # main program
foo.sql     # sql
foo.sql.csv # query result

$ cat foo.sql.csv # check query result
"_col0"
"1234"
'''
import logging
import pprint
import sys

import boto3
from retrying import retry

logging.basicConfig(filename='athena.log',level=logging.INFO)

athena = boto3.client('athena')
s3 = boto3.resource('s3')

S3BUCKET_NAME = 'XXX'
DATABASE_NAME = 'YYY'

@retry(stop_max_attempt_number = 10,
       wait_exponential_multiplier = 30 * 1000,
       wait_exponential_max = 10 * 60 * 1000)
def poll_status(_id):
    '''
    poll query status
    '''
    result = athena.get_query_execution(
        QueryExecutionId = _id
    )

    logging.info(pprint.pformat(result['QueryExecution']))
    state = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception

def query_to_athena(filename):
    sql = open(filename, 'r').read()
    result = athena.start_query_execution(
        QueryString = sql,
        QueryExecutionContext = {
            'Database': DATABASE_NAME
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + S3BUCKET_NAME,
        }
    )

    logging.info(pprint.pformat(result))

    QueryExecutionId = result['QueryExecutionId']
    result = poll_status(QueryExecutionId)

    # save response
    with open(filename + '.log', 'w') as f:
        f.write(pprint.pformat(result, indent = 4))

    # save query result from S3
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = QueryExecutionId + '.csv'
        local_filename = filename + '.csv'
        s3.Bucket(S3BUCKET_NAME).download_file(s3_key, local_filename)

def main():
    for filename in sys.argv[1:]:
        try:
            query_to_athena(filename)
        except Exception, err:
            print err

if __name__ == '__main__':
    main()
    
'''
  Glue Sample 
  
  import boto3

  athena = boto3.client('athena')
  s3 = boto3.resource('s3')
  result = athena.start_query_execution(
          QueryString = 'select * from tablename',
          QueryExecutionContext = {
              'Database': 'default'
          },
          ResultConfiguration = {
              'OutputLocation': 's3://bucketname/Glue/customer/PETCO/ecc22'
          }
      )  
'''