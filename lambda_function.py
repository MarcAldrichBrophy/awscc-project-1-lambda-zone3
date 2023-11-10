import logging
import json
from customEncoder import CustomEncoder
import boto3
import base64
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

getMethod = "GET"
healthPath = "/transcribe/health"

#main handler
def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']

    if httpMethod == getMethod and path == healthPath:
        client = boto3.resource('transcribe')

        body = json.loads(event['body'])
        audio_data_base64 = body['audio_data_base64']

        # Decode the base64 audio data
        audio_data = base64.b64decode(audio_data_base64)
        job_name = 'base64-audio-transcription'

        job = client.TranscriptionJob(
            TranscriptionJobName=job_name,
            LanguageCode='en-US',
            MediaFormat='mp3',
            Media={'MediaFileContent': audio_data}
        )

        job.start()
        
        # Wait for job to complete
        while job.get_transcription_job()['TranscriptionJobStatus'] in ['IN_PROGRESS']:
            print("Not ready yet...")
            time.sleep(5)

        print(job.get_transcript())

        response = buildResponse(200)
    else:
        response = buildResponse(404, 'Not Found')
    
    return response

# Response builder.
def buildResponse(statusCode, body = None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls = CustomEncoder)
    return response

def transcribe():
    