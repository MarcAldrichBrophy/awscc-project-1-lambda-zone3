import logging
import json
from customEncoder import CustomEncoder
import boto3
import base64
import time
import io 
import urllib3
import uuid


logger = logging.getLogger()
logger.setLevel(logging.INFO)

getMethod = "GET"
postMethod = "POST"

rootPath = "/transcribe"
healthPath = rootPath + "/health"

comprehend_client = boto3.client('comprehend')

#main handler
def lambda_handler(event, context):
    logger.info(event)
    print(boto3.__version__)
    httpMethod = event['httpMethod']
    path = event['path']

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "health OK")
    elif httpMethod == postMethod and path == rootPath:
        client = boto3.client('transcribe')
        s3 = boto3.client('s3')

        audio_data_base64 = json.loads(event['body'])['audio_data_base64']
        
        # Decode the base64 audio data
        audio_data = base64.b64decode(audio_data_base64)
        
        # check file type
        file_ext = ''
        if audio_data[:4] == b'RIFF':
            file_ext = '.wav'
        elif audio_data[:3] ==b'ID3':
            file_ext = '.mp3'
        s3_key = uuid.uuid4().hex + file_ext
        
        s3_bucket = 'project-1-datalake'

        s3.upload_fileobj(io.BytesIO(audio_data), s3_bucket, s3_key)
        
        job_uri = f"s3://{s3_bucket}/{s3_key}"
        
        job = client.start_transcription_job(
            TranscriptionJobName=s3_key,
            LanguageCode='en-US',
            MediaFormat='mp3',
            Media={'MediaFileUri': job_uri}
        )
        
        response = "None"
        while True:
            status = client.get_transcription_job(TranscriptionJobName=s3_key)
            if status['TranscriptionJob']['TranscriptionJobStatus'] == "COMPLETED":
                transcript = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
                break
            elif status['TranscriptionJob']['TranscriptionJobStatus'] == "FAILED":
                break
            
            time.sleep(0.5)
        
        transcript_text = "Error generating transcript."
        if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
            
            transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
            
            http = urllib3.PoolManager()
            response = http.request('GET', transcript_uri)
            transcript_text = json.loads(response.data.decode('utf-8'))['results']['transcripts'][0]['transcript']
        else:
            print("Transcribe failed:")
            print(status['TranscriptionJob']['FailureReason'])
        
        
        response = buildResponse(200, transcript_text)
        
        
        #upload text
        text_key = uuid.uuid4().hex
        text_file_name = f"{text_key}.txt"

        s3.put_object(Body=transcript_text.encode("utf-8"), Bucket=s3_bucket, Key=text_file_name)
        
        # get analysis for keys
        analysis_result = analyze_text(transcript_text)
        
        # get tags
        key_phrases_tags = [
            {'Key': phrase, 'Value': str(score)}
            for phrase, score in zip(analysis_result['KeyPhrases'][:10], analysis_result['KeyPhraseScores'][:10])
        ]
    
        #removing duplicates
        seen = set()
        unique_tags = []
        for tag in key_phrases_tags:
            if tag['Key'] not in seen:
                unique_tags.append(tag)
                seen.add(tag['Key'])
                
        key_phrases_tags = unique_tags
        
        #transcript tagging
        s3.put_object_tagging(
            Bucket=s3_bucket,
            Key=text_file_name,
            Tagging={'TagSet': key_phrases_tags}
        )
        
    else:
        response = buildResponse(404, 'Not Found')
    
    return response

# Analyze text using AWS Comprehend
def analyze_text(text):
    # Detect dominant language
    language_response = comprehend_client.detect_dominant_language(Text=text)
    detected_language = language_response['Languages'][0]['LanguageCode']

    # Detect sentiment
    sentiment_response = comprehend_client.detect_sentiment(Text=text, LanguageCode=detected_language)
    
    # Detect key phrases
    key_phrases_response = comprehend_client.detect_key_phrases(Text=text, LanguageCode=detected_language)

    # Build result dictionary
    result = {
        'DetectedLanguage': detected_language,
        'Sentiment': sentiment_response['Sentiment'],
        'SentimentScore': sentiment_response['SentimentScore'],
        'KeyPhrases': [phrase['Text'] for phrase in key_phrases_response['KeyPhrases']],
        'KeyPhraseScores': [str(phrase['Score']) for phrase in key_phrases_response['KeyPhrases']]
    }

    return result


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
