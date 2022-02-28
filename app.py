import azure.cognitiveservices.speech as speechsdk
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import datastore
import glob
import requests
import os
import datetime
import json

from flask import Flask
import subprocess


app = Flask(__name__)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ricciwawa-6e11b342c999.json'
ricciwawa_credentials = service_account.Credentials.from_service_account_file("ricciwawa-6e11b342c999.json")
storage_client = storage.Client()
datastore_client = datastore.Client()

@app.route('/')
def main():
    return "Hello World"

@app.route('/transcription/start/<filename>/')
def initiate_transcribing(filename):
    bucket_name = "ricciwawa_mp3"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    if blob.exists():
        blob.download_to_filename(filename)
    else:
        raise FileNotFoundError
    fname, ext = filename.split(".")
    if ext == 'mp4':
        subprocess.run(f"ffmpeg -y -i {filename} -ac 1 -ar 16000 {fname}.wav", shell=True)
        filename = f"{fname}.wav"
    
    kind = "TranscriptionTask"
    task_key = datastore_client.key(kind, fname)
    print(task_key)
    task = datastore.Entity(key=task_key)
    task['fname'] = fname
    task["status"] = "not-started"
    subprocess.run('ls -al', shell=True)
    subprocess.run('ffmpeg -version', shell=True)
    subprocess.run(f"ffmpeg -i {filename} -f segment -segment_time 30 -c copy out_{fname}_%03d.wav", shell=True)
    #subprocess.run(f'rm {filename}')
    file_list = glob.glob(f"out_{fname}_*.wav")
    bucket = storage_client.bucket("ricciwawa_tmp_files")
    results = []

    for each_file_name in file_list:
        blob = (bucket.blob(each_file_name))
        blob.upload_from_filename(each_file_name)
        # TODO : Update this function and call start transcribing only once.
        print(each_file_name)
    
    results.append(start_transcribing(file_list, "zh-CN"))
    task["transcription_ids"] = results
    datastore_client.put(task)
    subprocess.run(f"rm out_{fname}_*.wav", shell=True)
    subprocess.run(f'rm {filename}', shell=True)
    return {"status": "started", "transcript_id": fname}

@app.route('/transcription/result/<tid>/')
def get_transcription(tid):
    kind = 'TranscriptionTask'
    task = datastore_client.get(key=datastore_client.key(kind, tid))
    if task is None:
        return {"error": "invalid tid"}

    success_flag = True
    text_url_list = []
    for transcription_id in task['transcription_ids']:
        result_status = get_transcription_status(transcription_id["transcription_id"])
        
        if result_status=="Succeeded":
            result_url = get_transcription_url (transcription_id["transcription_id"])
            text_url_list.append(result_url["transcription_url"])
        else:
            success_flag = False
    
    if success_flag:
        transcript = ""
        for url in text_url_list:
            response = requests.get(url).json()
            if 'combinedRecognizedPhrases' in response and response['combinedRecognizedPhrases']:
                transcript = transcript + response['combinedRecognizedPhrases'][0]['display']
       # task['transcript'] = transcript   
        task['status'] = 'completed'
        datastore_client.put(task)
        print("che")
        return json.dumps({"status": "success", 
                "transcript": transcript}, ensure_ascii=False).encode('utf8')
    
    task['status'] = 'incomplete'
    datastore_client.put(task)
    return {"status": "incomplete"}

# utils.py
def download_get_signed_up(filename, bucket_name="ricciwawa_mp3"):
    """
    Generates Signed Download URL
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(minutes=30),
        method="GET",
    )

    # print("curl '{}'".format(url))
    return url

def start_transcribing(filenames, language_code):
    bucket_name = "ricciwawa_tmp_files"
    content_urls = [download_get_signed_up(filename, bucket_name) for filename in filenames] 
    
    print(content_urls)

    
    body = {
        'contentUrls': content_urls,
        'locale': language_code,
        'displayName': f'Transcription of file using default model for {language_code}'
    }
    subscription_key = "d054b5988d384c6da942e00133de18e7"  # transfer this to settings.py
    region = "centralus"  # transfer this to settings.py
    endpoint = f'https://{region}.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions'
    headers = {'Ocp-Apim-Subscription-Key': subscription_key}
    response = requests.post(endpoint, json=body, headers=headers).json()
    print(response)
    transcription_id = response['self'].split('/')[-1]

    data = {
        'transcription_id': transcription_id,
    }
    return data


def get_transcription_status(transcription_id):
    subscription_key = "d054b5988d384c6da942e00133de18e7"  # transfer this to settings.py
    region = "centralus"  # transfer this to settings.py
    endpoint = f'https://{region}.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions/{transcription_id}'
    headers = {'Ocp-Apim-Subscription-Key': subscription_key}
    response = requests.get(endpoint, headers=headers).json()
    print(response)
    status = response['status']
    return status

def get_transcription_url(transcription_id):
    subscription_key = "d054b5988d384c6da942e00133de18e7"  # transfer this to settings.py
    region = "centralus"  # transfer this to settings.py
    endpoint = f'https://{region}.api.cognitive.microsoft.com/speechtotext/v3.0/transcriptions/{transcription_id}/files'
    headers = {'Ocp-Apim-Subscription-Key': subscription_key}
    response = requests.get(endpoint, headers=headers).json()
    data = {
        'transcription_url': response['values'][1]['links']['contentUrl'],
    }
    return data


if __name__ == "__main__":
    app.run(host='0.0.0.0')

