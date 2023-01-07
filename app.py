# import libraries
import json
import os
from flask import Flask, redirect, session, url_for, render_template, request, abort
import random
import string
import requests
import time
import json
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from urllib.parse import urlencode
from dotenv import load_dotenv
import pandas as pd
from sklearn.preprocessing import StandardScaler

# loading environment
load_dotenv()

# cloud variables
# setting up client and credentials
CREDENTIALS = json.loads(os.environ.get("CREDENTIALS"))

if os.path.exists('credentials.json'):
    pass
else:
    with open('credentials.json', 'w') as credentials:
        json.dump(CREDENTIALS, credentials)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

# function to create a dataset in bigquery
def bq_create_dataset(client, dataset):
    dataset_ref = bigquery_client.dataset(dataset)

    try:
        dataset = bigquery_client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    return dataset

# function to create a dataset
def bq_create_table(client, dataset, table_name):
    dataset_ref = bigquery_client.dataset(dataset)

    # prepares a reference to the table
    table_ref = dataset_ref.table(table_name)

    try:
        table =  bigquery_client.get_table(table_ref)
        print('table {} already exists.'.format(table))
    except NotFound:
        schema = [
            bigquery.SchemaField('user_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('track_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('acousticness', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('danceability', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('duration_ms', 'FLOAT', mode='REQUIRED'), 
            bigquery.SchemaField('energy', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('instrumentalness', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('liveness', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('loudness', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('speechiness', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('tempo', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('valence', 'FLOAT', mode='REQUIRED')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))
    return table

# function to export data into table in bigquery
def export_items_to_bigquery(client, dataset, table, data):    

    # prepares a reference to the dataset
    dataset_ref = bigquery_client.dataset(dataset)

    table_ref = dataset_ref.table(table)
    table = bigquery_client.get_table(table_ref) 

    errors = client.insert_rows_json(table, json.loads(data))
    if errors == []:
        print('New rows have been added')
    else:
        print(f'Errors during insertion of rows: {errors}')

    assert errors == []

# starting Flask application
SESSION_TYPE = 'filesystem'
app = Flask(__name__)
app.config['SESSION_TYPE'] = SESSION_TYPE
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.secret_key = 'super_secret_key'

# defining global variables - spotify dev API
BASE_URL = 'https://accounts.spotify.com'
CLIENT_ID = 'a4c66247a69247e1b3a8ae4665f94753'
CLIENT_SECRET = # :P
CLIENT_SIDE_URL = 'http://127.0.0.1'
PORT = 5000
REDIRECT_URI = f'{CLIENT_SIDE_URL}:{PORT}/callback'
AUTH_URL = f'{BASE_URL}/authorize'
TOKEN_URL = f'{BASE_URL}/api/token'
RESPONSE_TYPE = 'code'
STATE = ''.join(random.choices(string.ascii_lowercase, k=3))
SCOPE = ' '.join(['user-read-playback-state', 'playlist-read-private', 'user-follow-read', 'user-top-read', 'user-read-recently-played', 'user-library-read'])

# endpoint urls
PROFILE_URL = 'https://api.spotify.com/v1/me'
TOP_TRACKS_URL = f'{PROFILE_URL}/top/tracks'
FOLLOWING_URL = f'{PROFILE_URL}/following'
SAVED_TRACKS_URL = f'{PROFILE_URL}/tracks'
USER_PLAYLIST_URL_BASE = 'https://api.spotify.com/v1/users'
AUDIO_FEATURES_URL_BASE = f'https://api.spotify.com/v1/audio-features'

# creating ldatabases in cloud, getting auth code
@app.route('/', methods=['GET', 'POST'])
def auth():
    '''
    Get the authorization code using the client details
    '''

    # defining params
    payload={
        'client_id': CLIENT_ID, 
        'response_type': RESPONSE_TYPE, 
        'redirect_uri': REDIRECT_URI,  
        'scope': SCOPE,
        'state': STATE
    }

    return redirect(f"{AUTH_URL}/?{urlencode(payload)}")

@app.route('/callback', methods=['GET', 'POST'])
def callback():
    '''
    Exchange the authorization code for the authorization and refresh tokens
    '''

    GRANT_TYPE = 'authorization_code'
    AUTH_CODE = request.args.get('code')

    payload={
        'grant_type': GRANT_TYPE, 
        'code': AUTH_CODE,
        'redirect_uri': REDIRECT_URI,
        'client_id': CLIENT_ID, 
        'client_secret': CLIENT_SECRET
    }

    token_request = requests.post(url=TOKEN_URL, data=payload).json()

    ACCESS_TOKEN = token_request.get('access_token')
    REFRESH_TOKEN = token_request.get('refresh_token')

    session['tokens'] = {
        'access_token': ACCESS_TOKEN,
        'refresh_token': REFRESH_TOKEN
    }

    return redirect(url_for('profile'))

@app.route('/refresh', methods=['GET', 'POST'])
def refresh():
    '''
    Request a new access token when authorization token expires - the refresh token defined earlier is used as a parameter in the request
    '''

    GRANT_TYPE = 'refresh_token'
    REFRESH_TOKEN = session.get('tokens').get('refresh_token')

    payload = {
        'grant_type': GRANT_TYPE,
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    refresh_request = requests.post(url=TOKEN_URL, data=payload, headers=headers).json()
    session['tokens']['access_token'] = refresh_request.get('access_token')
    return json.dumps(session['tokens'])

@app.route('/profile')
def profile():
    '''
    Get data of user and upload the data to our GCP instance
    '''

    # check for tokens
    if 'tokens' not in session:
        app.logger.error('No tokens in session.')
        abort(400)

    # get profile info
    headers = {'Authorization': f"Bearer {session['tokens'].get('access_token')}"}

    profile_res = requests.get(PROFILE_URL, headers=headers)
    profile_res_data = profile_res.json()

    if profile_res.status_code != 200:
        app.logger.error(
            f"Failed to get profile info: {profile_res_data.get('error', 'No error message returned.')}"
        )
        abort(profile_res.status_code)

    # creating cloud database, table
    dataset = "spotify_project_dataset"
    table_name = "audio_features"
    data = bq_create_dataset(bigquery_client, dataset)
    table = bq_create_table(bigquery_client, dataset, table_name)

    sql_query = '''
    SELECT DISTINCT user_id FROM `spotify_project_dataset.audio_features`
    '''
    query_job = bigquery_client.query(sql_query)

    # only add tracks to database if the user is not already present there
    if profile_res_data['id'] not in [i[0] for i in list(query_job)]:

        # get top tracks
        top_tracks_res = requests.get(TOP_TRACKS_URL, headers=headers, params={
            'limit': 50
        })
        top_tracks_res_data = top_tracks_res.json()

        if top_tracks_res.status_code != 200:
            app.logger.error(
                f"Failed to get top tracks info: {top_tracks_res_data.get('error', 'No error message returned.')}"
            )
            abort(top_tracks_res.status_code)  

        filt_top_tracks_res_data = []
        for i in range(len(top_tracks_res_data['items'])):
            filt_top_tracks_res_data.append({
                'id': top_tracks_res_data['items'][i]['id'],
                'name': top_tracks_res_data['items'][i]['name']
            })

        # get user saved tracks
        saved_tracks_res_data = []
        for i in range(5):
            saved_tracks_res = requests.get(SAVED_TRACKS_URL, headers=headers, params={
                'limit': 50, 
                'offset': 50*i
            })
            saved_tracks_res_data += saved_tracks_res.json()['items']

            if saved_tracks_res.status_code != 200:
                app.logger.error(
                    f"Failed to get saved tracks info: {saved_tracks_res_data[i].get('error', 'No error message returned.')}"
                )
                abort(saved_tracks_res.status_code)  

        filt_saved_tracks_res_data = []
        for i in range(len(saved_tracks_res_data)):
            filt_saved_tracks_res_data.append({
                'id': saved_tracks_res_data[i]['track']['id'],
                'name': saved_tracks_res_data[i]['track']['name']
            })
        
        total_filt_tracks_data = filt_top_tracks_res_data + filt_saved_tracks_res_data

        # getting audio analysis for all 300 tracks
        tot_audio_features_data = []
        for track in total_filt_tracks_data:
            audio_features_res = requests.get(f'{AUDIO_FEATURES_URL_BASE}/{track["id"]}', headers=headers)
            # sleeping to prevent getting rate limited
            time.sleep(1)
            audio_features_res_data = audio_features_res.json()
            audio_features_res_data['key_escaped'] = audio_features_res_data.pop('key')
            audio_features_res_data['track_name'] = track['name']
            audio_features_res_data['user_id'] = profile_res_data['id']
            [audio_features_res_data.pop(key) for key in ['id', 'analysis_url', 'uri', 'track_href', 'mode', 'key_escaped', 'time_signature', 'type']]
            tot_audio_features_data.append(audio_features_res_data)

        # writing the file to path 
        with open(f"audio_features.json", "w+") as f:
            json.dump(tot_audio_features_data, f)

        # preprocessing the data in pandas
        df = pd.read_json(f"audio_features.json")
        col_names = list(df.columns)
        col_names.remove('user_id')
        col_names.remove('track_name')
        scaled_features = df.copy()
        features = scaled_features[col_names]
        scaler = StandardScaler().fit(features.values)
        features = scaler.transform(features.values)
        scaled_features[col_names] = features
        tot_audio_features_data = scaled_features.to_json(orient='records')

        # pushing items to bigquery table
        export_items_to_bigquery(bigquery_client, dataset, table_name, tot_audio_features_data)

    # performing k-means clustering for genres
    # for i in range(5):
    sql_query = f'''
    CREATE MODEL `spotify_project_dataset.genre_clustering`
    OPTIONS (
        MODEL_TYPE='KMEANS', 
        NUM_CLUSTERS=4
    )
    AS
    SELECT 
    acousticness, danceability, duration_ms, energy, instrumentalness, liveness, loudness, speechiness, tempo, valence 
    FROM `spotify_project_dataset.audio_features`
    '''
    bigquery_client.query(sql_query)
    return render_template('profile_info.html')

# running the application
if __name__== "__main__":
    # creating client
    bigquery_client = bigquery.Client()
    app.run(debug=True)