import pandas as pd
import CONFIG as cg
from datetime import datetime
from datetime import date
from googleapiclient.discovery import build
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.models.variable import Variable

def youtubeFunc():
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(
        api_service_name, api_version, developerKey=cg.YTAPIKEY)

    channelIDs = ['UCBNnILlexKYtJu-EGUvq_iA','UCUNSNLJINivbSu-Yg9hk13w','UChRlaISXbl2gECFAmDzxgzg','UCdqp0KK_Io7TwK5cJMBvB0Q']
    
    ytList = []
    for channel in channelIDs:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel
        )
        response = request.execute()

        for item in response['items']:
            playlist = item['contentDetails']['relatedPlaylists']['uploads']

            request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist,
            maxResults=50,
                )
            response = request.execute()

            for item in response['items']:
                ytList.append(item['contentDetails']['videoId'])

            nextPageToken = response.get('nextPageToken')

            while nextPageToken is not None:
                request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist,
                maxResults=50,
                pageToken = nextPageToken
                    )
                response = request.execute()

                for item in response['items']:
                    ytList.append(item['contentDetails']['videoId'])

                nextPageToken = response.get('nextPageToken')

    dfRows = []
    for video in ytList:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video
            )
        response = request.execute()

        for item in response['items']:
            channel = item['snippet']['channelTitle']
            title = item['snippet']['title']
            uploadedStr = item['snippet']['publishedAt'].split('T')[0]
            uploadedDate = datetime.strptime(uploadedStr, '%Y-%m-%d')
            views = item['statistics']['viewCount']
            likes = item['statistics']['likeCount']
            comments = item['statistics']['commentCount']

            row = {'ChannelName': channel, 'VideoTitle': title, 'DateUploaded': uploadedStr, 'ViewCnt': views, 'LikeCnt': likes, 'CommentCnt': comments}
            if uploadedDate >= date.today():
                dfRows.append(row)

    masterDF = pd.DataFrame(dfRows)

    rows = []
    colNames = ['ChannelName', 'VideoTitle', 'DateUploaded', 'ViewCnt', 'LikeCnt', 'CommentCnt']
    for i, row in masterDF.iterrows():
        channel = row[0]
        title = str(row[1]).replace('(', '').replace(')', '').replace("'", '')
        dateuploaded = row[2]
        views = row[3]
        likes = row[4]
        comments = row[5]
        row = (channel, title, dateuploaded, views, likes, comments)
        row = str(row)
        rows.append(row)
    rows = ','.join(rows)
    
    Variable.set('sqlQUERY', f""" 
                INSERT INTO YoutubeCreators (ChannelName, VideoTitle, DateUploaded, ViewCnt, LikeCnt, CommentCnt)
                VALUES {rows};
                """)
            
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10)
}

with DAG(dag_id='youtubeCreator'
         ,default_args=args
         ,catchup=False
         ,schedule='45 18 * * *'
         ) as dag:
    youtube = PythonOperator(
        task_id='youtube'
        ,python_callable=youtubeFunc
        ,dag=dag
    )

    toSQLServer = MsSqlOperator(
                            task_id='populateYoutubeCreators',
                            mssql_conn_id='airflow_mssql',
                            sql= Variable.get('sqlQUERY')
                    )

    
youtube >> toSQLServer
    


