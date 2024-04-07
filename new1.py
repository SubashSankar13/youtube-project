import mysql.connector
import os
import googleapiclient.discovery
import googleapiclient.errors
import json
import pandas as pd
import streamlit as st
import requests_cache


def Api():
    api = "AIzaSyDvqkn9RUVJxaX4n6yMfQU-16555i-HmRM"
    api_service_name = "youtube"
    api_version = "v3"


    requests_cache.install_cache('youtube_api_cache', backend='sqlite', expire_after=3600)  

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api)
    return youtube

youtube = Api()



mydb=mysql.connector.connect(
 host='localhost',
 user='root',
 password='root',
 database='youtube3')

cursor = mydb.cursor()


def channel_info(id):

    cursor.execute("""CREATE TABLE IF NOT EXISTS channel_info (
                        channel_Name VARCHAR(255),
                        channel_Id VARCHAR(255),
                        subscribe INT,
                        views INT,
                        Total_videos INT,
                        channel_description TEXT,
                        Playlist_id VARCHAR(255)
                    )""")
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=id )
    response = request.execute()


    for i in response.get('items', []):
        data = dict(channel_Name=i['snippet']['title'],
                   channel_Id=i['id'],
                   subscribe=i['statistics']['subscriberCount'],
                   views=i['statistics']['viewCount'],
                   Total_videos=i['statistics']['videoCount'],
                   channel_description=i['snippet']['description'],
                   Playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
        cursor.execute("INSERT INTO channel_info (channel_Name, channel_Id, subscribe, views, Total_videos, channel_description, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (data['channel_Name'], data['channel_Id'], data['subscribe'], data['views'], data['Total_videos'], data['channel_description'], data['Playlist_id']))
        mydb.commit()

    return data



def get_Videos_Ids(channel_id):
    Video_Ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None
    
    while True:
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            Video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
            
    cursor.execute("""CREATE TABLE IF NOT EXISTS video_ids (
                        video_id VARCHAR(255)
                    )""")
    
    # Insert data into MySQL
    for video_id in Video_Ids:
        cursor.execute("INSERT INTO video_ids (video_id) VALUES (%s)", (video_id,))
    mydb.commit()
    return Video_Ids 


#get video information
def get_video_info(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
        )
        response=request.execute()
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS videos (
                    channel_Name VARCHAR(255),
                    channel_Id VARCHAR(255),
                    video_Id VARCHAR(255),
                    Title VARCHAR(255),
                    Tags TEXT,
                    Thumbnail TEXT,
                    Description TEXT,
                    Published_date VARCHAR(255),
                    Duration VARCHAR(255),
                    views BIGINT,
                    view INT,
                    likes INT,
                    comments INT,
                    Favourite_count INT,
                    Definition VARCHAR(255),
                    Caption_Status VARCHAR(255)
                )""")

       
        for item in response["items"]:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                     channel_Id=item['snippet']['channelId'],
                     video_Id=item['id'],
                     Title=item['snippet']['title'],
                     Tags=json.dumps(item.get('tags')),
                     Thumbnail=item['snippet']['thumbnails']['default']['url'],
                     Description=item['snippet'].get('description'),
                     Published_date=item['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                     Duration=item['contentDetails']['duration'],
                     views=item['statistics'].get('viewCount'),
                     likes=item['statistics'].get('likeCount'),
                     comments=item['statistics'].get('commentCount'),
                     Favourite_count=item['statistics'].get('favoriteCount'),
                     Definition=item['contentDetails'].get('definition'),
                     Caption_Status=item['contentDetails'].get('caption')
                     )
            video_data.append(data)

            # Insert data into MySQL
            cursor.execute("INSERT INTO videos (channel_Name, channel_Id, video_Id, Title, Tags, Thumbnail, Description, Published_date, Duration, views, likes, comments, Favourite_count, Definition,Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (data['channel_Name'], data['channel_Id'], data['video_Id'], data['Title'], data['Tags'], data['Thumbnail'], data['Description'], data['Published_date'], data['Duration'], data['views'], data['likes'], data['comments'], data['Favourite_count'], data['Definition'], data['Caption_Status']))

            mydb.commit()

    return video_data



#get comment

def get_comment_info(video_ids):
    comment_data=[]
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS comment_info (
                            comment_Id VARCHAR(255),
                            video_Id VARCHAR(255),
                            comment_Text TEXT,
                            comment_Author VARCHAR(255),
                            comment_Published VARCHAR(255)
                        )""")
        
        for video_id in video_ids:
            request=youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                         video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''))

                comment_data.append(data)

                # Insert data into MySQL
                cursor.execute("INSERT INTO comment_info (comment_Id, video_Id, comment_Text, comment_Author, comment_Published) VALUES (%s, %s, %s, %s, %s)",
                               (data['comment_Id'], data['video_Id'], data['comment_Text'], data['comment_Author'], data['comment_Published']))
                mydb.commit()
    except Exception as e:
        print(f"Error: {e}")
    
    return comment_data


#get playlist 

def get_playlist_details(channel_id):
    All_data=[]
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS playlist_details(
                            Playlist_Id VARCHAR(255),
                            channel_Name VARCHAR(255),
                            channel_Id VARCHAR(255),
                            video_Id VARCHAR(255),
                            Title VARCHAR(255),
                            Published_data VARCHAR(255),
                            video_count INT
                        )""")
        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50
        )
        response=request.execute()

        for item in response['items']:
            Playlist_Id=item['id']
            Title=item['snippet']['title']
            Published_data=item['snippet']['publishedAt'].replace('T', ' ').replace('Z', '')
            video_count=item['contentDetails']['itemCount']

            # Fetch video IDs in the playlist
            video_ids = []
            next_page_token = None
            while True:
                playlist_items_request = youtube.playlistItems().list(
                    part="snippet",
                    playlistId=Playlist_Id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                playlist_items_response = playlist_items_request.execute()

                for playlist_item in playlist_items_response["items"]:
                    video_ids.append(playlist_item["snippet"]["resourceId"]["videoId"])

                next_page_token = playlist_items_response.get("nextPageToken")
                if not next_page_token:
                    break

            for video_id in video_ids:
                data=dict(Playlist_Id=Playlist_Id,
                          channel_Name=item['snippet']['channelTitle'],
                          channel_Id=channel_id,
                          video_Id=video_id,
                          Title=Title,
                          Published_data=Published_data,
                          video_count=video_count)
                All_data.append(data)

                # Insert data into MySQL
                cursor.execute("INSERT INTO playlist_details (Playlist_Id, channel_Name, channel_Id, video_Id, Title, Published_data, video_count) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                               (data['Playlist_Id'], data['channel_Name'], data['channel_Id'], data['video_Id'], data['Title'], data['Published_data'], data['video_count']))
                mydb.commit()
    except Exception as e:
        print(f"Error: {e}")
    
    return All_data

def video(Video_Ids):
    video_get=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
        )
        response=request.execute()
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS videos (
                    channel_Name VARCHAR(255),
                    channel_Id VARCHAR(255),
                    video_Id VARCHAR(255),
                    Title VARCHAR(255),
                    Tags TEXT,
                    Thumbnail TEXT,
                    Description TEXT,
                    Published_date VARCHAR(255),
                    Duration VARCHAR(255),
                    views BIGINT,
                    view INT,
                    likes INT,
                    comments INT,
                    Favourite_count INT,
                    Definition VARCHAR(255),
                    Caption_Status VARCHAR(255)
                )""")

       
        for item in response["items"]:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                     channel_Id=item['snippet']['channelId'],
                     video_Id=item['id'],
                     Title=item['snippet']['title'],
                     Tags=json.dumps(item.get('tags')),
                     Thumbnail=item['snippet']['thumbnails']['default']['url'],
                     Description=item['snippet'].get('description'),
                     Published_date=item['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                     Duration=item['contentDetails']['duration'],
                     views=item['statistics'].get('viewCount'),                     
                     likes=item['statistics'].get('likeCount'),
                     comments=item['statistics'].get('commentCount'),
                     Favourite_count=item['statistics'].get('favoriteCount'),
                     Definition=item['contentDetails'].get('definition'),
                     Caption_Status=item['contentDetails'].get('caption')
                     )
            video_get.append(data)

            # Insert data into MySQL
            cursor.execute("INSERT INTO videos (channel_Name, channel_Id, video_Id, Title, Tags, Thumbnail, Description, Published_date, Duration, views, likes, comments, Favourite_count, Definition,Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
               (data['channel_Name'], data['channel_Id'], data['video_Id'], data['Title'], data['Tags'], data['Thumbnail'], data['Description'], data['Published_date'], data['Duration'], data['views'], data['likes'], data['comments'], data['Favourite_count'], data['Definition'], data['Caption_Status']))

            mydb.commit()


    return video_get

def get_channel_details(channel_id):
    channel_details = channel_info(channel_id)
    Video_Ids = get_Videos_Ids(channel_id)
    video_details = get_video_info(Video_Ids)
    comment_details = get_comment_info(Video_Ids)
    playlist_details = get_playlist_details(channel_id)
    video_details = video(Video_Ids)
    
    # Convert dictionaries to DataFrames
    channel_df = pd.DataFrame([channel_details])
    video_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)
    playlist_df = pd.DataFrame(playlist_details)
    video_detail_df = pd.DataFrame(video_details)
    
    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "comment_details": comment_df,
        "playlist_details": playlist_df,
        "video_data": video_detail_df
    }
    
def questions_page():
    cursor.execute("SELECT Title, channel_Name, views FROM videos ORDER BY views DESC LIMIT 10")
    result = cursor.fetchall()
    if len(result) > 0:
        df = pd.DataFrame(result, columns=['Title', 'Channel Name', 'Views'])
        st.dataframe(df)
    else:
        st.write("No videos found.")

def main():
    st.sidebar.title("Navigation")
    option = st.sidebar.radio("Select an option", ["Home", "Channel Details", "Go to Question"])

    if option == "Home":
        st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    elif option == "Channel Details":
        st.title('YouTube Channel Details')
        channel_id = st.text_input('Enter YouTube Channel ID:')
        if st.button('Get Channel Details'):
            details =get_channel_details(channel_id)
            st.subheader('Channel Details')
            st.write(details["channel_details"])

            st.subheader('Video Details')
            st.write(details["video_details"])

            st.subheader('Comment Details')
            st.write(details["comment_details"])

            st.subheader('Playlist Details')
            st.write(details["playlist_details"])
    elif option == "Go to Question":
        st.session_state.page = 'questions_page'
        
def questions_page():
    questions = [
        "Names of all the videos and their corresponding channels",
        "Channels with the most number of videos and how many videos they have",
        "Top 10 most viewed videos and their respective channels",
        "Number of comments for each video and their corresponding video names",
        "Videos with the highest number of likes and their corresponding channel names",
        "Total number of likes for each video and their corresponding video names",
        "Total number of views for each channel and their corresponding channel names",
        "Names of all the channels that have published videos in the year 2022",
        "Average duration of all videos in each channel and their corresponding channel names",
        "Videos with the highest number of comments and their corresponding channel names"
    ]

    selected_question = st.selectbox("Select a question", questions, key="selectbox_unique_key")

    # Fetch data based on the selected question
    if st.button('Submit'):
        if selected_question == questions[0]:
            cursor.execute("SELECT  Title, channel_name FROM videos")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['video Title','Channel_Name'])
            st.write(df)

        elif selected_question == questions[1]:
            cursor.execute("SELECT channel_name, COUNT(*) as video_count FROM videos GROUP BY channel_name ORDER BY video_count DESC")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel_Name', 'No of videos'])
            st.write(df)

        elif selected_question == questions[2]:
            cursor.execute("SELECT Title, channel_name, views FROM videos ORDER BY views DESC LIMIT 10")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video_Title', 'Channel name','View Count'])
            st.write(df)
           
        elif selected_question == questions[3]:
            cursor.execute("SELECT Title, COUNT(*) as comments FROM videos GROUP BY Title")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'No of comments'])
            st.write(df)

        elif selected_question == questions[4]:
            cursor.execute("SELECT MAX(likes) as max_likes FROM videos")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Highest Likes'])
            st.write(df)           

        elif selected_question == questions[5]:
            cursor.execute("SELECT Title, SUM(likes) as total_likes FROM videos GROUP BY Title")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title','Like Count'])
            st.write(df)       

        elif selected_question == questions[6]:
            cursor.execute("SELECT channel_name, SUM(views) as total_views FROM videos GROUP BY channel_name")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name', 'No of views'])
            st.write(df)           

        elif selected_question == questions[7]:
            cursor.execute("SELECT DISTINCT channel_name FROM videos WHERE YEAR(published_date) = 2022")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name'])
            st.write(df)

            # Count the number of unique channels
            num_channels = len(df)          

        elif selected_question == questions[8]:
            cursor.execute("""SELECT channel_name, AVG(duration_minutes) AS avg_duration 
                        FROM (
                                SELECT channel_name, TIME_TO_SEC(SUBSTRING(duration, 3)) / 60 AS duration_minutes 
                                FROM videos
                            ) AS durations 
                            GROUP BY channel_name """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name', 'Average Sec'])
            st.write(df)
            
                    
        elif selected_question == questions[9]:
            cursor.execute("""SELECT Title, channel_name, COUNT(*) as comment_count 
                FROM videos 
                GROUP BY Title, channel_name 
                ORDER BY comment_count DESC 
                LIMIT 1
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Title', 'Channel Name','No of Counts'])
            st.write(df)

        data = cursor.fetchall()
        df = pd.DataFrame()
        st.write()
    
    
    if st.button('Go to Home Page'):
        st.session_state.page = 'main_page'


if __name__ == '__main__':
    if 'page' not in st.session_state:
        st.session_state.page = 'main_page'

    if st.session_state.page == 'main_page':
        main()
    elif st.session_state.page == 'questions_page':
        questions_page()

    mydb.close()
