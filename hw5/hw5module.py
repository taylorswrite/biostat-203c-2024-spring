import time # For Sleep
import random # For Random Generator
import numpy as np # For Arrays
import pandas as pd # For DataFrames
import requests # For requesting website html
from bs4 import BeautifulSoup # For html parser

## List of User Agents from a github post. Link in Notebook.
user_agents_github = """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36
Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; Touch; LCJB; rv:11.0) like Gecko
Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36
Mozilla/5.0 (X11; CrOS x86_64 6812.88.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.153 Safari/537.36
Mozilla/5.0 (X11; Linux i686; rv:38.0) Gecko/20100101 Firefox/38.0
Mozilla/5.0 (Windows NT 6.3; Win64; x64; Trident/7.0; Touch; ASU2JS; rv:11.0) like Gecko
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.65 Safari/537.36
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.154 Safari/537.36
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.13 Safari/537.36
Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36
Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/537.16 (KHTML, like Gecko) Version/8.0 Safari/537.16
Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0
Mozilla/5.0 (Linux; Android 5.0; SAMSUNG SM-N900V 4G Build/LRX21V) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/2.1 Chrome/34.0.1847.76 Mobile Safari/537.36
Mozilla/5.0 (Linux; Android 4.4.3; KFTHWI Build/KTU84M) AppleWebKit/537.36 (KHTML, like Gecko) Silk/44.1.81 like Chrome/44.0.2403.128 Safari/537.36
Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; CMDTDF; .NET4.0C; .NET4.0E; GWX:QUALIFIED)
Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) CriOS/45.0.2454.68 Mobile/11D257 Safari/9537.53
Mozilla/5.0 (Windows NT 6.3; WOW64; rv:37.0) Gecko/20100101 Firefox/37.0
Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.6.1000 Chrome/30.0.1599.101 Safari/537.36
Mozilla/5.0 (Linux; Android 4.4.2; GT-P5210 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.84 Safari/537.36
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.99 Safari/537.36
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36
Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0
Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; MDDSJS; rv:11.0) like Gecko
Mozilla/5.0 (Linux; Android 4.4.2; QTAQZ3 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.84 Safari/537.36
Mozilla/5.0 (Linux; Android 4.4.2; QMV7B Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.84 Safari/537.36
Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36
Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; MATBJS; rv:11.0) like Gecko
Mozilla/5.0 (iPad; CPU OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) GSA/6.0.51363 Mobile/12H321 Safari/600.1.4
Mozilla/5.0 (iPad; CPU OS 8_1_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B436 Safari/600.1.4
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36
Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/530.19.2 (KHTML, like Gecko) Version/4.0.2 Safari/530.19.1
Mozilla/5.0 (iPhone; CPU iPhone OS 8_4_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12H321
Mozilla/5.0 (Linux; U; Android 4.0.3; en-ca; KFTT Build/IML74K) AppleWebKit/537.36 (KHTML, like Gecko) Silk/3.68 like Chrome/39.0.2171.93 Safari/537.36
Mozilla/5.0 (Windows NT 5.1; rv:30.0) Gecko/20100101 Firefox/30.0"""
# Convert the long copied string to a list by splitting by new lines.
user_agents = user_agents_github.split('\n')

def parse_actor_page(df, actor_directory, user_agents=user_agents):
    """
    Use the actors TMDB profile link to list movies that actor was a castmember of 
    and append the actor's name and movie name to df.
    ---
    Args:
        df: (pd.DataFrame) Actor and Movie DataFrame
        actor_directory: (String) Actor's profile TMDB link
    Return:
        df: (pd.DataFrame) df with appended actor movie combinations
    """
    # Requesting TMDB movie site
    headers = {'User-Agent': random.choice(user_agents)} # Random User Agent
    s = requests.session() # Initiate new session
    response = s.get(actor_directory, headers=headers) # Go to link as Random User
    if response.status_code == 403: # check for failed connection
        raise Exception("403 Error. Check link, internet, and bot status.")
    data = response.text # raw html from TMDB
    s.cookies.clear() # clear cookies
    s.close() # close session/browser
    
    # Webscraping
    soup = BeautifulSoup(data, 'html.parser') # parse html from TMDB
    actor = soup.select('.title a')[0].get_text() # extract actor's name
    if soup.select('.item_adult_true'): # check if movie name is blurred
        print(f"{actor} Does Adult Films! Not Included.")
        return df # return df to skip adding actor to df
    movies = soup.select('.tooltip bdi') # returns a list of movies
    
    # Append the actor's name and movie name to df
    for movie in movies:
        movie_iter = pd.DataFrame({ # convert dict to pd.DataFrame
            'actor': [actor], # actor's name
            'movie_or_TV_name': [movie.get_text()] # movie name
        }) 
        df = pd.concat([df, movie_iter], ignore_index=True) # append row to df
    return df # return df with appended actor-movie/tv rows
    
def parse_full_credits(movie_directory, user_agents=user_agents):
    """
    Create a list of cast members from a user provided movie cast TMDB link and
    and for each cast member call parse_actor_page to append cast member movie
    appearances.
    ---
    Args:
        movie_directory: (String)  Movie  TMDB link.
    Return:
        df: (pd.DataFrame) Actor and Movie combinations DataFrame
    """
    # Check provided TMDB movie link
    if not movie_directory.startswith("https://www.themoviedb.org/movie/"): # if not a TMDB movie link
        raise Exception("Not a TMDB movie. Enter a new cast link.") 
    if not movie_directory.endswith("/cast"): # check if a movie link
        movie_directory = f"{movie_directory}/cast" # make it a movie cast link
    
    # Requesting TMDB movie site
    headers = {'User-Agent': random.choice(user_agents)} # select a user agent
    s = requests.session() # start new session/browser as user agent
    response = s.get(movie_directory, headers=headers) # get raw html
    if response.status_code == 403: # If connection failed
        raise Exception("403 Error. Check link, internet, and bot status.")
    data = response.text # Raw html from TMDB
    s.cookies.clear() # clear cookies
    s.close() # close session/browser
    
    # Webscraping
    soup = BeautifulSoup(data, 'html.parser') # parse html from TMDB
    cast_crud = soup.select(".pad:nth-child(1) a")
    if soup.select('h2')[0].text == "Oops! We can't find the page you're looking for":
        raise Exception("Bad TMDB movie Link") # check for Empty TMDB page
    cast_link = np.unique([f"https://www.themoviedb.org{link['href']}?credit_department=Acting" 
                           for link in cast_crud]) # list of actor links
    
    # Loop through list of actor links an supply to parse_actor_page()
    df = pd.DataFrame(columns=['actor', 'movie_or_TV_name']) # init dict
    for link in cast_link: # loop actor links
        time.sleep(random.randint(1, 3)) # Rand. Delay between requests
        df = parse_actor_page(df, link, user_agents) # call parse_actor_page
    
    # Sort and remove duplicates. Occurs when an actor has mulitple role in a movie
    df = df.sort_values(by=["actor","movie_or_TV_name"]).drop_duplicates()
    return df