# /usr/bin/python3


import csv, glob, json, os
import traceback, time, random
import pandas as pd
import requests
import arrow
import time

from pprint             import pprint
from functools          import reduce
from itertools          import cycle
from urllib.request     import urlopen, Request
from concurrent.futures import ThreadPoolExecutor
from IPython            import embed

# Scraping libraries
from lxml import etree
from bs4 import BeautifulSoup
# Webdriver for JS scraping
from selenium import webdriver
from selenium.webdriver.common.keys import Keys




def extract_name(link):
    link = link.replace('?feature=fvstc', '')
    link = link.replace('?feature=watch', '')
    if link.endswith('/'):
        link = link[:-1]
    if link.endswith("/feed"):
        link = link.replace('/feed', '')
    if link.endswith("/about"):
        link = link.replace('/about', '')
    if link.endswith("/videos"):
        link = link.replace('/videos', '')
    if link.endswith("/featured"):
        link = link.replace('/featured', '')
    return link.split('/')[-1]


def format_number(views):
    if (type(views) == int) or (type(views) == float):
        return int(views)
    else:
        views = views.replace(',', '')
        if views.endswith('K'):
            views = views.replace('K', '')
            views = float(views) * 1000
        elif views.endswith('M'):
            views = views.replace('M', '')
            views = float(views) * 1000000
        elif views.endswith('B'):
            views = views.replace('B', '')
            views = float(views) * 1000000000
        else:
            views = float(views)
        return int(views)



def get_youtube_info(soup):
    top_info_id = {
        "Uploads": 'youtube-stats-header-uploads',
        "Subscribers": 'youtube-stats-header-subs',
        "Video Views": 'youtube-stats-header-views',
        "Country": 'youtube-stats-header-country',
        "Channel Type": 'youtube-stats-header-channeltype',
        "User Created": '',
    }
    top_info = {
        "Uploads": '',
        "Subscribers": '',
        "Video Views": '',
        "Country": '',
        "Channel Type": '',
        "User Created": '',
    }
    try:
        for info in soup.find(id='YouTubeUserTopInfoWrap').find_all('div', class_='YouTubeUserTopInfo'):
            try:
                key = info.find('span', class_='YouTubeUserTopLight').get_text()
                if key=="User Created":
                    top_info[key] = info.find_all('span')[-1].get_text()
                elif key in ['Uploads', 'Subscribers', 'Video Views']:
                    top_info[key] = int(info.find('span', id=top_info_id[key]).get_text())
                else:
                    top_info[key] = info.find('span', id=top_info_id[key]).get_text()
            except:
                print("\nErr:\t", info.get_text())
                continue

        top_info['Youtube Link'] = soup.find('a', class_='core-button -margin core-small-wide ui-black').attrs['href']
        return top_info
    except:
        print('Error scraping Youtube Profile on socialblade. Check youtube username/userID')
        return top_info



def get_yt_table_data(soup, num_rows=10):

    table_labels = [
        'Date',
        'Video Title',
        'Views',
        'Ratings',
        'Percentage Liked',
        'Comments',
        'Estimated Earnings',
    ]
    yt_table_data = []
    try:
        rows = soup.find_all('div', class_='RowRecentTable')
        for row in rows[:num_rows]:
            cell_data = [cell.get_text() for cell in row.find_all('div', class_='TableMonthlyStats')]
            dict_data = dict(zip(table_labels, cell_data))
            dict_data['Comments'] = format_number(dict_data['Comments'])
            dict_data['Ratings'] = format_number(dict_data['Ratings'])
            dict_data['Views'] = format_number(dict_data['Views'])
            yt_table_data.append(dict_data)
        return yt_table_data
    except:
        yt_table_data = {
            'Date': '',
            'Video Title': '',
            'Views': '',
            'Ratings': '',
            'Percentage Liked': '',
            'Comments': '',
            'Estimated Earnings': '',
        }
        print("Error scraping Youtube Profile Videos page on socialblade.")
        return yt_table_data


def get_twitter_info(soup):
    top_info = {
        "Followers": '',
        "Following": '',
        "Likes": '',
        "Tweets": '',
        "User Created": '',
        "twitter_link": '',
    }
    try:
        for info in soup.find_all('div', class_='YouTubeUserTopInfo'):
            try:
                key = info.find('span', class_='YouTubeUserTopLight').get_text()
                if key=='User Created':
                    top_info[key] = info.find_all('span')[-1].get_text()
                else:
                    top_info[key] = format_number(info.find_all('span')[-1].get_text())
            except:
                print("\nErr:\t", info.get_text())
                continue

        ## scrape twitter link
        links = [a for a in soup.find_all('a') if 'href' in a.attrs.keys()]
        links = [a for a in links if 'twitter.com' in a.attrs['href']]
        links = [a for a in links if 'socialblade' not in a.attrs['href']]
        links = [a for a in links if 'tweet?' not in a.attrs['href']]
        top_info['twitter_link'] = links[0].attrs['href']
        return top_info
    except:
        print('Missing Twitter Profile on socialblade')
        return top_info


def get_instagram_info(soup):
    try:
        labels = [s.get_text() for s in soup.find_all('div', class_='stats-top-data-header')]
        values = [format_number(s.get_text())
                if s.get_text()[0].isnumeric()
                else s.get_text().strip()
                for s in soup.find_all('div', class_='stats-top-data-content') ]

        instagram_data = dict(zip(labels, values))

        ## scrape instagram link
        links = [a for a in soup.find_all('a') if 'href' in a.attrs.keys()]
        links = [a for a in links if 'instagram.com' in a.attrs['href']]
        links = [a for a in links if 'https://instagram.com/socialblade' not in a.attrs['href']]
        instagram_data['instagram_link'] = links[0].attrs['href']
        if "Followers" not in instagram_data.keys():
            instagram_data['Followers'] = ''
        return instagram_data
    except:
        print('Missing Instagram Profile on socialblade')
        return { 'Followers': '', 'instagram_link': ''}




def format_scraped_data(youtube_info, yt_table_data, twitter_info, instagram_info):
    ## Post frequency
    td_videos = arrow.get(yt_table_data[0]['Date']) - arrow.get(yt_table_data[-1]['Date']) # time delta between first and last video
    post_frequency = len(yt_table_data) / td_videos.days * 30.5 # average posts per month (30.5 days average)
    ## Date of last post
    last_post_date = yt_table_data[0]['Date']
    ## Views of last post
    last_post_views = yt_table_data[0]['Views']
    ## Range of Views
    range_of_views = "{min} ~ {max}".format(
            min=min(t['Views'] for t in yt_table_data),
            max=max(t['Views'] for t in yt_table_data)
        )
    ## Range of number of comments
    range_of_comments = "{min} ~ {max}".format(
            min=min(t['Comments'] for t in yt_table_data),
            max=max(t['Comments'] for t in yt_table_data)
        )
    ## Total number of videos
    total_num_videos = youtube_info['Uploads']
    ## Total Video Views
    total_video_views = youtube_info['Video Views']

    influencer_data = {
        'location': youtube_info['Country'],
        'youtube_category': youtube_info['Channel Type'],
        'date_joined_platform': youtube_info['User Created'],
        'post_frequency': post_frequency,
        'date_of_last_post': last_post_date,
        'views_of_last_video': last_post_views,
        'range_of_views': range_of_views,
        'range_of_comments_on_videos': range_of_comments,
        'total_number_of_videos': youtube_info['Uploads'],
        'total_video_views': total_video_views,
        'youtube_subscribers': youtube_info['Subscribers'],
        'youtube_link': youtube_info['Youtube Link'],
        'twitter_followers': twitter_info['Followers'],
        'twitter_link': twitter_info['twitter_link'],
        'instagram_followers': instagram_info['Followers'],
        'instagram_link': instagram_info['instagram_link'],
    }
    return influencer_data





def get_influencer_info(link, driver):
    """
    link: youtube user's link
    driver: selenium webdriver instance (an automated browser)
    """
    ## find search box, input and search
    driver.get("https://socialblade.com")
    influencer_name = extract_name(link)
    driver.find_element_by_id('SearchInput').send_keys(influencer_name)
    driver.find_element_by_id('SearchInput').send_keys(Keys.RETURN)
    ## navigate to youtube Table Data (videos)
    for a in driver.find_elements_by_xpath('//div[@id="YouTubeUserMenu"]/a'):
        if a.text=='User Videos':
            a.click()
            break

    ### Parse page and grab Youtube data
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    youtube_info = get_youtube_info(soup)
    yt_table_data = get_yt_table_data(soup, num_rows=10)


    ######## Twitter - First table of data
    try:
        #### get links/usernames to instagram, twitter, facebook
        links = [a.attrs['href'] for a in soup.find('div', id='YouTubeUserTopSocial').find_all('a')]
        twitter_username = [s.split('/')[-1] for s in links if 'twitter' in s][0]
        url = "https://socialblade.com/{}/user/{}".format('twitter', twitter_username)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        twitter_info = get_twitter_info(soup)
        ##################################
    except:
        url = "https://socialblade.com/{}/user/{}".format('twitter', influencer_name)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        if soup.find('div', id='bodyContainer'):
            # if soup.find('div', id='bodyContainer') .get_text().strip().startswith('Uh Oh!'):
            print("Youtuber {} has no Twitter account".format(influencer_name))
            twitter_info = {
                'Followers': 0,
                'twitter_link': 'NA',
            }
            ##################################
        else:
            twitter_info = get_twitter_info(soup)
            ##################################

    ######## Instagram - First table of data
    try:
        #### get links/usernames to instagram, twitter, facebook
        links = [a.attrs['href'] for a in soup.find('div', id='YouTubeUserTopSocial').find_all('a')]
        instagram_username = [s.split('/')[-1] for s in links if 'instagram' in s][0]
        url = "https://socialblade.com/{}/user/{}".format('instagram', instagram_username)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        ##################################
        instagram_info = get_instagram_info(soup)
        ##################################
    except:
        url = "https://socialblade.com/{}/user/{}".format('instagram', influencer_name)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        if soup.find('div', id='bodyContainer'):
            # if soup.find('div', id='bodyContainer') .get_text().strip().startswith('Uh Oh!'):
            print("Youtuber {} has no Twitter account".format(influencer_name))
            instagram_info = {
                'Followers': 0,
                'instagram_link': 'NA',
            }
        else:
            ##################################
            instagram_info = get_instagram_info(soup)
            ##################################

    # try:
    #     facebook_username = [s.split('/')[-1] for s in links if 'facebook' in s][0]
    # except:
    #     print("Youtuber {} has no Facebook account".format(influencer_name))

    influencer_data = format_scraped_data(youtube_info, yt_table_data, twitter_info, instagram_info)
    influencer_data['youtube_username'] = influencer_name
    return influencer_data




if __name__=='__main__':
    ####### Get influencer names
    links = list(set([link for link in pd.read_csv('./names.csv')['Youtube link']]))
    names = set([extract_name(link) for link in pd.read_csv('./names.csv')['Youtube link']])
    # iternames = (extract_name(link) for link in pd.read_csv('./names.csv')['Youtube link'])
    # influencer_name = next(iternames)
    # print(influencer_name)

    ######## Initiate web driver
    DRIVER = webdriver.Chrome()
    DRIVER.get("https://socialblade.com")

    df = pd.read_csv("./influencers.csv")
    df.set_index('youtube_link', inplace=True)
    #
    # influencer_data = []
    # new_names = []
    # for n, link in enumerate(links[:10]):
    #     if link in df.index:
    #         print('{} is already in table, skipping'.format(link))
    #         continue
    #     else:
    #         # new_names.append(link)
    #         print(n, link)
    #
    #     try:
    #         influencer_data.append(
    #             get_influencer_info(link, DRIVER)
    #         )
    #     except:
    #         influencer_data.append({
    #             'location': '',
    #             'youtube_category': '',
    #             'date_joined_platform': '',
    #             'post_frequency': '',
    #             'date_of_last_post': '',
    #             'views_of_last_video': '',
    #             'range_of_views': '',
    #             'range_of_comments_on_videos': '',
    #             'total_number_of_videos': '',
    #             'total_video_views': '',
    #             'youtube_subscribers': '',
    #             'original_youtube_link': link,
    #             'youtube_link': '',
    #             'twitter_followers': '',
    #             'twitter_link': '',
    #             'instagram_followers': '',
    #             'instagram_link': '',
    #         })
    #         pass
    #



    # ## Reload most recent dataframe from file before updating
    # df = pd.read_csv("./influencers.csv")
    # df.set_index('youtube_username', inplace=True)
    # ## create dataframe of new scrapped data
    # new_df = pd.DataFrame(influencer_data)
    # new_df['youtube_username'] = [extract_name(l) for l in new_df['youtube_link']]
    # new_df.set_index('youtube_username', inplace=True)
    # ## merge new dataframe we just scrapped
    # df = df.append(new_df)
    # ## remove duplicate entries if any
    # df = df[df.duplicated() == False]
    # ## Safe to file
    # df.to_csv("./influencers.csv")


    ######## Initiate web driver
    DRIVER = webdriver.Chrome()
    DRIVER.get("https://socialblade.com")
    driver = DRIVER
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    def ismonth(str):
        return any(m in str for m in months)

    for n, idx in enumerate(df.index):
        try:
            if ismonth(df.loc[idx]['range_of_comments_on_videos'].lower()):
                print(df.loc[idx]['range_of_comments_on_videos'])

                link = idx
                print(n, link)

                driver.get("https://socialblade.com")
                influencer_name = extract_name(link)
                driver.find_element_by_id('SearchInput').send_keys(influencer_name)
                driver.find_element_by_id('SearchInput').send_keys(Keys.RETURN)
                ## navigate to youtube Table Data (videos)
                for a in driver.find_elements_by_xpath('//div[@id="YouTubeUserMenu"]/a'):
                    if a.text=='User Videos':
                        a.click()
                        break

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                # youtube_info = get_youtube_info(soup)
                yt_table_data = get_yt_table_data(soup, num_rows=10)
                new_range = "{min} to {max}".format(
                        min=min(t['Comments'] for t in yt_table_data),
                        max=max(t['Comments'] for t in yt_table_data)
                    )
                # new_range = get_influencer_info(link, DRIVER)['range_of_comments_on_videos']
                print(new_range)
                # df.ix[idx].set_value('range_of_comments_on_videos', new_range)
                df.set_value(idx, 'range_of_comments_on_videos', new_range)

                # df['range_of_comments_on_videos'] = [x.replace('-', 'to')  if type(x)==str else x for x in df['range_of_comments_on_videos']]
                if n%10 == 0:
                    df.to_csv("./influencers.csv")
                    df = pd.read_csv("./influencers.csv")
                    df.set_index('youtube_link', inplace=True)

        except:
            pass




    # # close selenium webbrowser
    # DRIVER.close()

