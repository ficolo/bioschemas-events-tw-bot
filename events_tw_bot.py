from twitter import *
from extruct.w3cmicrodata import MicrodataExtractor
from docs import conf
import urllib2
from datetime import datetime, timedelta
from sets import Set
import requests
import json


def get_edam_tags(text):
    tags = []
    text = ' '.join(text.split())
    post_data = dict(apikey=conf.BIOPORTAL_API_KEY, text=text,
                     display_links='false', display_context='false', minimum_match_length='3',
                     exclude_numbers='true', longest_only='true', ontologies='EDAM', exclude_synonyms='false',
                     expand_class_hierarchy='false', class_hierarchy_max_level='4',
                     include='prefLabel')
    register = {}
    try:
        response = requests.post(conf.ANNOTATOR_URL, post_data)
        json_results = json.loads(response.text)
        for result in json_results:
            if 'topic' in result['annotatedClass']['@id']:
                tag = result['annotatedClass']['prefLabel']
                if tag != 'Topic' and tag not in register:
                    tags.append({'tag': tag, 'type': 'direct'})
                    register[tag] = 1
                for annotated_class in result['hierarchy']:
                    tag = annotated_class['annotatedClass']['prefLabel']
                    if tag != 'Topic' and tag not in register:
                        tags.append({'tag': tag, 'type': 'ancestor'})
                        register[tag] = 1
        return tags
    except (ValueError, IndexError, KeyError) as e:
        print e
        return tags


def start_bot():
    auth = OAuth(conf.TOKEN, conf.TOKEN_SECRET, conf.CONSUMER_KEY, conf.CONSUMER_SECRET)
    events = get_events('https://tess.elixir-uk.org/events?include_expired=true&sort=late',
                        date_format="%Y-%m-%d %H:%M:%S %Z",
                        start_date='2017-02-01 00:00:00', end_date='2017-03-31 00:00:00',
                        pagination_param='page', sort=True)
    events = tag_events(events)
    events = get_tracking_keywords(events)
    keywords = [event['tracking_keywords'] for event in events['past_events']]
    query = ''
    for word in keywords:
        query += word + ','
    twitter_public_stream = TwitterStream(auth=auth, domain='stream.twitter.com')
    twitter = Twitter(auth=auth)
    print query
    it = twitter_public_stream.statuses.filter(track=query)
    for tweet in it:
        for event in events['past_events']:
            if event['tracking_keywords'] in tweet['text']:
                related = get_most_related_event(event, events['incoming_events'])
                if related['event'] != {}:
                    prefix = u'Hi @%s' % (tweet[u'user'][u'screen_name'])
                    # response = twitter.statuses.update(
                    #     status=prefix + ' The common topics are: ' + ', '.join(related['intersection']),
                    #     in_reply_to_status_id=tweet[u'id_str'],
                    #     in_reply_to_user_id=tweet[u'user'][u'id_str'],
                    #     in_reply_to_screen_name=tweet[u'user'][u'screen_name']
                    #)
                    response = twitter.statuses.update(
                        status=prefix + ' This is a related event: ' +
                               related['event']['properties']['name'] + ' ' + related['event']['properties']['sameAs'],
                        in_reply_to_status_id=tweet[u'id_str'],
                        in_reply_to_user_id=tweet[u'user'][u'id_str'],
                        in_reply_to_screen_name=tweet[u'user'][u'screen_name']
                    )
        print tweet


def get_most_related_event(event, events):
    event_topics = [topic['tag'] for topic in event['edam_tags']]
    event_topics = Set(event_topics)
    intersection = Set()
    related_event = {}
    for upcoming in events:
        related_event_topics = [topic['tag'] for topic in upcoming['edam_tags'] if topic['type'] == 'direct']
        related_event_topics = Set(related_event_topics)
        if len(event_topics & related_event_topics) > len(intersection):
            intersection = event_topics & related_event_topics
            related_event = upcoming
    return dict(event=related_event, intersection=intersection)


def tag_events(events):
    for event in events['past_events'] + events['incoming_events']:
        event['edam_tags'] = get_edam_tags(event['properties']['description']) \
            if 'description' in event['properties'] else []
    return events


def get_tracking_keywords(events):
    for event in events['past_events']:
        words = event['properties']['name']
        if len(words) > 60:
            words = words[:59]
        words = words.replace(',', ' ').replace('-', ' ').replace('/', ' ')
        words = ' '.join(words.split())
        event['tracking_keywords'] = words
    return events


def get_data(url):
    mde = MicrodataExtractor()
    response = urllib2.urlopen(url)
    html = response.read()
    data = mde.extract(html)
    return [item for item in data['items'] if item['type'] == 'http://schema.org/Event']


def get_events(url, pagination_param=None, date_format=None, start_date=None, end_date=None, sort=False):
    data = []
    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    if not pagination_param:
        data = get_data(url)
    else:
        page = 1
        temp = get_data(url + '&' + pagination_param + '=' + str(page))
        while len(temp) > 0:
            page += 1
            data += temp
            temp = get_data(url + '&' + pagination_param + '=' + str(page))
            if sort and datetime.strptime(temp[0]['properties']['startDate'], date_format) < start_date:
                break
    past_events = []
    incoming_events = []
    for item in data:
        if 'startDate' in item['properties']:
            event_start_dt = datetime.strptime(item['properties']['startDate'], date_format)
            if start_date < event_start_dt < end_date:
                if event_start_dt <= datetime.today() + timedelta(days=1):
                    past_events.append(item)
                if event_start_dt > datetime.today() + timedelta(days=1):
                    incoming_events.append(item)
    return dict(past_events=past_events, incoming_events=incoming_events)


start_bot()
