import twitter
import time
import sys
import os
import re
import spacy

from pymongo import MongoClient
from twitter import Twitter
from datetime import datetime

'''States geo codes and details'''
states = [{'State_abbv': 'AL', 'State': 'Alabama', 'Largest city': 'Birmingham', 'Middle point': 'Tyler',
           'geo_code_large': '33.5206608,-86.80248999999998,120km',
           'geo_code_middle_point': '32.3182314,-86.90229799999997,100km', 'max_id': {0: '', 1: ''},
           'example:': '39.95372,-74.19792,200km'},
          {'State_abbv': 'AK', 'State': 'Alaska', 'Largest city': 'Anchorage', 'Middle point': 'Clear',
           'geo_code_large': '61.2180556,-149.90027780000003,350km',
           'geo_code_middle_point': '64.2008413,-149.4936733,350km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'AZ', 'State': 'Arizona', 'Largest city': 'Phoenix', 'Middle point': 'Gisela',
           'geo_code_large': '33.4483771,-112.07403729999999,150km',
           'geo_code_middle_point': '34.0833745509365,-111.16310119628906,150km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'AR', 'State': 'Arkansas', 'Largest city': 'Little Rock', 'Middle point': 'Searcy',
           'geo_code_large': '34.7464809,-92.28959479999997,120km',
           'geo_code_middle_point': '35.20105,-91.8318334,100km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'CA', 'State': 'California', 'Largest city': 'Los Angeles', 'Middle point': 'Gravesboro',
           'geo_code_large': '34.0522342,-118.2436849,170km',
           'geo_code_middle_point': '36.778261,-119.41793239999998,130km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'CO', 'State': 'Colorado', 'Largest city': 'Denver', 'Middle point': 'Keystone',
           'geo_code_large': '39.7392358,-104.990251,100km',
           'geo_code_middle_point': '39.5500507,-105.78206740000002,150km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'CT', 'State': 'Connecticut', 'Largest city': 'Bridgeport', 'Middle point': 'Watertown',
           'geo_code_large': '41.1865478,-73.19517669999999,15km',
           'geo_code_middle_point': '41.6032207,-73.08774900000003,30km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'DE', 'State': 'Delaware', 'Largest city': 'Wilmington', 'Middle point': 'Milford',
           'geo_code_large': '39.73907210000001,-75.5397878,10km',
           'geo_code_middle_point': '38.9108325,-75.52766989999998,15km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'DC', 'State': 'DC', 'Largest city': 'Washington', 'Middle point': 'Washington',
           'geo_code_large': '38.910255,-77.006461,4km', 'geo_code_middle_point': '38.910255,-77.006461,4km',
           'max_id': {0: '', 1: ''}},
          {'State_abbv': 'FL', 'State': 'Florida', 'Largest city': 'Jacksonville', 'Middle point': 'Avon Park',
           'geo_code_large': '30.3321838,-81.65565099999998,32km',
           'geo_code_middle_point': '27.6648274,-81.51575350000002,140km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'GA', 'State': 'Georgia', 'Largest city': 'Atlanta', 'Middle point': 'McRae',
           'geo_code_large': '33.7489954,-84.3879824,80km',
           'geo_code_middle_point': '32.1656221,-82.90007509999998,120km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'HI', 'State': 'Hawaii', 'Largest city': 'Honolulu', 'Middle point': '',
           'geo_code_large': '21.3069444,-157.85833330000003,400km',
           'geo_code_middle_point': '21.3069444,-157.85833330000003,400km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'ID', 'State': 'Idaho', 'Largest city': 'Boise', 'Middle point': 'Stanley',
           'geo_code_large': '43.61871019999999,-116.21460680000001,60km',
           'geo_code_middle_point': '44.0682019,-114.74204079999998,110km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'IL', 'State': 'Illinois', 'Largest city': 'Chicago', 'Middle point': 'Peoria',
           'geo_code_large': '41.8781136,-87.62979819999998,12km',
           'geo_code_middle_point': '40.6331249,-89.39852830000001,115km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'IN', 'State': 'Indiana', 'Largest city': 'Indianapolis', 'Middle point': 'Kokomo',
           'geo_code_large': '39.768403,-86.15806800000001,110km',
           'geo_code_middle_point': '39.768403,-86.15806800000001,110km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'IA', 'State': 'Iowa', 'Largest city': 'Des Moines', 'Middle point': 'Marshaltown',
           'geo_code_large': '41.6005448,-93.60910639999997,100km',
           'geo_code_middle_point': '41.8780025,-93.09770200000003,100km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'KS', 'State': 'Kansas', 'Largest city': 'Wichita', 'Middle point': 'Sylvan Grove',
           'geo_code_large': '37.68717609999999,-97.33005300000002,70km',
           'geo_code_middle_point': '39.011902,-98.48424649999998,100km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'KY', 'State': 'Kentucky', 'Largest city': 'Louisville', 'Middle point': 'Richmond',
           'geo_code_large': '38.175664,-85.737125,10km', 'geo_code_middle_point': '37.8393332,-84.27001789999997,95km',
           'max_id': {0: '', 1: ''}},
          {'State_abbv': 'LA', 'State': 'Louisiana', 'Largest city': 'New Orleans', 'Middle point': 'Alexandria',
           'geo_code_large': '29.95106579999999,-90.0715323,50km', 'geo_code_middle_point': '31.340789,-92.439195,70km',
           'max_id': {0: '', 1: ''}}, {'State_abbv': 'ME', 'State': 'Maine', 'Largest city': 'Portland',
                                       'Middle point': 'Monson', 'geo_code_large': '43.66147100000001,-70.2553259,55km',
                                       'geo_code_middle_point': '45.2869412,-69.4991248,80km',
                                       'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MD', 'State': 'Maryland', 'Largest city': 'Baltimore', 'Middle point': 'Millersville',
           'geo_code_large': '39.2903848,-76.61218930000001,40km',
           'geo_code_middle_point': '39.2903848,-76.61218930000001,40km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MA', 'State': 'Massachusetts', 'Largest city': 'Boston', 'Middle point': 'Quabbin Reservoir',
           'geo_code_large': '42.3600825,-71.05888010000001,35km',
           'geo_code_middle_point': '42.3346870,-72.2877120,35km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MI', 'State': 'Michigan', 'Largest city': 'Detroit', 'Middle point': 'Harrietta',
           'geo_code_large': '42.4662590,-83.4039530,30km',
           'geo_code_middle_point': '44.3148443,-85.60236429999998,100km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MN', 'State': 'Minnesota', 'Largest city': 'Minneapolis', 'Middle point': 'Nimrod',
           'geo_code_large': '44.977753,-93.26501080000003,30km',
           'geo_code_middle_point': '46.729553,-94.68589980000002,140km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MS', 'State': 'Mississippi', 'Largest city': 'Jackson', 'Middle point': 'Forest',
           'geo_code_large': '32.2987573,-90.18481029999998,60km',
           'geo_code_middle_point': '32.35466789999999,-89.39852830000001,80km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MO', 'State': 'Missouri', 'Largest city': 'Kansas City', 'Middle point': 'Rolla',
           'geo_code_large': '39.2934490,-93.9738200,55km', 'geo_code_middle_point': '37.9642529,-91.8318334,130km',
           'max_id': {0: '', 1: ''}},
          {'State_abbv': 'MT', 'State': 'Montana', 'Largest city': 'Billings', 'Middle point': 'Sapphire Village',
           'geo_code_large': '45.78328559999999,-108.5006904,80km',
           'geo_code_middle_point': '46.8796822,-110.36256579999997,200km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NE', 'State': 'Nebraska', 'Largest city': 'Omaha', 'Middle point': 'Merna',
           'geo_code_large': '41.2933710,-97.1498660,90km',
           'geo_code_middle_point': '41.4925374,-99.90181310000003,150km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NV', 'State': 'Nevada', 'Largest city': 'Las Vegas', 'Middle point': 'Duckwater',
           'geo_code_large': '36.1699412,-115.13982959999998,30km',
           'geo_code_middle_point': '38.8026097,-116.41938900000002,190km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NH', 'State': 'New Hampshire', 'Largest city': 'Manchester', 'Middle point': 'Concord',
           'geo_code_large': '42.9956397,-71.45478909999997,30km',
           'geo_code_middle_point': '43.19385159999999,-71.57239529999998,50km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NJ', 'State': 'New Jersey', 'Largest city': 'Newark', 'Middle point': 'Cassville',
           'geo_code_large': '40.735657,-74.1723667,9km', 'geo_code_middle_point': '40.0583238,-74.4056612,28km',
           'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NM', 'State': 'New Mexico', 'Largest city': 'Albuquerque', 'Middle point': 'Willard',
           'geo_code_large': '35.0853336,-106.60555340000002,200km',
           'geo_code_middle_point': '34.5199402,-105.87009009999997,200km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NY', 'State': 'New York', 'Largest city': 'New York City', 'Middle point': 'Hope',
           'geo_code_large': '40.7456050,-73.9582210,4km',
           'geo_code_middle_point': '43.2994285,-74.21793260000004,70km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'NC', 'State': 'North Carolina', 'Largest city': 'Charlotte', 'Middle point': 'Bells',
           'geo_code_large': '35.2270869,-80.84312669999997,15km',
           'geo_code_middle_point': '35.7595731,-79.01929969999998,80km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'ND', 'State': 'North Dakota', 'Largest city': 'Fargo', 'Middle point': 'Turtle Lake',
           'geo_code_large': '46.9357940,-98.0039180,88km',
           'geo_code_middle_point': '47.55149259999999,-101.00201190000001,150km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'OH', 'State': 'Ohio', 'Largest city': 'Columbus', 'Middle point': 'Ashley',
           'geo_code_large': '39.9611755,-82.99879420000002,120km',
           'geo_code_middle_point': '40.4172871,-82.90712300000001,130km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'OK', 'State': 'Oklahoma', 'Largest city': 'Oklahoma City', 'Middle point': 'Wanette',
           'geo_code_large': '35.4675602,-97.51642759999999,140km',
           'geo_code_middle_point': '35.0077519,-97.09287699999999,200km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'OR', 'State': 'Oregon', 'Largest city': 'Portland', 'Middle point': 'Brothers',
           'geo_code_large': '45.52306220000001,-122.67648159999999,9km',
           'geo_code_middle_point': '43.8041334,-120.55420119999997,195km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'PA', 'State': 'Pennsylvania', 'Largest city': 'Philadelphia', 'Middle point': 'Larryville',
           'geo_code_large': '40.0410350,-75.2343260,12km',
           'geo_code_middle_point': '41.2033216,-77.19452469999999,85km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'RI', 'State': 'Rhode Island', 'Largest city': 'Providence', 'Middle point': 'Wickford',
           'geo_code_large': '41.8239891,-71.41283429999999,5km',
           'geo_code_middle_point': '41.5800945,-71.4774291,24km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'SC', 'State': 'South Carolina', 'Largest city': 'Columbia', 'Middle point': 'Gaston',
           'geo_code_large': '34.0007104,-81.03481440000002,90km',
           'geo_code_middle_point': '34.0007104,-81.03481440000002,90km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'SD', 'State': 'South Dakota', 'Largest city': 'Sioux Falls', 'Middle point': 'Kennebec',
           'geo_code_large': '43.6660130,-97.4528290,70km',
           'geo_code_middle_point': '43.9695148,-99.90181310000003,100km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'TN', 'State': 'Tennessee', 'Largest city': 'Memphis', 'Middle point': 'Anchor Mill',
           'geo_code_large': '35.1382480,-89.9140320,12km', 'geo_code_middle_point': '35.5174913,-86.5804473,55km',
           'max_id': {0: '', 1: ''}}, {'State_abbv': 'TX', 'State': 'Texas', 'Largest city': 'Houston',
                                       'Middle point': 'Winters', 'geo_code_large': '29.7604267,-95.3698028,140km',
                                       'geo_code_middle_point': '31.9685988,-99.90181310000003,250km',
                                       'max_id': {0: '', 1: ''}},
          {'State_abbv': 'UT', 'State': 'Utah', 'Largest city': 'Salt Lake City', 'Middle point': 'Huntington',
           'geo_code_large': '40.7607793,-111.89104739999999,75km',
           'geo_code_middle_point': '39.3209801,-111.09373110000001,170km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'VT', 'State': 'Vermont', 'Largest city': 'Burlington', 'Middle point': 'Morrisville',
           'geo_code_large': '44.4271240,-72.9412990,27km',
           'geo_code_middle_point': '44.5588028,-72.57784149999998,45km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'VA', 'State': 'Virginia', 'Largest city': 'Virginia Beach', 'Middle point': 'Sliders',
           'geo_code_large': '36.8529263,-75.97798499999999,30km',
           'geo_code_middle_point': '37.4315734,-78.65689420000001,90km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'WA', 'State': 'Washington', 'Largest city': 'Seattle', 'Middle point': 'Plain',
           'geo_code_large': '47.6062095,-122.3320708,100km',
           'geo_code_middle_point': '47.7510741,-120.74013860000002,130km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'WV', 'State': 'West Virginia', 'Largest city': 'Charleston', 'Middle point': 'Diana',
           'geo_code_large': '38.3498195,-81.6326234,50km',
           'geo_code_middle_point': '38.59762619999999,-80.45490259999997,60km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'WI', 'State': 'Wisconsin', 'Largest city': 'Milwaukee', 'Middle point': 'Ripon',
           'geo_code_large': '43.0389025,-87.90647360000003,55km',
           'geo_code_middle_point': '43.78443970000001,-88.78786780000001,120km', 'max_id': {0: '', 1: ''}},
          {'State_abbv': 'WY', 'State': 'Wyoming', 'Largest city': 'Cheyenne', 'Middle point': 'Powder River',
           'geo_code_large': '41.1399814,-104.82024619999999,12km',
           'geo_code_middle_point': '43.0759678,-107.29028390000002,200km', 'max_id': {0: '', 1: ''}}]

'''*** Twitter authentication details***'''
consumer_key = '<Your consumer key provided by Twitter>'
consumer_secret = '<Your consumer secret provided by Twitter>'
BEARER_TOKEN = twitter.oauth2_dance(consumer_key, consumer_secret)

'''Ignored fields to filter before push the data to the DB'''

ignored_fields = ['id_str', 'truncated', 'in_reply_to_status_id_str','in_reply_to_user_id_str',
                  'in_reply_to_screen_name', 'place', 'quoted_status_id','quoted_status_id_str','quoted_status',
                  'quote_count', 'reply_count', 'favorite_count','extended_entities','favorited', 'possibly_sensitive',
                  'filter_level', 'lang', 'matching_rules', 'scopes', 'withheld_copyright', 'withheld_in_countries',
                  'withheld_scope','metadata', 'contributors']

user_ignored_fields = ['id_str', 'url', 'verified', 'friends_count', 'listed_count', 'favourites_count',
                       'statuses_count', 'created_at', 'utc_offset', 'geo_enabled', 'profile_image_url_https',
                       'entities', 'derived', 'protected', 'contributors_enabled', 'profile_background_color',
                       'profile_background_image_url', 'profile_background_image_url_https', 'profile_background_tile',
                       'profile_banner_url', 'profile_image_url', 'profile_image_url_https', 'profile_link_color',
                       'profile_sidebar_border_color', 'profile_sidebar_fill_color', 'profile_text_color',
                       'profile_use_background_image', 'default_profile', 'default_profile_image',
                       'withheld_in_countries', 'withheld_scope', 'is_translator', 'following', 'notifications',
                       'is_translation_enabled', 'has_extended_profile', 'follow_request_sent', 'translator_type']


search_order = {0: 'geo_code_large', 1: 'geo_code_middle_point'}


# Text Analysis Tools:
nlp = spacy.load('en')
'''TATE = importr('TATE')'''


# Use re to get rid of the milliseconds.
def remove_ms(x):
    return re.sub("\+\d+\s", "", x)


# Make the string into a datetime object.
def mk_dt(x):
    return datetime.strptime(remove_ms(x), "%a %b %d %H:%M:%S %Y")


# Analyzing tweets text field
def analyze(tweets):
    for tweet in tweets:
        # Spacy Analyze:
        total_count = 0
        auxpass_count = 0
        doc = nlp(tweet['text'])

        for token in doc:
            # print(token.text, token.dep_)
            if token.dep_ != 'punct':
                total_count += 1
                if token.dep_ == 'auxpass':
                    auxpass_count += 1

        auxpass = auxpass_count / total_count
        tweet['auxpass'] = auxpass
        return tweets
'''
        # TATE Analyze:
        un_emoticon_text = tweet['text'].encode("ascii", 'ignore').decode()
        vad = TATE.NoVAD(un_emoticon_text)
        tweet['valence'] = vad[0]
        tweet['arousal'] = vad[1]
        tweet['dominance'] = vad[2]

        con = TATE.concretness(un_emoticon_text)
        tweet['concretness'] = con[0]
    # explicit garbage collection
    gc.collect()
    '''


# Filter unnecessary fields and collecting the tweets
# Returns lowest id - 1
def filter_and_store(tweets, State_abbv, search_loc,  db_collection):
    store = []
    for tweet in tweets:
        try:    # Ignore retweet
            tweet['retweeted_status']
            pass
        except KeyError:
            if tweet['source'].find('twitter.com') > 0 or tweet['source'].find('facebook.com') > 0:
                for key in ignored_fields:
                    try:
                        del tweet[key]
                    except KeyError:
                        pass

                for key in user_ignored_fields:
                    try:
                        del tweet['user'][key]
                    except KeyError as e:
                        pass

                tweet['State_abbv'] = State_abbv
                tweet['Location'] = search_loc
                tweet['created_at'] = mk_dt(tweet['created_at'])
                store.append(tweet)

    if store.__len__() > 0:

        try:    # ordered=False important to insert any tweet that doesn't exist in the DB
            count = db_collection.insert_many(documents=analyze(store), ordered=False).inserted_ids.__len__()
            print(count, " tweets collected from ", State_abbv)
        except Exception as e:
            print(e.details['nInserted'], " tweets collected from ", State_abbv)

    return tweet['id'] - 1


def main():
    # TODO: add twitter api connection check (Optional)
    tw_api = Twitter(auth=twitter.OAuth2(bearer_token=BEARER_TOKEN))

    try:
        rate_limit_status = tw_api.application.rate_limit_status(resources="search")['resources']['search']['/search/tweets']['limit']
        if rate_limit_status < 408:
            print("rate limit exceeded - will try again in 30 minutes...")
            exit(-2)
        else:
            print("rate limit check - OK")
    except Exception as e:
        print(e)
        rate_limit_status = 450     # Rate limit status as in documentation

    client = MongoClient(appname="Twitter Get & Store")

    try:
        client.address  # Throws exception if the database is not connected
    except Exception as e:
        print("Could not connect to MongoDB, check if the process 'mongod' is running. Exception: ", e)
        exit(-1)

    db_collection = client.twitter.main
    #db_collection.create_index([("id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique

    num_of_iters = rate_limit_status // 102     # 51 states, 2 requests for each = 102 requests
    for x in range(0, num_of_iters):
        print(x, ":")
        for i in range(0, 2):
            for state in states:
                if state['max_id'][i] != '':
                    res = tw_api.search.tweets(q=" ", count=100, lang="en", geocode=state[search_order[i]],
                                               max_id=state['max_id'][i], include_entities=0)
                else:   # first search
                    res = tw_api.search.tweets(q=" ", count=100, lang="en", geocode=state[search_order[i]],
                                               include_entities=0)

                state['max_id'][i] = filter_and_store(res['statuses'], state['State_abbv'], search_order[i],
                                                      db_collection)


if __name__ == '__main__':
    # If running with windows scheduler (the running process is python which has different working directory).
    # os.chdir("<GetAndStore Project Directory Path>")
    sys.stdout = open("log.txt", "a")
    sys.stderr = sys.stdout  # prints errors to the log file.

    print("\n", time.ctime(), ":") # Start time printing
    start = time.time()

    main()

    end = time.time()
    print("Execution Time: ", (end - start)/60, "minutes\n")
    exit(0)
