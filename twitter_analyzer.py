# Databricks notebook source
# Import package
import tweepy
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import csv
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from googletrans import Translator

# ---------------------------------------------------------------------------------------------------------------------------
# API Authentication

# Store OAuth authentication credentials in relevant variables
access_token = "1110590844120248321-f3ZoMl6XugySu1Expl0Z6HoYX1keZA"
access_token_secret = "HnFvRGQj6opdA0p1AXW5Imi8FBD2zjT3LlZbDahT063MR"
consumer_key = "UV7YTD3qrxNcSKvCCPWU1R1BE"
consumer_secret = "dtYxHaVjXzlrW5dmLuiqEDDCAHkvHWkxgF80CFotCaIdXxqHCQ"

# Pass OAuth details to tweepy's OAuth handler
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# cria um objeto api
api = tweepy.API(auth)
sents = []

# COMMAND ----------

# api.update_status(auth)

# COMMAND ----------

# create an object called 'customStreamListener'
def twitter_stream_listener(filter_track):

    # class foi stream listener
    class classStreamListener(tweepy.Stream):
        def on_status(self, status):

            # translator ---------------------------------------------------------------------------------------------------
            def translate_text(status_text):
                analysis = TextBlob(status_text)
                lang_detected = analysis.detect_language()
                if analysis.detect_language() != "en":
                    if (
                        analysis.detect_language() == "nl"
                    ):  # got some error with nl language
                        trans = analysis
                    else:
                        # trans = TextBlob(str(analysis.translate(from_lang=lang_detected, to='en')))
                        trans = Translator().translate(status_text).text
                else:
                    trans = analysis

                return trans

            # sentiment analyzer -------------------------------------------------------------------------------------------
            def sentiment_analyzer_scores(tx_trans):
                # score analyser
                score = SentimentIntensityAnalyzer().polarity_scores(tx_trans)
                lb = score["compound"]
                if lb >= 0.05:
                    return 1
                elif (lb > -0.05) and (lb < 0.05):
                    return 0
                else:
                    return -1

            tx_trans = translate_text(
                status.text.encode("latin1", errors="ignore").decode(
                    "latin1", errors="ignore"
                )
            )
            st = sentiment_analyzer_scores(tx_trans)

            ##          print('lang_detected =', lang_detected) # DEBUG
            ##          print(lb) # DEBUG
            ##          print(tx_trans) # DEBUG mostra o valor real do sentimento antes do filtro # DEBUG
            ##          print('') # DEBUG
            ##          print('Mensagem', status.text.encode('latin1', errors='ignore').decode('latin1', errors='ignore')) # DEBUG
            ##          print('O tipo é', type(status.text)) # DEBUG
            ##          print('comeco tweepy') # DEBUG
            ##          print(status.text.encode('latin1', errors='ignore').decode('latin1', errors='ignore')) # DEBUG
            ##          print('fim tweepy') # DEBUG
            ##          print('') # DEBUG

            # list_tweets --------------------------------------------------------------------------------------------------
            def list_tweets(status_text):
                tw = []  # lista de tweets inicialmente vazia
                tw.append(status_text)
                return tw

            tw = list_tweets(
                status.text.encode("latin1", errors="ignore").decode(
                    "latin1", errors="ignore"
                )
            )

            ##          print('O tipo é', type(tw)) # DEBUG
            ##          print('comeco lista') # DEBUG
            ##          print(tw) # DEBUG
            ##          print('fim lista') # DEBUG
            ##          print('') # DEBUG

            ## remove patterns ---------------------------------------------------------------------------------------------
            def remove_pattern(input_txt, pattern):
                r = re.findall(pattern, input_txt)
                for i in r:
                    input_txt = re.sub(i, "", input_txt)
                return input_txt

            ## clean tweets ------------------------------------------------------------------------------------------------
            def clean_tweets(lst):
                # remove twitter Return handles (RT @xxx:)
                lst = np.vectorize(remove_pattern)(lst, "RT @[\w]*:")
                # remove twitter handles (@xxx)
                # lst = np.vectorize(remove_pattern)(lst, "@[\w]*")
                lst = np.core.defchararray.replace(lst, "@", " ")
                # remove URL links (httpxxx)
                lst = np.vectorize(remove_pattern)(lst, "https?://[A-Za-z0-9./]*")
                # remove special characters, numbers, punctuations (except for #)
                lst = np.core.defchararray.replace(lst, "[^a-zA-Z#]", " ")
                return lst

            tw = clean_tweets(tw)

            ##          print('O tipo é', type(tw)) # DEBUG
            ##          print('comeco array') # DEBUG
            ##          print(tw) # DEBUG
            ##          print('fim array') # DEBUG
            ##          print('') # DEBUG

            ## tweets analyzer and graph plot --------------------------------------------------------------------------------
            def anl_tweets(tx_trans, title="Tweets Sentiment"):
                st = sentiment_analyzer_scores(tx_trans)
                sents.append(st)
                ax = sns.distplot(sents, kde=False, bins=5)
                ax.set(
                    xlabel="Negative                            Neutral                             Positive",
                    ylabel="#Tweets",
                    title="Tweets of @" + title,
                )
                plt.show(block=False)
                return sents

            tw_sent = anl_tweets(tx_trans)

            ##          print('tw', tw) # DEBUG
            ##          print('st', st) # DEBUG
            ##          print('O tipo de st é', type(st)) # DEBUG
            ##          print('sents', sents) # DEBUG
            ##          print('O tipo de sents é', type(sents))
            ##          print('tw_sent', tw_sent) # DEBUG
            ##          print('O tipo de tw_sent é', type(tw_sent)) # DEBUG

            # ---------------------------------------------------------------------------------------------------------------------------
            # write data in file

            print(
                "#---------------------------------------------------------------------------------------------------------------------------"
            )
            print("")
            print(status.author.screen_name, status.created_at, tw)
            # Writing status data
            with open("OutputStreaming.txt", "a") as f:
                writer = csv.writer(f)
                ##                writer.writerow([status.author.screen_name, status.created_at, tw, st])
                if st == 1:
                    print("O sentimento da mensagem é POSITIVE")
                    writer.writerow(
                        [status.author.screen_name, status.created_at, tw, "POSITIVE"]
                    )
                elif st == 0:
                    print("O sentimento da mensagem é NEUTRAL")
                    writer.writerow(
                        [status.author.screen_name, status.created_at, tw, "NEUTRAL"]
                    )
                else:
                    print("O sentimento da mensagem é NEGATIVE")
                    writer.writerow(
                        [status.author.screen_name, status.created_at, tw, "NEGATIVE"]
                    )
            print("")

        # ---------------------------------------------------------------------------------------------------------------------------
        # ---------------------------------------------------------------------------------------------------------------------------
        # error
        def on_error(self, status_code):
            if status_code == 420:
                print("Encountered error code 420. Disconnecting the stream")
                # returning False in on_data disconnects the stream
                return False
            else:
                print("Encountered error with status code: {}".format(status_code))
                return True  # Don't kill the stream

        # ---------------------------------------------------------------------------------------------------------------------------
        # ---------------------------------------------------------------------------------------------------------------------------
        # timeout
        def on_timeout(self):
            print(sys.stderr, "Timeout...")
            return True  # Don't kill the stream

    # ---------------------------------------------------------------------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------------------------
    # Writing csv titles
    with open("OutputStreaming.txt", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["Author", "Date", "Text", "Sentimental"])

    # ---------------------------------------------------------------------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------------------------
    streamingAPI = tweepy.streaming.Stream(consumer_key, consumer_secret, access_token, access_token_secret)

    streamingAPI.filter(track=filter_track)

    f.close()

# COMMAND ----------

filter_track1 = ["realDonaldTrump", "wall"]
filter_track2 = ["jairbolsonaro", "previdencia"]

print("-------------------------------------------------------------")
print("API TWITTER Portuguese ANALYZER")
print("-------------------------------------------------------------")
print("Please Type the filter that you want to analyze in Twitter")
print('e.g. "jairbolsonaro" and "previdencia"')
print("To FINISH, press ctrl+c and visualize the sentimental graph")

# filter_track1 = input('First filter: ')
# filter_track2 = input('Second filter: ')
filter_track = [filter_track1, filter_track2]
twitter_stream_listener(filter_track)

# COMMAND ----------


