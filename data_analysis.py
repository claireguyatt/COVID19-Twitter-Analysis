import pandas as pd
import argparse
import os
import re

# list of topics of interest & sentiment scores
topics = ['vaccination', 'covid', 'pandemic', 'restrictions', 'testing', 'conspiracy_theories', 'other']
sentiments = ['positive', 'neutral', 'negative']


# check that annotations were done correctly
def check_annotation(data, topics, sentiments):
    # put csv into pandas df
    data_df = pd.read_csv(data, sep="\t")

    # make all annotations lowercase & remove accidental spaces
    annotation_columns = ['topic', 'sentiment']
    for annotation in annotation_columns:
        data_df[annotation] = data_df[annotation].str.lower()
        data_df[annotation] = data_df[annotation].str.replace('conspiracy theories', 'conspiracy_theories')
        data_df[annotation] = data_df[annotation].str.replace(' ', '')

    # check all Tweets are labeled properly with topics & sentiment
    labeled_incorrectly = data_df[~data_df['topic'].isin(topics)]
    labeled_incorrectly = labeled_incorrectly[~data_df['sentiment'].isin(sentiments)]

    # return correctly annotated df or print wrongly annotated rows
    labeled_incorrectly.reset_index()
    if len(labeled_incorrectly.index) != 0:
        print(labeled_incorrectly)

    else:
        return data_df


# get sentiment score
def get_sentiment(topic_data, index, stats):
    # subtract negative from positive
    num_pos = len(topic_data[topic_data['sentiment'] == 'positive'])
    num_neg = len(topic_data[topic_data['sentiment'] == 'negative'])
    score = num_pos - num_neg
    stats.at[index, 'sentiment'] = score
    stats.at[index, 'sentiment_ratio'] = score / len(topic_data)


def get_tf(word, topic_words):
    return topic_words[word]


def get_idf(word, data):
    word_usage_in_topic_count = 0
    for topic in topics:
        words = set()
        data[data['topic'] == topic].text.str.lower().str.replace(r"[,.:]", "", regex=True).str.split().apply(words.update)
        if word in words:
            word_usage_in_topic_count += 1
    return len(topics) / word_usage_in_topic_count


# get tf-idf (note, pass entire dataset)
def get_top_10_tfidf(data, index, stats, topic, stopwords):
    # data['prepped_text'] = data['text'].str.lower().str.replace(r"[,().?!:;&>']", " ", regex=True)
    topic_words = data[data['topic'] == topic].text.str.lower().str.replace(r"[,.:]", "", regex=True).str.split(expand=True).stack().value_counts()
    tf_idfs = []

    for word in topic_words.keys():
        # remove stopwords
        if (word not in stopwords) and (re.match(r"^[a-zA-Z]*(-19)*$",word)):
            word_tf_idf = get_tf(word, topic_words) * get_idf(word, data)
            tf_idfs.append((word, word_tf_idf))

    tf_idfs.sort(key=lambda tf_idf: tf_idf[1], reverse=True)

    stats.at[index, 'tfidf'] = [tf[0] for tf in tf_idfs[:10]]


def get_engagement(topic_data, index, stats):
    total_engage = topic_data['like_count'].sum() + topic_data['quote_count'].sum() + topic_data['reply_count'].sum() + \
                   topic_data['retweet_count'].sum()
    stats.at[index, 'engagement'] = total_engage
    stats.at[index, 'engagement_ratio'] = total_engage / len(topic_data)


def main():
    # use argparse to get input / output files
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", help="input filename")
    parser.add_argument("-o", help="output filename")
    args = parser.parse_args()
    data = args.d
    output = args.o

    # ensure all Tweets were properly annotated
    data = check_annotation(data, topics, sentiments)

    # get stats for topics
    stats = pd.DataFrame([],
                         columns=['topic', 'num_tweets', 'sentiment', 'sentiment_ratio', 'tfidf', 'engagement',
                                  'engagement_ratio'])
    stats['topic'] = topics

    stopwards_path = os.path.join(os.path.dirname(__file__), "data/stopwords.txt")
    with open(stopwards_path, "r") as f:
        stopwords = f.read().split()[17:]

    index = 0

    for t in topics:
        topic_data = data[data['topic'] == t]
        stats.at[index, 'num_tweets'] = len(data[data['topic'] == t])
        get_sentiment(topic_data, index, stats)
        get_engagement(topic_data, index, stats)
        get_top_10_tfidf(data, index, stats, t, stopwords)
        index = index + 1

    print(stats)

    stats.to_csv(output)


if __name__ == '__main__':
    main()
