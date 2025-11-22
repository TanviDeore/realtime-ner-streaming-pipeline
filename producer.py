import time, json, argparse
from newsapi import NewsApiClient
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=int, default=30)
args = parser.parse_args()

newsapi = NewsApiClient(api_key="6769ed6725af476bb056a55033c63c84")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#TOPIC ROTATION
TOPICS = [
    "politics",
    "technology",
    "sports",
    "business",
    "entertainment",
]

#20 minutes per topic
TOPIC_DURATION = 1200

def get_current_topic(start_time):
    elapsed = time.time() - start_time
    segment_index = int(elapsed // TOPIC_DURATION) % len(TOPICS)
    return TOPICS[segment_index]

def stream_news_to_kafka():
    start_time = time.time()

    while True:
        try:
            topic = get_current_topic(start_time)
            print(f"\n>>> Fetching news for topic: {topic}")

            response = newsapi.get_everything(
                q=topic,
                language="en",
                sort_by="publishedAt",
                page_size=20,
            )

            for article in response["articles"]:
                title = article.get("title", "")
                description = article.get("description", "")
                content = f"{title}. {description}"
                if content.strip():
                    producer.send("topic1", {"text": content})
                    print(f"Sent to Kafka: {title}")

            time.sleep(args.interval)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    stream_news_to_kafka()
