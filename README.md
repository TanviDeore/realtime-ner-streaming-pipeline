# Installation of dependancies
pip install newsapi-python kafka-python pyspark spacy \
python -m spacy download en_core_web_sm

# Start Kafka Server (WSL)
cd 'your-kafka-folder' \
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)" \
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties \
bin/kafka-server-start.sh config/server.properties

# Start Elasticssearch (PowerShell)
cd 'your-elasticsearch-folder' \
bin\elasticsearch.bat

# Start Kibana (PowerShell)
cd 'your-kibana-folder' \
bin\kibana.bat

# Start Logstash with NER pipeline (PowerShell) (create ner.conf)
cd 'your-logstash-folder' \
bin\logstash.bat -f pipeline\ner.conf

# Start the Kafka Producer (WSL) (if interval not given, default interval is 30)
python3 producer.py --interval 45

# Start the Spark Consumer (WSL)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2 spark_consumer.py

# Open Kibana, create data view and visualization, and observe the streaming entity counts.
• Open localhost:5601 \
• Go to Stack Management → Data Views → Create data view \
• Name: new_data \
• Index pattern: named-entities-* \
• Timestamp field: @timestamp \
• Save \
• Create a Bar Chart: \
• Go to Analytics → Visualize Library → Create visualization → Lens \
• Select data view: new_data \
• Drag entity.keyword to the X-axis (Top values, size = 10) \
• Drag Count to the Y-axis \

# Data Source 
Used NewsAPI as streaming data source. The producer script calls get_top_headlines every 30 seconds (default interval unless you pass any other value), also added a topic switching loop in the producer. The script changes the NewsAPI query category every 20 minutes. i.e., during each time window, the producer fetches news from a different topic category. This approach ensures that the incoming data slowly shifts from one domain to another. This helps produce noticeable changes in the extracted entities over time.

<img width="1616" height="186" alt="image" src="https://github.com/user-attachments/assets/d074059f-2ced-488c-b6b5-7e8e9f217e1d" />

# Result
The results show the top-named entities appearing in the news stream for each time period. When the produces is fetching “politics” news, the most frequent entities are politics-related names (e.g., Trump, Donald Trump, etc. ), when it switches to “technology”, the dominant entities switch toward tech-related words like YouTube, AI terms. The bar charts taken at different time stamps reflect these changes. They show how the entity distribution evolves based on the news category being collected at that time. This demonstrates that the pipeline is working end-to-end and that new streaming data leads to different real-time NER results.
@ 19:27
<img width="1094" height="550" alt="image" src="https://github.com/user-attachments/assets/0e3ddc54-0d59-4d43-978c-63420fc77b16" />

@ 19:45 
<img width="1627" height="750" alt="image" src="https://github.com/user-attachments/assets/dae89439-a99a-469a-91a0-0d39b754a9ce" />

@ 20:20
<img width="1553" height="676" alt="image" src="https://github.com/user-attachments/assets/cab9be35-4423-429b-9ebe-4b68f6b72a17" />

@ 21:30 
<img width="1521" height="721" alt="image" src="https://github.com/user-attachments/assets/9671015b-71d5-4399-bd8e-595b68d99f26" />




