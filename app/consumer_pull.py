from common import Config, ConsumeJob

CONFIG = Config(
    group__id='consumer_pull',
    auto__offset__reset='earliest',
    enable__auto__commit=False,
    bootstrap__servers='localhost:9094'
)
TOPIC = 'topic-1'


with ConsumeJob(CONFIG) as job:
    for message in job.run(TOPIC, 0.5):
        print(message)
