from common import Config, ConsumeJob

CONFIG = Config(
    group__id='consumer_push',
    auto__offset__reset='earliest',
    enable__auto__commit=True,
    bootstrap__servers='localhost:9094',
    fetch__min__bytes=1024
)
TOPIC = 'topic-1'


with ConsumeJob(CONFIG) as job:
    for message in job.run(TOPIC):
        print(message)
