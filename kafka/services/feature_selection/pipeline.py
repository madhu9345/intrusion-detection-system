from consumer import consumer
from producer import publish

print("Feature Selection Service Started")

for msg in consumer:
    data = msg.value

    print("Received:")
    print(data)

    publish(data)