# SmoothMQ

SmoothMQ is a drop-in replacement for SQS with a much smoother developer experience.
It has a functional UI, observability, tracing, message scheduling, and rate-limiting.
SmoothMQ lets you run a private SQS intance on any cloud.

<!-- A drop-in replacement for SQS designed for great developer experience and efficiency. -->

## Getting Started

SmoothMQ deploys as a single go binary and can be used by any existing SQS client.

## Running

This will run a UI on `:3000` and an SQS-compatible server on `:3001`.

```
$ go run .
```

## Connecting

This works with any SQS client in any language.

### Python

``` py
import boto3

# Simply change the endpoint_url
sqs = boto3.client('sqs',
    aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_KEY',
    region_name='us-east-1',
    endpoint_url='http://localhost:3001'
)

sqs.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/1/my_queue'
    MessageBody="hello world"
)
```

Celery also works seamlessly:

``` py
app = Celery("tasks",
    broker_url=f"sqs://ACCESS_KEY:SECRET_KEY@localhost:3001",
    broker_transport_options={"region": "us-east-1"},
)
```

## UI

The UI lets you manage queues and search individual messages.

![Dashboard UI](docs/queue.gif)