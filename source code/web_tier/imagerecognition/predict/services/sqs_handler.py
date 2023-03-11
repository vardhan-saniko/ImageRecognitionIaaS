from predict.services.aws_config import AWSConfig
import boto3


class SQSHandler:
    def __init__(self, queue_name):
        self.client = boto3.client('sqs', aws_access_key_id=AWSConfig.API_KEY, aws_secret_access_key=AWSConfig.API_SECRET, region_name="us-east-1")
        self.queue_name = queue_name
        self.queue = self.client.get_queue_url(QueueName=queue_name)

    def create_message(self, message, message_attrs):
        response = self.client.send_message(QueueUrl=self.queue['QueueUrl'], MessageBody=message, MessageAttributes=message_attrs)
        print("message id = {}".format(response.get('MessageId')))
        return response.get('MessageId')

    def delete_message(self, receipt_handle):
        return self.client.delete_message(QueueUrl=self.queue['QueueUrl'], ReceiptHandle=receipt_handle)

    def receive_message(self, visibility_timeout=1, max_no_of_messages=1):
        res = self.client.receive_message(QueueUrl=self.queue['QueueUrl'], VisibilityTimeout=visibility_timeout, MaxNumberOfMessages=max_no_of_messages, MessageAttributeNames=['All'])
        print("response = {}".format(res))

        return res
