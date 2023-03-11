#!/bin/bash
#cloud-boothook

export AWS_ACCESS_KEY_ID=#############
export AWS_SECRET_ACCESS_KEY=############
export AWS_STORAGE_INPUT_BUCKET_NAME=input-bucket-mavericks
export AWS_STORAGE_OUTPUT_BUCKET_NAME=input-bucket-mavericks
export INPUT_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/316986363398/image_processing_queue'
export OUTPUT_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/316986363398/output_queue'


# Define the Python script to read the file from S3
python_script=$(cat <<EOF

import os.path
from flask import Flask
import boto3
import subprocess
import os

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'
AWS_STORAGE_INPUT_BUCKET_NAME = os.environ.get('AWS_STORAGE_INPUT_BUCKET_NAME')
AWS_STORAGE_OUTPUT_BUCKET_NAME = os.environ.get('AWS_STORAGE_OUTPUT_BUCKET_NAME')
input_queue_url = os.environ.get('INPUT_QUEUE_URL')
output_queue_url = os.environ.get('OUTPUT_QUEUE_URL')
# input_queue_url = 'https://sqs.us-east-1.amazonaws.com/316986363398/image_processing_queue'
# output_queue_url = 'https://sqs.us-east-1.amazonaws.com/316986363398/output_queue'

sqs = boto3.client('sqs', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

app = Flask(__name__)

def poll_incoming_messages():
    # Continuously poll for messages from the queue

    # base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = "/home/ubuntu/"
    sqs_images_dir_path = os.path.join(base_path, 'sqs_images')
    if not os.path.exists(sqs_images_dir_path):
        os.makedirs(sqs_images_dir_path)
        print('Directory created successfully')
    else:
        print('Directory already exists')

    while True:
        response = sqs.receive_message(
            QueueUrl=input_queue_url,
            MaxNumberOfMessages=1,  # Maximum number of messages to receive in one call
            WaitTimeSeconds=20,  # Wait time for long polling
            # VisibilityTimeout=50,
            MessageAttributeNames=['All']
        )

        # Check if any messages were received
        if 'Messages' in response:
            for message in response['Messages']:

                # Save file to local for processing
                file_name = message['Body']
                encoded_data = message['MessageAttributes']['data']['BinaryValue']
                path = os.path.join(sqs_images_dir_path, file_name)
                with open(path, "wb") as binary_file:
                    binary_file.write(encoded_data)

                # Delete the message from the queue
                sqs.delete_message(
                    QueueUrl=input_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )

                # Process the image, fetch result:
                print("Running image_classification.py. Processing the image")
                script_path = 'image_classification.py'
                result = subprocess.run(['python', os.path.join(base_path, script_path), os.path.join(sqs_images_dir_path, file_name)], stdout=subprocess.PIPE)
                file_name, image_result = result.stdout.decode('utf-8').split(',')

                if os.path.exists(os.path.join(sqs_images_dir_path, file_name)):
                    os.remove(os.path.join(sqs_images_dir_path, file_name))
                    print(f"File '{os.path.join(sqs_images_dir_path, file_name)}' has been deleted.")
                else:
                    print(f"File '{os.path.join(sqs_images_dir_path, file_name)}' does not exist.")

                # Upload results to S3
                key_val_string = f'{file_name}: {image_result}'
                print(key_val_string)
                data = bytes(key_val_string, 'utf-8')
                s3_op_response = s3.put_object(Bucket=AWS_STORAGE_OUTPUT_BUCKET_NAME, Key=file_name,
                                               Body=data)
                if s3_op_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print("Results uploaded successfully to {}/{}".format(AWS_STORAGE_OUTPUT_BUCKET_NAME,
                                                                          file_name))
                else:
                    print(
                        "Error uploading results to S3 bucket {}/{}.".format(AWS_STORAGE_OUTPUT_BUCKET_NAME,
                                                                             file_name))

                # Publish the results to SQS
                sqs_response = sqs.send_message(
                    QueueUrl=output_queue_url,
                    MessageBody=f'{file_name}',
                    MessageAttributes={
                        'data': {
                            'DataType': 'String',
                            'StringValue': image_result
                        }
                    }
                )
                if sqs_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(f"Message sent with ID: {sqs_response['MessageId']} to Output queue")
                else:
                    print("Failed to send message to Output queue")


if __name__ == '__main__':
    poll_incoming_messages()
    app.run()


EOF
)

echo "Starting script." >> /home/ubuntu/logs.txt

# Run the Python script
python3 -c "${python_script}" | tee -a "/home/ubuntu/logs.txt"
