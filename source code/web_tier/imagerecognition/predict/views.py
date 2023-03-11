from django.http import HttpResponse
from uuid import uuid4
from pathlib import Path
from django.core.files import File
import json
from django.views.decorators.csrf import csrf_exempt
from PIL import Image
from io import BytesIO
from predict.services.sqs_handler import SQSHandler

import io

@csrf_exempt
def index(request):
    path = Path(__file__).resolve().parent.parent.parent

    file = request.FILES['myfile']

    image = Image.open(file)

    with BytesIO() as binary_stream:
        image.save(binary_stream, format='JPEG')
        binary_data = binary_stream.getvalue()

    sqs = SQSHandler('image_processing_queue')
    # print(binary_data)

    sqs.create_message(file.name, {'data': {'BinaryValue': binary_data, 'DataType': 'Binary'}})

    with open("/Users/vishnuvardhan/Desktop/Spring-2023/CSE-546/project-1/CSE546_Sum22_workload_generator/vishnu1.JPEG","wb") as binary_file:
        # Write bytes to file
        binary_file.write(binary_data)

    sqs = SQSHandler('output_queue')

    while True:
        messages = sqs.receive_message(visibility_timeout=1, max_no_of_messages=10)['Messages']
        message = [msg for msg in messages if msg['Body'] == file.name]
        if message:
            sqs.delete_message(message[0]['ReceiptHandle'])
            yy = message[0]['MessageAttributes']['data']['StringValue']

            return HttpResponse(yy)
