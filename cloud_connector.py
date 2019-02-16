#!/usr/bin/python3
import boto3
import time
sqsr = boto3.resource('sqs')
sqsc = boto3.client('sqs')


class AWSCloudSender:
    def __init__(self, outgoing_queue):
        self.queue = sqsr.get_queue_by_name(QueueName=outgoing_queue)

    def send_to_cloud(self, img):
        self.queue.send_message(MessageBody=img, MessageGroupId='main')
        # will be useful to add check that the sending actually went well,
        # id not - retry mechanism + some notification (SNS)
        return True


# this class should be ran in a seperate thread, it polls the result queue and handles the
# incoming msgs using handle_func.
# assumes each product has a UUID passed to the constructor as 'serialNumber'
class AWSCloudReciever:
    def __init__(self, serialNumber, handle_func, sleep_timer, debug=False):
        self.queue = sqsr.get_queue_by_name(
            QueueName='results'+str(serialNumber)+'.fifo').url
        while True:
            receipt_handle, msg = self.recieve_from_cloud(sleep_timer)
            handle_func(msg)
            sqsc.delete_message(
                QueueUrl=self.queue,
                ReceiptHandle=receipt_handle)
            if debug:
                return None

    def recieve_from_cloud(self, sleep_timer):
        while True:
            response = sqsc.receive_message(
                QueueUrl=self.queue, MaxNumberOfMessages=1)
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            if message:
                return receipt_handle, message
            time.sleep(1)


def example():
        # this will not work without existing queue in your AWS account named: results123.fifo
        # with ContentBasedDeduplication enabled
    img = "type safety is not the strongest in python ;-), so lets assume this is a img"
    # for the ease of example, both cloud input queue and result queue are the same.
    serial = 123
    sender_queue_name = 'results123.fifo'
    sender = AWSCloudSender(sender_queue_name)
    print("sending img to cloud, img=", img)
    sender.send_to_cloud(img)
    # recieve - note this is not the proper use (should be in seperate thread...)
    print("now lets hope we receive same from the cloud")
    AWSCloudReciever(serial, print, 1, True)


if __name__ == "__main__":
    example()
