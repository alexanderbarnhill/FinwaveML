import sys
import os
from azure.messaging.webpubsubservice import WebPubSubServiceClient

from utilities.data_descriptor import singleton
import logging as log
import asyncio
import sys
import websockets



@singleton
class PubSubClient:
    def __init__(self):
        self.connection_string = os.getenv('AZ_WEB_PUB_SUB_CONNECTION_STRING')
        self.hub_name = os.getenv('AZ_WEB_PUB_SUB_HUB_ML')
        self.service = WebPubSubServiceClient.from_connection_string(connection_string=self.connection_string, hub=self.hub_name)

    def send_message(self, message):
        self.service.send_to_all(message)


@singleton
class PubSubClientSubscriber:
    def __init__(self):
        self.connection_string = os.getenv('AZ_WEB_PUB_SUB_CONNECTION_STRING')
        self.hub_name = os.getenv('AZ_WEB_PUB_SUB_HUB_ML')
        self.service = WebPubSubServiceClient.from_connection_string(connection_string=self.connection_string,
                                                                     hub=self.hub_name)
        token = self.service.get_client_access_token()

        try:
            asyncio.get_event_loop().run_until_complete(self.connect(token['url']))
        except KeyboardInterrupt:
            pass

    async def connect(self, url):
        async with websockets.connect(url) as ws:
            print('connected')
            while True:
                print('Received message: ' + await ws.recv())


PubSubClient()