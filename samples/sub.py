# coding:utf-8
import logging
import asyncio
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib/site-packages")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_1, QOS_2, QOS_0


@asyncio.coroutine
def uptime_coro():
    C = MQTTClient()
    # yield from C.connect('mqtt://test.mosquitto.org/')
    # yield from C.connect('mqtt://broker.emqx.io/') # 34.215.212.114
    # yield from C.connect('mqtt://172.29.17.3/')
    future = yield from C.connect('mqtt://user:test@127.0.0.1/')
    yield from C.subscribe([
        # ('#', QOS_0),
        ('topic_taboo', QOS_1),
        ('data#', QOS_0),
        ('data#', QOS_1),
        ('repositories#', QOS_1),
        ('calendar#', QOS_1),
        ('AAAA#', QOS_2),
    ])
    try:
        for i in range(1, 1000000):
            print("lai le " + str(i))
            message = yield from C.deliver_message()
            # print("lai le " + str(i))
            try:
                packet = message.publish_packet
                print(packet.variable_header.topic_name)
                print(packet.payload.data)
                # print(str(i) + ":" + packet.variable_header.topic_name +  "=>" + packet.payload.data)
            except Exception as ex:
                print(ex)
                pass
        # yield from C.unsubscribe(['$SYS/broker/uptime', '$SYS/broker/load/#'])
        # yield from C.disconnect()
    except ClientException as ce:
        logging.error("Client exception: %s" % ce)


if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(uptime_coro())