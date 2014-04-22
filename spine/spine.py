#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import socket
from collections import namedtuple

from kombu import Connection, Exchange, Queue
from kombu.mixins import Consumer
from kombu.common import maybe_declare



# ---- 8< ---- 8< ---- 8< ----
# message.py

MessageType = namedtuple("MessageType", "message command query")
msg_type = MessageType(message=1, command=2, query=3)

class Message(object):
    def __init__(self, *args, **kwargs):
        super(Message, self).__init__()
        self.type = kwargs.get("type", msg_type.message)
        self.dest_list = kwargs.get("dest", [])
        self.body = kwargs.get("body", {})
        self.amqp_msg = kwargs.get("amqp_msg", None)
    
    def ack(self):
        if self.amqp_msg:
            self.amqp_msg.ack()
    
    def to_dict(self):
        return {"type": self.type,
                "dest_list": self.dest_list,
                "body": self.body
                }
                
class Command(Message):
    def __init__(self, *args, **kwargs):
        kwargs['type'] = msg_type.command
        super(Command, self).__init__(*args, **kwargs)
        
class Query(Message):
    def __init__(self, *args, **kwargs):
        kwargs['type'] = msg_type.query
        super(Command, self).__init__(*args, **kwargs)

# ---- 8< ---- 8< ---- 8< ----
# generic_component.py

class GenericComponent(object):
    
    def __init__(self, *args, **kwargs):
        super(GenericComponent, self).__init__()
        self.log = logging.getLogger("spine")
        self.name = kwargs.get("name", None)
        self.broker_url = kwargs.get("broker_url", None)
        self.log.debug("Name: %s" % self.name)
        self.log.debug("broker_url: %s" % self.broker_url)
        self.continue_to_drain = True
        
        self.msg_mapping = {msg_type.message : self._on_message,
                            msg_type.command: self._on_command,
                            msg_type.query : self._on_query
                            }

    def send(self, message):
        if self.conn:
            producer = conn.Producer(serializer='json')
            for dest in message.dest_list:
                producer.publish(message.to_dict(), exchange=self.ex, routing_key=dest)
    
    def on_receive(self, body, message):
        try:
            m = Message(dest=body['dest_list'], body=body['body'], type=body['type'], amqp_msg=message)
            self.log.debug("Received message: %s", m.body )
            self.msg_mapping[m.type](m)
        except KeyError as e:
            self.log.debug( e )
            pass
        
    def _on_message(self, message):
        pass
        
    def _on_command(self, message):
        if message.body == 'quit':
            self.log.info("Received 'QUIT'. Quitting.")
            self.stop()
        else:
            self.on_command(message)
    
    def _on_query(self, message):
        pass
            
    def start(self):
        from kombu.log import setup_logging 
        setup_logging() 
        with Connection(self.broker_url) as conn:
            self.log.info("Connected to %s" % self.broker_url)
            self.conn = conn
            self.rkey = ".".join(["components", self.name, "inbox"])
            self.ex = Exchange("components", 'topic', durable=True)
            self.q = Queue('inbox', exchange=self.ex, routing_key=self.rkey)
            
            maybe_declare( self.ex, conn )
            self.log.info("Declared Exchange: '%s' (%s)" % (self.ex.name, self.ex.type) )
            
            maybe_declare( self.q, conn )
            self.log.info("Declared Queue: %s with routing key: '%s' " % (self.q, self.rkey) )

            with Consumer(conn.channel(), queues=self.q, callbacks=[self.on_receive]) as c:
                while self.continue_to_drain:
                    try:
                        conn.drain_events()
                        c.consume()
                    except socket.timeout as e: 
                        pass
                        
    def stop(self):
        self.continue_to_drain = False

# ---- 8< ---- 8< ---- 8< ----
# cli_component.py

import cli.log
import uuid

class CLIComponent(cli.log.LoggingApp):
        
    def __init__(self, *args, **kwargs):
        super(CLIComponent, self).__init__(*args, **kwargs)
        cli.log.LoggingApp.__init__(self, 
                                    *args,
                                    main=self.main,
                                    message_format='[%(asctime)s] - %(levelname)8s - %(name)5s - %(filename)5s:%(lineno)3s - %(funcName)10s() - %(message)s',
                                    version='0.1',
                                    name='CLIComponent',
                                    root = True,
                                    **kwargs )

    def setup(self):
        super(CLIComponent, self).setup()
        self.add_param("-n", "--name", help="component name")
        self.add_param("-b", "--broker_url", help="broker url", default="amqp://guest:guest@localhost/components")
                
    def main(self):
        broker_url = self.params.broker_url
        name = self.params.name or str(uuid.uuid4())        
        logfile = self.params.logfile
        
        c=GenericComponent(name=name, broker_url=broker_url)
        try:
            c.start()
        except KeyboardInterrupt:
            self.log.info("Detected CTRL+C. Stopping Component")
            c.stop()
            self.log.info("Exit.")
        

if __name__ == '__main__':
    c = CLIComponent()
    c.run()
