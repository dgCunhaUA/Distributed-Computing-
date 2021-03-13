from enum import Enum
#imports
import socket
import json
import sys      #
import fcntl    #
import os       #
import selectors
import re       #
import pickle


#import xml

#from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element
import xml.etree.ElementTree as etree
import xml.etree.ElementTree as ET


#########

HOST = 'localhost'      # Address of the host running the server  
PORT = 8000             # The same port as used by the server
sel = selectors.DefaultSelector()   #TODO desnecessario?

consumer_request = {}

class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class Queue:
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        self.topic = topic
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((HOST, PORT))     # connect to server
        self.type = type.value
        #sel.register(self.s, selectors.EVENT_READ, read)

    def push(self, value):
        """ Sends data to broker. """
        print(self.topic, value) #TODO remover

        pass

    def pull(self):  
        """ Receives (topic, data) from broker.
            Should block the consumer!"""
   

class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)
        

        #Envia para o broker a fazer sub a topico
        if (self.type == 1):
            consumer_request["op"] = "sub"
            consumer_request["topic"] = self.topic
            consumer_request["serializacao"] = "JSON"
            print(consumer_request)
            json_consumer_request = json.dumps(consumer_request)
            self.s.sendall(json_consumer_request.encode('utf-8'))

    def push(self, value):
        super().push(value)
        """ Sends data to broker. """
        json_value = json.dumps({"op": "pub", "topic": self.topic,"value": value, "serializacao": "JSON"})
        self.s.sendall(json_value.encode('utf-8'))
        #print(self.topic, value) #TODO remover

        pass

    def pull(self):  
        """ Receives (topic, data) from broker.
            Should block the consumer!"""
        
        data = self.s.recv(1024)
        if data:
            info = json.loads(data.decode()) 
            print("DATA FROM BROKER : ", info)
            
            return info.get("topic"), info.get("value")
        pass

class XMLQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)

        #Envia para o broker a fazer sub a topico
        if (self.type == 1):
            xml_msg = ET.Element("message")
            xml_op = ET.SubElement(xml_msg, "op")
            xml_topic = ET.SubElement(xml_msg, "topic")
            xml_type = ET.SubElement(xml_msg, "type")
            xml_serializacao = ET.SubElement(xml_msg, "serializacao")

            xml_op.text = "sub"
            xml_topic.text = self.topic
            xml_type.text = str(type)
            xml_serializacao.text = "XML"

            xml_send = ET.tostring(xml_msg, encoding='utf-8')
            self.s.sendall(xml_send)   




    def push(self, value):
        super().push(value)
        """ Sends data to broker. """
        #xml_value = Element("producer_info", op="pub", topic=self.topic, value=value, serializacao="XML")
        #xml_send = ET.tostring(xml_value, encoding="utf-8")
        xml_msg = ET.Element("message")
        xml_op = ET.SubElement(xml_msg, "op")
        xml_topic = ET.SubElement(xml_msg, "topic")
        xml_value = ET.SubElement(xml_msg, "value")
        xml_serializacao = ET.SubElement(xml_msg, "serializacao")

        xml_op.text = "pub"
        xml_topic.text = self.topic
        xml_value.text = str(value)
        xml_serializacao.text = "XML"

        xml_send = ET.tostring(xml_msg, encoding='utf-8')

        self.s.sendall(xml_send)   
        pass

    def pull(self):  
        """ Receives (topic, data) from broker.
            Should block the consumer!"""
        
        data = self.s.recv(1024)
        if data:
            info = ET.fromstring(data)
            info = { info[0].tag : info[0].text, info[1].tag : info[1].text}
            #print(info)
            
            return info.get("topic"), info.get("value")
        pass

class PickleQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER):
        super().__init__(topic, type)

        #Envia para o broker a fazer sub a topico
        if (self.type == 1):
            consumer_request["op"] = "sub"
            consumer_request["topic"] = self.topic
            consumer_request["serializacao"] = "Pickle"
            print(consumer_request)
            pickle_consumer_request = pickle.dumps(consumer_request)
            self.s.sendall(pickle_consumer_request)     #nao se pode dar enconde?

    def push(self, value):
        super().push(value)
        """ Sends data to broker. """
        pickle_value = pickle.dumps({"op": "pub", "topic": self.topic,"value": value, "serializacao": "Pickle"})
        self.s.sendall(pickle_value)        #enconde?
        #print(self.topic, value) #TODO remover

        pass

    def pull(self):  
        """ Receives (topic, data) from broker.
            Should block the consumer!"""
        
        data = self.s.recv(1024)
        if data:
            info = pickle.loads(data) 
            
            return info.get("topic"), info.get("value")
        pass