# Echo server program
import socket
import selectors
import json
import pickle


from xml.etree.ElementTree import Element
import xml.etree.ElementTree as etree
import xml.etree.ElementTree as ET

sel = selectors.DefaultSelector()
ultima_msg = {}
consumer_info = {}      #verificar



def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready   returns new socket and addr.
    print('accepted', conn, 'from', addr)
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def read(conn, mask):
    global ultima_msg
    data = conn.recv(1024)  # Should be ready
    
    if data:
        try:
            info = ET.fromstring(data)
            info = { info[0].tag : info[0].text, info[1].tag : info[1].text, info[2].tag : info[2].text, info[3].tag : info[3].text }
        except :
            pass 
        try:
            info = pickle.loads(data) 
        except :
            pass       
        try:
            info = json.loads(data.decode()) 
        except :
            pass
          

        if (info.get("op") == "sub"):            #se consumidor subescrever um topico
            consumer_info[conn] = [info.get("topic"), info.get("serializacao")]
            topico = info.get("topic")

            print("Consumer: ", consumer_info)

            print(ultima_msg)

            if consumer_info[conn][1] == "JSON":
                if (info.get("topic") in ultima_msg.keys() ):
                    msg_enviar = {"topic": topico, "value": ultima_msg[info.get("topic")], "serializacao": "JSON"  }
                    msg_enviar_json = json.dumps(msg_enviar)
                    conn.send(msg_enviar_json.encode('utf-8'))
            elif consumer_info[conn][1] == "Pickle":
                if (info.get("topic") in ultima_msg.keys() ):
                    msg_enviar = {"topic": topico, "value": ultima_msg[info.get("topic")], "serializacao": "Pickle" }
                    msg_enviar_pickle = pickle.dumps(msg_enviar)
                    conn.send(msg_enviar_pickle)   
            elif consumer_info[conn][1] == "XML":
                if (info.get("topic") in ultima_msg.keys()):
                    xml_consumer_request = ET.Element("message")
                    xml_topic = ET.SubElement(xml_consumer_request, "topic")
                    xml_value = ET.SubElement(xml_consumer_request, "value")
                    xml_serializacao = ET.SubElement(xml_consumer_request, "serializacao")

                    xml_topic.text = topico
                    xml_value.text = str(ultima_msg[info.get("topic")])
                    xml_serializacao.text = "XML"

                    xml_send = ET.tostring(xml_consumer_request, encoding='utf-8')

                    conn.sendall(xml_send)   

        elif (info.get("op") == "pub"):             #quando o produtor publica topicos
            print("DATA RECEIVED: ", info)
            #buscar topic e value e a sua serializacao ao dicionario info 
            topico = info.get("topic")
            valor = info.get("value")
            #serializacao = info.get("serializacao") NAO E PRESCISO?

            #adicionar ao dicionario que guarda a ultima mensagem
            ultima_msg[topico] = valor                      #PRECISO?
            msg_enviar = {"topic": topico, "value": valor }

            
            for consumer in consumer_info:
                if consumer_info[consumer][1] == "JSON":
                    if ( consumer_info[consumer][0] == topico):
                        msg_enviar_json = json.dumps(msg_enviar)
                        consumer.send(msg_enviar_json.encode('utf-8'))  
                elif consumer_info[consumer][1] == "Pickle":
                    if ( consumer_info[consumer][0] == topico):
                        msg_enviar_pickle = pickle.dumps(msg_enviar)
                        consumer.send(msg_enviar_pickle) 
                elif consumer_info[consumer][1] == "XML":
                    if ( consumer_info[consumer][0] == topico):
                        xml_consumer_request = ET.Element("message")
                        xml_topic = ET.SubElement(xml_consumer_request, "topic")
                        xml_value = ET.SubElement(xml_consumer_request, "value")

                        xml_topic.text = topico
                        xml_value.text = str(valor)

                        xml_send = ET.tostring(xml_consumer_request, encoding='utf-8')

                        consumer.sendall(xml_send)   
        


HOST = ''                 # Symbolic name meaning all available interfaces
PORT = 8000               # Arbitrary non-privileged port


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(100)
    s.setblocking(False)
    sel.register(s, selectors.EVENT_READ, accept)

    while True:
        events = sel.select()
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)



