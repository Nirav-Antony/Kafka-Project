import selectors
import socket
import json
import pickle
import time

dicConsumers = {}   #dictionary stores pairs of {conn, topic}
content = {}        #stores only the last message that was published
protocolCon = {}


sel = selectors.DefaultSelector()

def accept(sock, mask):
    conn, addr = sock.accept() # Should be ready
    
    print('accepted', conn, 'from', addr)
    data = conn.recv(1000)

                     #receives the record message from the producer/consumer
    protocol, Midtype, topic = AckDecode(data)
    topics = []
    if(topic.count('/') > 1):
        topics = topic[1:].split('/')
    else:
        topics.append(topic[1:])
    
                    #associates the corresponding protocol to its conn
    protocolCon[conn] = protocol

    #se for consumer
    if(Midtype == 'MiddlewareType.CONSUMER'):
        dicConsumers[conn] = topics[len(topics)-1]          #associates the subscribed topic to its conn
        topico = topics[len(topics)-1]
        if(topico in content and content[topico] != None):
            message = content[topico]
            #protocolCon[conn] == 'ProtocolType.Pickle':
            pickleT = pickleEncode(topico, message, '')
            conn.send(pickleT)

        print('SUB FROM ', conn, ' TO TOPIC :', topic)

    sel.register(conn, selectors.EVENT_READ, read)



def read(conn, mask):
    data = conn.recv(1000)

    if data:
                        #according to the producer protocol, decode the message
        method, topic, message = pickleDecode(data)

        topics = []
        if(topic.count('/') > 1):
            topics = topic[1:].split('/')
        else:
            topics.append(topic[1:])
        
      #in the case of publication, it is necessary to send the message to the corresponding consumers
        if method == 'PUB':
            #save the message in the corresponding topic
            for t in topics:
                content[t] = message
            #PUBLISH message in a thread
            print('PUB IN TOPIC: ', topic, ' THE MESSAGE: ', message)
            #travels through consumers
            for c in dicConsumers:
                #scroll through parent and child topics
                for t in topics:
                    #in case there is a topic match
                    if dicConsumers[c] == t:
                        #according to the consumer's protocol, it encodes the message and sends it to the consumer
                        pickleT = pickleEncode(topic, message, method)
                        c.send(pickleT)
        
        #in case the method is for listing topics
        if method == 'LIST':
            method = "list the topics: \n"
            #in case there are topics
            if(len(content) > 0):
                message = []
                #add existing topics
                for topic in content:
                    message.append(topic)
                #according to the consumer protocol, it encodes and sends the list of topics
                pickleT = pickleEncode(topic, message, method)
                conn.send(pickleT)
            #in case there are no topics
            else:
                message = "no topics!"
                pickleT = pickleEncode(topic, message, method)
                conn.send(pickleT)
        #in case the method is for the cancellation of the subscription
    #when the producer or consumer disconnects
    else:
        print('closing', conn)
        if(conn in dicConsumers):
            del dicConsumers[conn]
            print('CANCELED SUB BY: ', conn)
        del protocolCon[conn] 
        sel.unregister(conn)
        conn.close()


#encode in Pickle
def pickleEncode(topic, message, method):
    pickleText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
    pickleText = pickle.dumps(pickleText)
    return pickleText

#decode in Pickle
def pickleDecode(content):
    pickleText = pickle.loads(content)
    method = pickleText['METHOD']
    topic = pickleText['TOPIC']
    message = pickleText['MESSAGE_CONTENT']
    return method, topic, message

#decode message
def AckDecode(data):
    data = data.decode('utf-8')
    jsonText = json.loads(data)
    protocol = jsonText['PROTOCOL']
    Midtype = jsonText['TYPE']
    topic = jsonText['TOPIC']
    return protocol, Midtype, topic

sock = socket.socket()
sock.bind(('', 8000))
sock.listen(100)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)