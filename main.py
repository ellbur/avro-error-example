
from http.server import BaseHTTPRequestHandler, HTTPServer
import avro.ipc as ipc
import avro.protocol
import avro.schema as schema
import threading
from time import sleep

protocol = avro.protocol.parse('''
{
  "namespace": "a",
  "protocol": "b",
  "types": [
    {
      "name": "Foo",
      "type": "record",
      "fields": [
        {"name": "b", "type": "int"}
      ]
    }
  ],
  "messages": {
    "foo": {
      "request": [],
      "response": "Foo"
    },
    "bar": {
      "request": [],
      "response": "null"
    }
  }
}
''')

class Responder(ipc.Responder):
    def __init__(self):
        ipc.Responder.__init__(self, protocol)
        
    def invoke(self, method, args):
        if method.name == 'foo':
            return { 'a': 1 }
        else:
            raise schema.AvroException('unknown method', method.name)

responder = Responder()

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        request_reader = ipc.FramedReader(self.rfile)
        request = request_reader.read_framed_message()
        response_body = responder.respond(request)
        self.send_response(200)
        self.send_header('Content-Type', 'avro/binary')
        self.end_headers()
        response_writer = ipc.FramedWriter(self.wfile)
        response_writer.write_framed_message(response_body)
        
server = HTTPServer(('localhost', 8856), Handler)
server.allow_reuse_address = True

try:
    def server_routine():
        server.serve_forever()
        
    server_thread = threading.Thread(target = server_routine)
    server_thread.start()
    
    sleep(1.0)
    
    client = ipc.HTTPTransceiver('localhost', 8856)
    requestor = ipc.Requestor(protocol, client)
    
    # This type of AvroRemoteException is handled correctly:
    try:
        print('<', requestor.request('bar', {}))
   except ipc.AvroRemoteException as e:
        print('Error: %s' % (e,))
        
    # This type is not:
    try:
        print('<', requestor.request('foo', {}))
    except ipc.AvroRemoteException as e:
        print('Error: %s' % (e,))
finally:
    server.shutdown()

