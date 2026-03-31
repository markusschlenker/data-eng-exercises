import grpc
from concurrent import futures
import hello_pb2
import hello_pb2_grpc

class HelloService(hello_pb2_grpc.HelloServicer):
    def Say(self, request, context):
        return hello_pb2.Greeting(message=f"Hello {request.name}")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    hello_pb2_grpc.add_HelloServicer_to_server(HelloService(), server)
    server.add_insecure_port("0.0.0.0:50051")
    server.start()
    print("Server running on 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
