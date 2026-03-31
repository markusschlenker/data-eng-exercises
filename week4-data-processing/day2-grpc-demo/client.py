import grpc
import hello_pb2
import hello_pb2_grpc

def run():
    channel = grpc.insecure_channel("localhost:50051")
    stub = hello_pb2_grpc.HelloStub(channel)

    response = stub.Say(hello_pb2.Name(name="Alice"))
    print(response.message)

if __name__ == "__main__":
    run()
