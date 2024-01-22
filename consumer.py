import sys
import argparse
import middleware
import time

class Consumer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = middleware.PickleQueue(f"/{self.type}")
        

    @classmethod
    def datatypes(self):
        return ["temp", "msg"]    

    def run(self, length=10):
        while True:
            topic, data = self.queue.pull()
            print('topic is',topic,'data is', data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg]", default="temp")
    args = parser.parse_args()

    if args.type not in Consumer.datatypes():
        print("Error: topic does not exist")
        sys.exit(1)

    p = Consumer(args.type)

    p.run()