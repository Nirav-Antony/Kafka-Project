import sys
import argparse
import middleware
import random
import time


class Producer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = [middleware.PickleQueue(
            f"/{self.type}", middleware.MiddlewareType.PRODUCER)]
        if datatype == "temp" or datatype == "msg":
            self.gen = self._msg

    @classmethod
    def datatypes(self):
        return ["temp", "msg"]

    def _msg(self):
        time.sleep(0.2)
        d = []
        for i in l:
            #print(i.split(' '))
            d.extend(i.split(' '))
        yield d

    def run(self, length=10):
        for _ in range(length):
            for queue, value in zip(self.queue, self.gen()):
                queue.push(value)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--type", help="type of producer: [temp, msg]", default="temp")
    parser.add_argument(
        "--length", help="number of messages to be sent", default=10)
    parser.add_argument("-f", "--inputfile1",
                        type=argparse.FileType('r'), help="name of file")
    args = parser.parse_args()

    inputfileone = args.inputfile1
    l = []
    for i in inputfileone:
        if i.strip() != 'EOF':
            b = i.strip().split("\n")
            l.extend(i for i in b)
            print(f'Input : {b}')
    # print(l)

    if args.type not in Producer.datatypes():
        print("Error: topic does not exist")
        sys.exit(1)

    p = Producer(args.type)

    p.run(int(args.length))
