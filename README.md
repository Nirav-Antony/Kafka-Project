# BD2_220_227_230_275


## How to run
This implementation supports communication between **N** producers and **N** consumers through the middleware, using different **topics** .

Topic used in this implementation:
* temp
* msg

Run **broker.py**:
```console
$ python3 broker.py
````
in another terminal 
Run **producer.py**:
```console
$ python3 producer.py [--type TYPE] [--length LENGTH]  [-f FILENAME]
```
in a third terminal
Run **consumers.py**:
```console
$ python3 consumers.py [--type TYPE] 
```
