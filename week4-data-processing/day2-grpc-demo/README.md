# Steps for this exercise

1. Install requirements e.g. in venv

```bash
python -m venv .venv
source ./.venv/bin/activate
pip install -r requirements.txt
```

2. Execute

`python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. hello.proto`

3. Run server.py, then client.py in seperate terminals


# Exercise adding new service

Add a separate HelloShout service using Shout method instead of Say method to see differences

