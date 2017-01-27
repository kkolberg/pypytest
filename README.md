# pypytest

for osx do

1.

    ```sh
    brew install pypy
    pip install virtualenv
    brew install yajl
    pip install yajl-py==2.0.2
    ```

2. clone

3. 

    ```sh
    cd <git folder>
    mkdir temp
    which pypy
    virtualenv -p <path to pypy> .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```