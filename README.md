# pypytest

for osx do

1. os stuff

    ```sh
    brew install pypy
    pip install virtualenv
    brew install yajl
    pip install yajl-py==2.0.2
    pip install cffi
    ```

2. clone

3. project stuff

    ```sh
    cd <git folder>
    mkdir temp
    which pypy
    virtualenv -p <path to pypy> .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
4. vscode make workspace settings has "python.pythonPath": "${workspaceRoot}/.venv/bin/pypy"
