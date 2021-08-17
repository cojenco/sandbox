# sandbox

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Set Environment Variables

```bash
# This is already the default, so only needs set if using a different port
export STORAGE_EMULATOR_HOST=http://localhost:9000
```


## Start the Emulator

```bash
gunicorn --bind "localhost:9000" --worker-class sync --threads 10 --reload --access-logfile - --chdir ./google/cloud/storage/emulator "emulator:run()"
```

## Run Test Retries Locally

```bash
python test_retries.py 
```
