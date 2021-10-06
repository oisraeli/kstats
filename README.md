kstats
=====

`kstats` is the collective name of scripts (`pullMSKStats.py`) which will pull raw usage data from MSK.

## `pullMSKStats.py`

### Copy config file and edit
```
$ cp config.cfg.example config.cfg
```

### Run locally (docker in the near future)
```
# Clone:
git clone https://github.com/oisraeli/kstats

# Prepare virtualenv:
cd ecstats
mkdir .env
virtualenv .env

# Activate virtualenv
. .env/bin/activate

# Install necessary libraries
pip install -r requirements.txt

# When finished
deactivate
```

To run the script copy the configuration file and edit:

```
cp config.cfg.example config.cfg
```

Execute 

```
python pullMSKStats.py -c config.cfg
```
