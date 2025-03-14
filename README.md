# 📊 kstats
 
`kstats` is a collection of scripts (including `pullMSKStats.py`) designed to pull raw usage data from MSK.
 
## 🚀 Installation and Setup
 
### 1️⃣ Clone the Repository
```bash
git clone https://github.com/oisraeli/kstats
cd kstats
```
 
### 2️⃣ Set Up a Virtual Environment
```bash
mkdir .env
virtualenv .env
source .env/bin/activate
```
 
### 3️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```
 
### 4️⃣ Configure the Script
Copy the example configuration file and update it as needed:
```bash
cp config.cfg.example config.cfg
```
 
### 5️⃣ Run the Script
Execute the script with the configuration file:
```bash
python pullMSKStats.py -c config.cfg
```
 
### 6️⃣ Deactivate the Virtual Environment (When Finished)
```bash
deactivate
```
 
## 🔮 Future Plans
- 🐳 Docker support (coming soon)
