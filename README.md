# NFL Player Stats & Props ETL

This project automates the extraction, transformation, and loading (ETL) of NFL player statistics, schedules, team info, and betting lines into a PostgreSQL database.  
It is designed to support analytics and visualizations in tools like **Power BI** for betting-related dashboards.

---

## 📋 Features
- Pulls **NFL player stats** from the `nfl_data_py` package.
- Fetches **Vegas betting lines** (DraftKings, FanDuel, Fanatics) from [TheOdds API](https://the-odds-api.com/).
- Cleans and normalizes player, team, and timeslot data into **fact** and **dimension** tables.
- Handles **historical seasons** and **daily updates**.
- Writes results to a PostgreSQL database.
- Logging to both console and file for troubleshooting.

---

## 📂 Project Structure
nfl_predict/  
├── main.py        # Main entry point for running the ETL  
├── db.py          # Database connection & schema helpers  
├── odds.py        # Functions to fetch and process betting lines  
├── nfl_utils.py   # NFL data API helpers  
├── logs/          # Log files  
├── .venv/         # Virtual environment (not tracked in Git)  
└── README.md      # This file  

---

## ⚙️ Requirements
- **Python 3.12+**
- PostgreSQL database (with schema `nfl`)
- API key for [TheOdds API](https://the-odds-api.com/)
- Dependencies listed in `requirements.txt`

---

## 📦 Installation
```bash
# Clone the repo
git clone https://github.com/your-username/nfl_predict.git
cd nfl_predict

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## 🔑 Environment Variables
Create a `.env` file or set environment variables:  
```
POSTGRES_URL=postgresql+psycopg2://user:password@host:5432/dbname
ODDS_API_KEY=your_api_key_here
```

---

## ▶️ Usage

### Run for all seasons (historical load)
```bash
python main.py --start-year 2015 --end-year 2024 --replace
```

### Run for last 4 weeks (daily update)
```bash
python main.py --start-year 2024 --weeks-back 4
```

---

## 📊 Power BI Integration
Once the ETL has run and populated the database, you can connect Power BI directly to PostgreSQL:
1. **Get Data** → PostgreSQL
2. Enter the connection string from `POSTGRES_URL`.
3. Select `nfl` schema tables (`fact_player_timeslot`, `fact_player_prop_lines`, etc.).
4. Build visuals such as:
   - Player performance trends
   - Betting line comparisons by sportsbook
   - Historical performance vs. betting odds

---

## 🐛 Troubleshooting
- **Empty fact tables**: Ensure that your API key is valid and your start/end years have data.
- **Schema mismatches**: Run the database migration script before loading data.
- **Rate limits**: TheOdds API has rate limits — space out API calls or upgrade your plan.

---

## 📜 License
This project is released under the MIT License.
