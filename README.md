
Hey there! Welcome to the Moon and Crashes project. 🌙🚗

Ever wondered if the moon's phases have any influence on car accidents? This project dives into that intriguing question by analyzing crash data alongside lunar phases.

What's Inside?
Some description of ETL Process:
- Extract: I pull crash data from public APIs(https://data.cityofnewyork.us) and enrich it with moon phase information(using pylib ephem).
- Transform: Using python,clean and merge these datasets to prepare for loading and later - analysis. Also connect Airflow's DAG files.
- Loading: Load merged datasets to postgresql.