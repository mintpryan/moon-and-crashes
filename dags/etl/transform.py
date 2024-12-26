import pandas as pd
import datetime


def transform_data(crash_file, moon_file,output_file):
    # Загрузка данных
    crash_data = pd.read_csv(crash_file)
    moon_data = pd.read_csv(moon_file)

    # Приведение дат к одному формату
    crash_data["crash_date"] = pd.to_datetime(crash_data["crash_date"])
    moon_data["date"] = pd.to_datetime(moon_data["date"])
    daily_summary = (
        crash_data
        .groupby("crash_date")
        .agg(
            total_accidents=("crash_date", "count"),
            total_injured=("number_of_persons_injured", "sum"),
            total_killed=("number_of_persons_killed", "sum")
        )
        .reset_index()
    )

    # Объединение данных по дате
    merged = pd.merge(daily_summary, moon_data,
                      left_on="crash_date", right_on="date", how="left")

    # Классификация фаз Луны
    def classify_phase(phase):
        if phase <= 7:
            return "New Moon"
        elif 7 < phase <= 22:
            return "First Quarter"
        elif 22 < phase <= 29:
            return "Full Moon"
        else:
            return "Last Quarter"

    merged["moon_phase_category"] = merged["moon_phase"].apply(classify_phase)

    # Сохранение в CSV
    merged.to_csv(output_file, index=False)
