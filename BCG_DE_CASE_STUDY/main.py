from pyspark.sql import SparkSession

from Code.utils import read_yaml
from Code.Analysis import Analysis

if __name__ == "__main__":
    # Initialize spark session
    spark = SparkSession.builder.appName("CarCrashAnalysis").getOrCreate()

    config_file_name = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    config = read_yaml(config_file_name)
    output_file_paths = config.get("OUTPUT_PATH")
    file_format = config.get("FILE_FORMAT")

    cca = Analysis(spark, config)

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print(
        "Q 1:",
        cca.male_crashes(output_file_paths.get(1), file_format.get("Output"))
    )

    # 2. How many two wheelers are booked for crashes?
    print(
        "Q 2:",
        cca.two_wheeler_crashes(
            output_file_paths.get(2), file_format.get("Output")
        ),
    )

    # 3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died
    # and Airbags did not deploy
    print(
        "Q 3:",
        cca.vehicles_deadly_crashes(
            output_file_paths.get(3), file_format.get("Output")
        ),
    )

    # 4. Determine the number of Vehicles with a driver having valid licences involved in hit and run?
    print(
        "Q 4:",
        cca.count_hit_and_runs(
            output_file_paths.get(4), file_format.get("Output")
        ),
    )

    # 5. Which state has the highest number of accidents in which females are not involved?
    print(
        "Q 5:",
        cca.state_with_no_female_accident(
            output_file_paths.get(5), file_format.get("Output")
        ),
    )

    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(
        "Q 6:",
        cca.top_vehicle_id_by_injuries(
            output_file_paths.get(6), file_format.get("Output")
        ),
    )

    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("Q 7:")
    cca.top_ethnic_by_body_style(
        output_file_paths.get(7), file_format.get("Output")
    ).show(truncate=False)

    # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "Q 8:",
        cca.zip_codes_alcohol(
            output_file_paths.get(8), file_format.get("Output")
        ),
    )

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
    # 4 and car avails Insurance
    print(
        "Q 9:",
        cca.count_crashes_with_no_damage(
            output_file_paths.get(9), file_format.get("Output")
        ),
    )

    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
    # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offenses (to be deduced from the data)
    print(
        "Q 10:",
        cca.top_5_vehicle_speeding_offenses(
            output_file_paths.get(10), file_format.get("Output")
        ),
    )

    spark.stop()