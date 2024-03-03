from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from Code.utils import csv_to_df, df_to_csv


class Analysis:
    def __init__(self, spark, config):
# LOAD RAW DATA FROM SOURCE

        input_file_paths = config.get("INPUT_FILENAME")
        self.df_charges = csv_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = csv_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = csv_to_df(spark, input_file_paths.get("Endorse"))
        self.df_restrict = csv_to_df(spark, input_file_paths.get("Restrict"))
        self.df_primary_person = csv_to_df(spark, input_file_paths.get("Primary_Person"))
        self.df_units = csv_to_df(spark, input_file_paths.get("Units"))

    def male_crashes(self, output_path, output_format):
        
        df = self.df_primary_person.filter(
            self.df_primary_person.PRSN_GNDR_ID == "MALE"
        ).filter(self.df_primary_person.DEATH_CNT > 2)
        df_to_csv(df, output_path, output_format)
        return df.count()

    def two_wheeler_crashes(self, output_path, output_format):
       
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")).select('CRASH_ID').distinct()
        
        df_to_csv(df, output_path, output_format)

        return df.count()

    def vehicles_deadly_crashes(self, output_path, output_format):
        
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .filter(
                (col("PRSN_INJRY_SEV_ID") == "KILLED")
                & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                & (col("VEH_MAKE_ID") != "NA")
            )
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        df_to_csv(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def count_hit_and_runs(self, output_path, output_format):
        
        df = (
            self.df_units.select("CRASH_ID", "VEH_HNR_FL")
            .join(
                self.df_primary_person.select("CRASH_ID", "DRVR_LIC_TYPE_ID"),
                on=["CRASH_ID"],
                how="inner")
            .filter(col("VEH_HNR_FL") == "Y")
            .filter(col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            .select('CRASH_ID').distinct()
            )

        df_to_csv(df, output_path, output_format)

        return df.count()

    def state_with_no_female_accident(self, output_path, output_format):
        
        df = (
            self.df_primary_person.filter(
                self.df_primary_person.PRSN_GNDR_ID != "FEMALE"
            )
            .groupby("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        df_to_csv(df, output_path, output_format)

        return df.first().DRVR_LIC_STATE_ID

    def top_vehicle_id_by_injuries(self, output_path, output_format):
        
        df = (
            self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA")
            .withColumn("TOT_CASUALTIES_CNT", self.df_units['TOT_INJRY_CNT'] + self.df_units['DEATH_CNT'])
            .groupby("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
            .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        )

        df_top_3_to_5 = df.limit(5).subtract(df.limit(2)).select("VEH_MAKE_ID")
        df_to_csv(df_top_3_to_5, output_path, output_format)

        return [veh[0] for veh in df_top_3_to_5.collect()]

    def top_ethnic_by_body_style(self, output_path, output_format):
        
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.df_units.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                )
            )
            .filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )

        df_to_csv(df, output_path, output_format)

        return df

    def zip_codes_alcohol(self, output_path, output_format):
        
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .select('DRVR_ZIP')
            .limit(5)
        )
        df_to_csv(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def count_crashes_with_no_damage(self, output_path, output_format):
        
        df = (
            self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(((self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                    & (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
                | ((self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                    & (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
            )
            .filter(self.df_damages.DAMAGED_PROPERTY == "NONE")
            .filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
            .join(self.df_charges, on=["CRASH_ID"], how = "left")
            .filter(~col("CHARGE").contains("INSURANCE"))
            .select('CRASH_ID').distinct()
        )
        df_to_csv(df, output_path, output_format)

        return df.count()

    def top_5_vehicle_speeding_offenses(self, output_path, output_format):
        
        top_25_state_list = [
            row[0]
            for row in self.df_units.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(
                self.df_primary_person.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        df_to_csv(df, output_path, output_format)

        return [row[0] for row in df.collect()]