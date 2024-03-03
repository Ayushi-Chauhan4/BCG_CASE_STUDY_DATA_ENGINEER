from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

from Code.utils import read_yaml, csv_to_df
from Code.Analysis import Analysis

import yaml

spark = SparkSession.builder.appName("CarCrashAnalysis").getOrCreate()

config_file_name = "config.yaml"
config = read_yaml(config_file_name)

output_file_paths = config.get("OUTPUT_PATH")
file_format = config.get("FILE_FORMAT")

input_file_paths = config.get("INPUT_FILENAME")

df_charges = csv_to_df(spark, input_file_paths.get("Charges"))
df_damages = csv_to_df(spark, input_file_paths.get("Damages"))
df_endorse = csv_to_df(spark, input_file_paths.get("Endorse"))
df_restrict = csv_to_df(spark, input_file_paths.get("Restrict"))
df_primary_person = csv_to_df(spark, input_file_paths.get("Primary_Person"))
df_units = csv_to_df(spark, input_file_paths.get("Units"))

df_charges.createOrReplaceTempView("charges")
df_damages.createOrReplaceTempView("damages")
df_endorse.createOrReplaceTempView("endorse")
df_primary_person.createOrReplaceTempView("pp")
df_units.createOrReplaceTempView("units")
df_restrict.createOrReplaceTempView("restrict")

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
#df_primary_person = csv_to_df(spark, input_file_paths.get("Primary_Person"))
# df_primary_person.printSchema()
# df1 = df_primary_person.filter((col("prsn_gndr_id") == "MALE") & (col("death_cnt") > 2)) \
#     .agg(countDistinct("crash_id").alias("more_than_2_male_dead"))
# print("1. ")
# df1.show()
# df1_sql = spark.sql("select count(distinct crash_id) from pp where prsn_gndr_id = 'MALE' and death_cnt >2")
# print("1. sql ")
# df1_sql.show()

    # 2. Find the number of crashes involving Motorcycles
# df2 = df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")).select('Crash_id').distinct().count()
# print("2. ", df2)
# df2_sql = spark.sql("select count(distinct crash_id) from units where upper(VEH_BODY_STYL_ID) like '%MOTORCYCLE%'")
# print("2. sql ")
# df2_sql.show()

#3.Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

# df3 = df_units.join(df_primary_person, on=["CRASH_ID"], how="inner")\
#             .filter((col("PRSN_INJRY_SEV_ID") == "KILLED")\
#                 & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")\
#                 & (col("VEH_MAKE_ID") != "NA"))\
#             .groupby("VEH_MAKE_ID")\
#             .count()\
#             .orderBy(col("count").desc())\
#             .limit(5)
# print("3. ")
# df3.show()
# df3_sql = spark.sql('''SELECT VEH_MAKE_ID, COUNT(*) AS count
#                     FROM units JOIN pp ON units.CRASH_ID = pp.CRASH_ID
#                     WHERE PRSN_TYPE_ID = 'DRIVER' AND PRSN_INJRY_SEV_ID = 'KILLED'
#                     AND PRSN_AIRBAG_ID = 'NOT DEPLOYED' AND VEH_MAKE_ID != 'NA'
#                     GROUP BY VEH_MAKE_ID ORDER BY count DESC
#                     LIMIT 5''')
# print("3. sql ")
# df3_sql.show()

# 4. number of Vehicles with driver having valid licences involved in hit and run? 

# df4 = df_units.select("CRASH_ID", "VEH_HNR_FL")\
#             .join(df_primary_person.select("CRASH_ID", "DRVR_LIC_TYPE_ID"),on=["CRASH_ID"],how="inner")\
#             .filter(col("VEH_HNR_FL") == "Y")\
#             .filter(col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))\
#             .select('CRASH_ID').distinct().count()
# print("4. ", df4)

# df4_sql = spark.sql('''SELECT count(distinct pp.CRASH_ID)
#                     FROM units JOIN pp ON units.CRASH_ID = pp.CRASH_ID
#                     WHERE VEH_HNR_FL = 'Y' 
#                     AND DRVR_LIC_TYPE_ID IN ('DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.')''')

# print("4. sql ")
# df4_sql.show()

# 5. Which state has highest number of accidents in which females are not involved? 

# df5 = df_primary_person.filter( df_primary_person.PRSN_GNDR_ID != "FEMALE")\
#           .filter(~col("DRVR_LIC_STATE_ID").isin('NA','Unknown'))\
#             .groupby("DRVR_LIC_STATE_ID")\
#             .count()\
#             .orderBy(col("count").desc()).limit(2)

# print("5. ")
# df5.show()

# df5_sql = spark.sql('''SELECT DRVR_LIC_STATE_ID, COUNT(*) AS count
#                     FROM pp WHERE PRSN_GNDR_ID != 'FEMALE'
#                     and DRVR_LIC_STATE_ID not in ('NA', 'Unknown')
#                     GROUP BY DRVR_LIC_STATE_ID ORDER BY count DESC limit 2''')

# print("5. sql")
# df5_sql.show()

# 6. Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death 


# df6a = ( df_units.filter(df_units.VEH_MAKE_ID != "NA")\
#             .withColumn("TOT_INJURIES", df_units['TOT_INJRY_CNT'] + df_units['DEATH_CNT'])\
#             .groupby("VEH_MAKE_ID")\
#             .sum("TOT_INJURIES")\
#             .withColumnRenamed("sum(TOT_INJURIES)", "TOT_INJURIES")\
#             .orderBy(col("TOT_INJURIES").desc())\
#         )

# df6 = df6a.limit(5).subtract(df6a.limit(2))
# print('6. ')
# df6.show()

# df6_sql = spark.sql('''select VEH_MAKE_ID from (
#                     SELECT VEH_MAKE_ID, sum(TOT_INJRY_CNT + DEATH_CNT) as TOT_INJURIES, 
#                     dense_rank() over (order by sum(TOT_INJRY_CNT + DEATH_CNT) desc) as ranks
#                     FROM units
#                     WHERE VEH_MAKE_ID!= 'NA'
#                     GROUP BY VEH_MAKE_ID) a where ranks>=3 and ranks <=5''')
# print("6. sql")
# df6_sql.show()

# df6_opt = df_units.filter(col("VEH_MAKE_ID") != "NA").groupBy("VEH_MAKE_ID")\
#         .agg(sum(col("TOT_INJRY_CNT") + col("DEATH_CNT")).alias("TOT_INJURIES"))\
#         .withColumn("ranks", dense_rank().over(Window.orderBy(col("TOT_INJURIES").desc())))
# print("6. opt")
# df6_opt.filter((col("ranks") >= 3) & (col("ranks") <= 5)).select("VEH_MAKE_ID").show()

# 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  

# w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
# df7 = ( df_units.join(df_primary_person, on=["CRASH_ID"], how="inner")\
#     .filter(~ df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]))\
#     .filter(~ df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))\
#     .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")\
#     .count()\
#     .withColumn("row", row_number().over(w))\
#     .filter(col("row") == 1)\
#     .drop("row", "count")
# )
# print("7. ")
# df7.show()

# df7_sql = spark.sql('''select VEH_BODY_STYL_ID, PRSN_ETHNICITY_ID from 
#                     (select VEH_BODY_STYL_ID, PRSN_ETHNICITY_ID,
#                     rank() over (partition by VEH_BODY_STYL_ID order by cnt desc) as ranks
#                     from (select VEH_BODY_STYL_ID, PRSN_ETHNICITY_ID, count(1) as cnt 
#                         from units join pp on units.crash_id = pp.crash_id group by 1,2)) as a
#                     where ranks = 1''')
# print("7. sql")
# df7_sql.show()

# 8. Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the 
# contributing factor to a crash (Use Driver Zip Code)

# df8 = (df_units.join(df_primary_person, on=["CRASH_ID"], how="inner")\
#             .filter(~ df_primary_person.DRVR_ZIP.isin(["UNK", "UNKNOWN", ""]))\
#             # .dropna(subset=["DRVR_ZIP"])
#             .filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")|col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))\
#             .groupby("DRVR_ZIP")\
#             .count()\
#             .orderBy(col("count").desc())\
#             .limit(5))

# print("8. ")
# df8.show()

# df8_sql = spark.sql('''select DRVR_ZIP from (
#                     select DRVR_ZIP, count(1) as cnt 
#                     from pp join units on pp.crash_id = units.crash_id
#                     where DRVR_ZIP not in ('UNK', 'UNKNOWN', '')
#                     and (CONTRIB_FACTR_1_ID like '%ALCOHOL%' or CONTRIB_FACTR_2_ID like '%ALCOHOL%')
#                     group by DRVR_ZIP order by cnt desc
#                     ) limit 5
#                     ''')
# print("8. sql")
# df8_sql.show()

# 9. Count of Distinct Crash IDs where No Damaged Property was observed and 
# Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

df9 = (df_damages.join(df_units, on=["CRASH_ID"], how="inner")
            .filter( ( (df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                & ( ~df_units.VEH_DMAG_SCL_1_ID.isin( ["NA", "NO DAMAGE", "INVALID VALUE"])) )
                | ( (df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & ( ~df_units.VEH_DMAG_SCL_2_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"] ) ) ) )
            .filter(col("DAMAGED_PROPERTY").contains("NO DAMAGE"))
            .filter(df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
            .join(df_charges, on=["CRASH_ID"], how = "left")
            .filter(~col("CHARGE").contains("INSURANCE"))
            .select('CRASH_ID').distinct()
            )
print("9. ")
df9.show()

spark.stop()