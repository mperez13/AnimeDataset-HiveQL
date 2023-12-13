--Importing the csv file to the hive---

scp C:/Users/anataka2/Downloads/Anime_dataset/final_animedataset.csv anataka2@129.153.66.218:/home/anataka2


-- Create an intial Hive table for Descriptive/Temporory analysis---

CREATE TABLE IF NOT EXISTS anime_ratings (
    username STRING,
    anime_id INT,
    my_score INT,
    user_id INT,
    gender STRING,
    title STRING,
    type STRING,
    source STRING,
    scored_by INT,
    rank INT,
    popularity INT,
    genre STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/anataka2/TermProject/'
TBLPROPERTIES ('skip.header.line.count'='1');


--Importing the csv file to the hive---

scp C:/Users/anataka2/Downloads/Anime_dataset/users-details-2023.csv anataka2@129.153.66.218:/home/anataka2

---Creating a new table for Descriptive/Temporory analysis---

CREATE TABLE IF NOT EXISTS anime_user_data (
    Mal_ID INT,
    Username STRING,
    Gender STRING,
    Birthday STRING,
    Location STRING,
    Joined STRING,
    Days_Watched INT,
    Mean_Score DOUBLE,
    Watching INT,
    Completed INT,
    On_Hold INT,
    Dropped INT,
    Plan_to_Watch INT,
    Total_Entries INT,
    Rewatched INT,
    Episodes_Watched INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/anataka2/TermProject/'
TBLPROPERTIES ('skip.header.line.count'='1');

---Query---
SELECT Username,Gender,Birthday,Location
FROM anime_user_data WHERE Days_Watched > 100 
ORDER BY Days_Watched DESC
LIMIT 20;


i)***Temporal Analysis***
Query 1:

1) Number of the users joined and count per month from 2004 TO 2010
SELECT
    SUBSTR(Joined, 1, 7) AS join_month,
    COUNT(Username) AS user_count
FROM
    anime_user_data
WHERE
    Joined IS NOT NULL
    AND SUBSTR(Joined, 1, 4) BETWEEN '2004' AND '2010'
GROUP BY
    SUBSTR(Joined, 1, 7)
ORDER BY
    join_month;



---Saving it to CSV file---
INSERT OVERWRITE DIRECTORY '/user/anataka2/tmp/JoinedMonth/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    SUBSTR(Joined, 1, 7) AS join_month,
    COUNT(Username) AS user_count
FROM
    anime_user_data
WHERE
    Joined IS NOT NULL
    AND SUBSTR(Joined, 1, 4) BETWEEN '2004' AND '2010'
GROUP BY
    SUBSTR(Joined, 1, 7)
ORDER BY
    join_month;
	
---Download the file and then save it to local computer---
hdfs dfs -cat tmp/JoinedMonth/000000_0 | tail -n 2
hdfs dfs -ls ./tmp/JoinedMonth/
hdfs dfs -get ./tmp/JoinedMonth/000000_0 JoinedMonth.csv
tail -n 2 JoinedMonth.csv	

scp anataka2@129.153.66.218:/home/anataka2/JoinedMonth.csv .


Query 2:

1) Percentage change of the users joined and count per month from 2004 TO 2010	

SELECT
    join_month,
    user_count,
    LAG(user_count) OVER (ORDER BY join_month) AS prev_user_count,
    CASE
        WHEN LAG(user_count) OVER (ORDER BY join_month) IS NOT NULL
        THEN ((user_count - LAG(user_count) OVER (ORDER BY join_month)) / LAG(user_count) OVER (ORDER BY join_month)) * 100
        ELSE NULL
    END AS annual_percentage_change
FROM (
    SELECT
        SUBSTR(Joined, 1, 7) AS join_month,
        COUNT(Username) AS user_count
    FROM
        anime_user_data
    WHERE
        Joined IS NOT NULL
        AND SUBSTR(Joined, 1, 4) BETWEEN '2004' AND '2010'
    GROUP BY
        SUBSTR(Joined, 1, 7)
) UserCounts
ORDER BY
    join_month;
	
---Saving it to CSV file---
INSERT OVERWRITE DIRECTORY '/user/anataka2/tmp/JoinedMonthPercentage/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    join_month,
    user_count,
    LAG(user_count) OVER (ORDER BY join_month) AS prev_user_count,
    CASE
        WHEN LAG(user_count) OVER (ORDER BY join_month) IS NOT NULL
        THEN ((user_count - LAG(user_count) OVER (ORDER BY join_month)) / LAG(user_count) OVER (ORDER BY join_month)) * 100
        ELSE NULL
    END AS annual_percentage_change
FROM (
    SELECT
        SUBSTR(Joined, 1, 7) AS join_month,
        COUNT(Username) AS user_count
    FROM
        anime_user_data
    WHERE
        Joined IS NOT NULL
        AND SUBSTR(Joined, 1, 4) BETWEEN '2004' AND '2010'
    GROUP BY
        SUBSTR(Joined, 1, 7)
) UserCounts
ORDER BY
    join_month;

---Downloading the file to the local computer---
hdfs dfs -cat tmp/JoinedMonthPercentage/000000_0 | tail -n 2
hdfs dfs -ls ./tmp/JoinedMonthPercentage/
hdfs dfs -get ./tmp/JoinedMonthPercentage/000000_0 JoinedMonthPercentage.csv
tail -n 2 JoinedMonthPercentage.csv

scp anataka2@129.153.66.218:/home/anataka2/JoinedMonthPercentage.csv .


---Import a new csv file to join the tables---

scp C:/Users/anataka2/Downloads/Anime_dataset/anime-dataset-2023.csv anataka2@129.153.66.218:/home/anataka2

CREATE TABLE IF NOT EXISTS anime_dataset (
anime_id INT,
Name STRING,
English_Name STRING,
Other_Name STRING,
Score INT,
Genres STRING,
Synopsis STRING,
Type STRING,
Episodes STRING,
Aired STRING,
Premiered STRING,
Status STRING,
Producers STRING,
Licensors STRING,
Studios STRING,
Source STRING,
Duration STRING,
Rating STRING,
Rank INT,
Popularity INT,
Favorites INT,
Scored_By INT,
Members INT,
Image_URL STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/anataka2/TermProject/'
TBLPROPERTIES ('skip.header.line.count'='1');



ii)***Spatial Analysis***
---Creating a table for heatmap---

CREATE TABLE IF NOT EXISTS joined_anime_data AS
SELECT
    u.Mal_ID,
    u.Username,
    u.Gender,
    u.Birthday,
    u.Location,
    u.Joined,
    u.Days_Watched,
    u.Mean_Score,
    u.Watching,
    u.Completed,
    u.On_Hold,
    u.Dropped,
    u.Plan_to_Watch,
    u.Total_Entries,
    u.Rewatched,
    u.Episodes_Watched
FROM
    anime_user_data u
JOIN
    anime_ratings r
ON
    u.Username = r.Username
WHERE
    u.Username IS NOT NULL
    AND r.Username IS NOT NULL;
LIMIT 20;

--This query sums the Episodes_Watched for each location--

SELECT
    Location,
    SUM(Episodes_Watched) AS Total_Watched_Episodes
FROM
    joined_anime_data
WHERE
    Location IS NOT NULL
    AND Episodes_Watched IS NOT NULL
GROUP BY
    Location
ORDER BY
    Total_Watched_Episodes DESC
LIMIT 10;

--saving it to csv format--

INSERT OVERWRITE DIRECTORY '/user/anataka2/tmp/joined_anime_data/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    Location,
    SUM(Episodes_Watched) AS Total_Watched_Episodes
FROM
    joined_anime_data
WHERE
    Location IS NOT NULL
    AND Episodes_Watched IS NOT NULL
GROUP BY
    Location
ORDER BY
    Total_Watched_Episodes DESC
LIMIT 10;

--Downloading the csv file to hdsf, then to you local computer--
hdfs dfs -cat tmp/joined_anime_data/000000_0 | tail -n 2
hdfs dfs -ls ./tmp/joined_anime_data/
hdfs dfs -get ./tmp/joined_anime_data/000000_0 joined_anime_data.csv
tail -n 2 joined_anime_data.csv

scp anataka2@129.153.66.218:/home/anataka2/joined_anime_data.csv .



