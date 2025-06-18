Source [Flood Planning Submission Collector](https://twdb.sharepoint.com/teams/Flood_Planning_Submission_Collector/Shared%20Documents/Forms/All.aspx?id=%2Fteams%2FFlood%5FPlanning%5FSubmission%5FCollector%2FShared%20Documents%2FCursory%20Floodplain%20Dataset%20Phase%202%2FCursory%20Floodplain%20Phase%202%20%2D%20Downloads&viewid=bdced3a4%2D41bd%2D4507%2D9dd6%2D466171b55b9e&p=true&ga=1)

The code will iterate all the tiles with all the frequencies in parallel, for each instance, will try at most 5 times with random pause if returned state code **429**, and the <u>download_errors.log</u> can be found inside each frequency sub folder.

Examples:
```[bash]
python download.py \
--scenario 5 \
--flood-type Pluvial \
--code "2020_0p50_P" \
--workers 8 \
--ck "..."

```

```[bash]
python download.py \
--scenario 3 \
--flood-type Pluvial \
--code "2060_0p83_LU_S_P" \
--workers 8 \
--ck "..."

```

- `code`is the unique strings between `TILE` and `FREQ.tif`, e.g. n26w100_2020_0p50_P_1in500.tif, `code` here is **2020_0p50_P**.
- `ck` is user unique Cookies, follow the instruction [here](./Instructions%20for%20Downloading%20TWDB%20Flood%20Data_AGB.docx) to get your Cookies.
- `workers` enables parallel downloading, default core number is **8**.
