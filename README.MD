Source [Flood Planning Submission Collector](https://twdb-flood-planning-resources-twdb.hub.arcgis.com/pages/cursory-floodplain-data-2025)

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
