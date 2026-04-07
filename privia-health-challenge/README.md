# Privia Health Data Engineering Challenge

## What is this

A data engineering take-home challenge from Privia Health. The task: take a messy Excel file with patient demographics and risk scores, clean it, transform it, and load it into queryable Delta tables on Databricks.

The data is fictional. The code is real.

---

## The problem

I was given a file called `Privia Family Medicine 113018.xlsx`. It has three sections in one sheet: Demographics, Quarters, and Risk Data. The file naming convention carries two facts the sheet itself doesn't: the provider group name and the file date — both packed into the filename. More files from different groups will arrive later, so I had to write the ETL to handle any file that follows the same naming pattern, not just this one.

---

## What I built

Two ETL notebooks and one test notebook, all running on Databricks with Delta Lake.

**Notebook 01 — Demographics ETL**

I read the Excel file with pandas (PySpark can't read xlsx natively), sliced out the Demographics columns, and cleaned three things the challenge required: middle names get reduced to just the first letter, the Sex field goes from 0/1 to M/F, and I parsed the provider group and file date out of the filename using a regex. Then I handed it to Spark and wrote it as a Delta table.

**Notebook 02 — Risk & Quarters ETL**

This one needed an unpivot. The raw data is wide — one row per patient, with Q1 and Q2 sitting side by side. I needed it long — one row per patient per quarter. I used pandas `melt` to do that, then joined the attribution and risk halves back together. After that I filtered to only the patients whose risk score went up from Q1 to Q2. Wrote the result to a second Delta table.

**Notebook 03 — Tests**

I wrote unit tests for every piece of logic that doesn't need a cluster: the filename parser, the middle initial function, the sex code mapper, the risk increase filter, and the unpivot shape. None of these tests hit the actual Excel file or a database — they use tiny in-memory DataFrames so they run in a few seconds.

---

## The core idea behind each transformation

**Filename parsing:** I use a regex that says "match any text, then exactly 6 digits, then .xlsx." The 6 digits are always MMDDYY. Everything before them is the group name. This works regardless of how long the group name is.

**Middle initial:** If the middle name field is blank or null, return null. Otherwise take the first character and uppercase it. That's it.

**Sex mapping:** 0 → M, 1 → F. Anything unexpected → null. I chose null over a crash because future files might have gaps.

**Unpivot:** I melt the attributed columns into one column and the risk columns into another, separately, then join them on ID + Quarter. This gives me the long format the challenge asked for.

**Risk increase filter:** I compare Q1 and Q2 risk scores per patient in the wide format first, collect the IDs that went up, then filter the long table to just those IDs. Doing the comparison before the unpivot is cleaner because Q1 and Q2 are on the same row.

---

## How to run it

1. Upload `Privia Family Medicine 113018.xlsx` to your Databricks Volume at:
   `/Volumes/workspace/default/prospa_volume/`

2. Import the three notebooks from the `notebooks/` folder into your Databricks workspace:
   `Workspace → Import → select .py file`

3. Attach a cluster and run the notebooks in order: `01`, `02`, `03`.

4. Query the output tables:
   ```sql
   SELECT * FROM workspace.default.demographics;
   SELECT * FROM workspace.default.risk_quarters;
   ```

---

## What I would add with more time

- A job that runs both ETLs on a schedule when a new file lands in the volume
- A merge strategy instead of overwrite, so re-delivering the same file doesn't duplicate data
- Schema validation on ingest so bad data gets caught at the door, not downstream
- A logging table that records every file processed, row counts, and any errors

---

## Tech stack

- Databricks (Unity Catalog, Delta Lake)
- PySpark 3.5
- pandas 2.0 (for xlsx reading and melt)
- pytest (for unit tests)
- openpyxl (pandas xlsx backend)
