Here is how I tackled the entire Prospa ETL pipeline challenge from start to finish. I will break it down simply. 

Imagine you have a messy pile of receipts from a grocery store. The receipts are pipe delimited text files in a zip folder. Some of the lines have weird formatting and there are millions of them. You cannot make any smart business decisions just by holding a massive pile in your hands. You need to organize them.

I used Databricks to organize this mess in three very clear stages. In data engineering, we call this the Medallion Architecture. Think of it like purifying water.

First is the Bronze Layer. This is the raw intake. I took those raw pipe delimited text files and loaded them exactly as they were into Databricks using PySpark. PySpark is just a distributed engine that lets me read massive amounts of data across multiple computers at once. I forced a strict structure onto the files while reading them so that any malformed trailing pipes were instantly ignored. Now my data is safely stored in a digital format called Delta Tables, but it is still messy.

Second is the Silver Layer. This is the filter. I took the Bronze tables and started cleaning them and adding value. The challenge asked me to calculate revenue per line item and categorize customer balances into low, medium or high groups. I did that right here natively in PySpark. I mathematically added the revenue columns and the groupings, and I saved the corrected tables. Now the data is clean, but it is still highly separated across eight different tables. If an analyst wanted to know a simple answer like who is my top customer by revenue, they would have to write a painful query joining five tables together. That is very slow.

Third is the Gold Layer. This is the final bottled water. I performed Dimensional Modeling, specifically designing a Star Schema. Instead of keeping a customer table separate from a nation table and a region table, I flattened them. I glued nation and region directly onto the customer records to make one wide, easily readable Customer Dimension. I then took the core sales transactions and made them the center of the star, known as the Fact Table. 

Now, when the analytical questions hit, I did not need to run massive, slow joins across the database. Using simple Databricks SQL, I queried the golden Star Schema directly. I found the top five nations by revenue and calculated year over year financial growth using standard SQL window functions effortlessly.

If I needed to automate this to run every four hours, I would simply wrap my Databricks notebook into a Databricks Workflow Job and set a CRON schedule. If the business told me the data was now arriving in random, continuous streams instead of a daily zip file, I would swap my static PySpark read commands for Databricks Auto Loader. Auto Loader continuously listens to the storage bucket and naturally trickles the new data into the pipeline using Delta Lake Merge operations to handle late arriving data without duplicating anything. 

That is the entire, end to end process.
