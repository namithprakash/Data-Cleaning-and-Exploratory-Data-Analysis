# Data-Cleaning-and-Exploratory-Data-Analysis:

This project demonstrates an end-to-end data cleaning workflow in SQL using a real-world layoffs dataset. The goal is to convert raw, inconsistent data into a structured and trustworthy foundation for exploratory data analysis (EDA) and downstream insights.

Key Objectives:

- Preserve the integrity of the raw dataset by working in staging tables
- Identify and remove duplicate records using window functions
- Standardize categorical values (e.g., industry and country names)
- Clean text fields by trimming whitespace and fixing inconsistencies
- Convert improperly formatted dates into SQL DATE types
- Handle missing values by imputing from related records where possible
- Remove unusable rows where critical metrics are absent

Techniques Used:

- ROW_NUMBER() for duplicate detection
- CTEs and joins for validation and backfilling null values
- LIKE, and conditional updates for normalization
- STR_TO_DATE and ALTER TABLE for type correction
- Safe staging-table strategy to avoid modifying raw data

Outcome:

The final dataset is streamlined, consistent, and ready for analysis and visualization. Future updates to this repository will include exploratory data analysis (EDA) to uncover trends in layoffs across companies, industries, locations, and time.
