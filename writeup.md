# Project Write-up: ETL Log Search and Category Mapping

## Objective

This project aims to analyze user search behavior from log data in order to:
- Identify each user's most searched keyword in **June** and **July**.
- Map each keyword to a corresponding **content category**.
- Detect any **category shift** in user interest between the two months.

## Tools & Technologies

- **PySpark** – for processing large-scale Parquet log data  
- **Pandas** – for handling keyword-category mappings from Excel  
- **n8n + OpenAI API** – to automate category labeling using a language model  
- **Power BI** – for visualizing user behavior   

## ETL Pipeline Overview

1. **Extract**
   - Load `.parquet` files from `20220601` and `20220714` directories
   - Filter logs where the action is `"search"` and remove null entries

2. **Transform**
   - Extract each user’s most searched keyword for both June and July
   - Join with keyword-category mappings from Excel
   - Compare June and July categories to track user interest changes

3. **Load**
   - Save results as into `\output` folder

## Automated Category Labeling with n8n

The `category-mapping.json` workflow enables:
- Real-time keyword classification using a prompt-engineered OpenAI agent
- Predefined category list (e.g., Romance, Fantasy, Action, Comedy, etc.)
- Results are saved directly to a connected Google Sheet
- Useful for enriching identical keywords in the log data

## Key Results

- **45.83%** of users maintained the same content category across June and July (No Change), indicating a large portion of stable user interests.
- Most common category shifts include:
  - `Drama → Romance`
  - `Romance → Historical / Costume`

## Insights

- Some genres (e.g., **Romance**, **Anime / Cartooon**) show high stability
- Comparing the **bar charts** of June vs. July:
   - **Romance** consistently ranks highest in both months, confirming its dominant appeal.
   - Categories like **Fantasy** and **Drama** slightly gained traction, while **Comedy**, **Sports/News**, and **Medical/Legal** remained less popular.
- The fact that **54.17%** of users changed their search focus suggests that:
   - Recommendations should adapt month-over-month.

## Recommendations for Extension

- Build an interactive dashboard in Power BI
- Apply clustering to group users based on behavior patterns
- Implement keyword normalization to group semantically or case-insensitive identical search words