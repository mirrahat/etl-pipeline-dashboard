# Power BI Integration Guide for Movie Analytics Pipeline

## 🔗 Connecting Power BI to Your Databricks Gold Layer

### Prerequisites
- Power BI Desktop installed
- Access to your Azure Databricks workspace
- Gold layer Delta tables available

### Connection Options

#### Option 1: Direct Delta Lake Connection (Recommended)
1. **Open Power BI Desktop**
2. **Get Data** → **More** → **Azure** → **Azure Databricks**
3. **Connection Details:**
   - Server hostname: `your-databricks-workspace-url`
   - HTTP path: `/sql/1.0/warehouses/your-warehouse-id`
   - Authentication: Azure Active Directory

#### Option 2: Parquet Files Connection
1. **Get Data** → **Azure** → **Azure Data Lake Storage Gen2**
2. **Account URL:** `https://yourstorageaccount.dfs.core.windows.net`
3. **Navigate to:** `/datalake/gold/exports/`

#### Option 3: CSV Export Connection
1. **Get Data** → **Text/CSV**
2. **Import from:** Gold layer CSV exports

### 📊 Recommended Dashboard Structure

## Executive Dashboard (Page 1)
### KPI Cards Row
- Total Revenue ($)
- Total Profit ($) 
- Average ROI (%)
- Blockbuster Rate (%)
- Profitability Rate (%)
- Average Rating

### Main Visualizations
- **Movie Count by Genre** (Donut Chart)
- **Revenue Trend by Era** (Line Chart)
- **Top 10 Performers** (Horizontal Bar Chart)
- **Budget vs Revenue Scatter** (Scatter Plot)

## Genre Analysis Dashboard (Page 2)
### Genre Performance Matrix
- **Average Rating by Genre** (Clustered Column)
- **ROI Distribution** (Box and Whisker Plot)
- **Genre Market Share** (Treemap)
- **Budget Efficiency** (Bubble Chart)

## Financial Analysis Dashboard (Page 3)
### Investment Analysis
- **Profit Margin Analysis** (Waterfall Chart)
- **Risk-Return Matrix** (Scatter Plot)
- **Budget Category Performance** (Stacked Bar)
- **Blockbuster Prediction Factors** (Correlation Matrix)

## Movie Explorer Dashboard (Page 4)
### Interactive Filters
- Year Range Slider
- Genre Multi-select
- Rating Range
- Budget Range

### Detailed Views
- **Movie Performance Table** (Table with conditional formatting)
- **Interactive Movie Explorer** (Scatter with drill-through)
- **Performance Score Breakdown** (Gauge Charts)

### 🎨 Design Guidelines

#### Color Scheme
- **Primary:** #1f77b4 (Blue)
- **Secondary:** #ff7f0e (Orange)  
- **Success:** #2ca02c (Green)
- **Warning:** #d62728 (Red)
- **Neutral:** #7f7f7f (Gray)

#### Typography
- **Headers:** Segoe UI, Bold, 16-18pt
- **Titles:** Segoe UI, Semibold, 12-14pt
- **Body:** Segoe UI, Regular, 10-11pt

#### Layout Principles
- Use consistent margins (20px)
- Align visualizations to grid
- Group related KPIs together
- Maintain white space for readability

### 📈 Key Measures (DAX)
```dax
// Total Revenue
Total Revenue = SUM('movie_performance_summary'[box_office])

// Total Profit
Total Profit = SUM('movie_performance_summary'[profit])

// Average ROI
Average ROI = AVERAGE('movie_performance_summary'[roi])

// Blockbuster Rate
Blockbuster Rate = 
DIVIDE(
    COUNTROWS(FILTER('movie_performance_summary', 'movie_performance_summary'[is_blockbuster] = TRUE)),
    COUNTROWS('movie_performance_summary')
) * 100

// Profitability Rate
Profitability Rate = 
DIVIDE(
    COUNTROWS(FILTER('movie_performance_summary', 'movie_performance_summary'[is_profitable] = TRUE)),
    COUNTROWS('movie_performance_summary')
) * 100

// Performance Score Average
Avg Performance Score = AVERAGE('movie_performance_summary'[performance_score])

// Genre Performance Rank
Genre Rank = RANKX(ALL('movie_performance_summary'[genre]), [Average ROI], , DESC)

// Year over Year Growth
YoY Revenue Growth = 
VAR CurrentYear = MAX('movie_performance_summary'[year])
VAR PreviousYear = CurrentYear - 1
VAR CurrentRevenue = CALCULATE([Total Revenue], 'movie_performance_summary'[year] = CurrentYear)
VAR PreviousRevenue = CALCULATE([Total Revenue], 'movie_performance_summary'[year] = PreviousYear)
RETURN
IF(PreviousRevenue > 0, (CurrentRevenue - PreviousRevenue) / PreviousRevenue, BLANK())

// Top Performer Indicator
Is Top Performer = 
IF('movie_performance_summary'[performance_score] > 8, "⭐ Top Performer", "Standard")

// Budget Category
Budget Category = 
SWITCH(
    TRUE(),
    'movie_performance_summary'[budget] >= 100000000, "Big Budget (>$100M)",
    'movie_performance_summary'[budget] >= 50000000, "Medium Budget ($50M-$100M)",
    'movie_performance_summary'[budget] >= 10000000, "Low Budget ($10M-$50M)",
    "Micro Budget (<$10M)"
)
```

### 🔧 Advanced Features

#### Conditional Formatting
- **Profit/Loss:** Green for positive, red for negative
- **Performance Score:** Color scale from red (low) to green (high)
- **ROI:** Data bars with conditional colors

#### Interactive Features
- **Cross-filtering:** Enable between all visuals
- **Drill-through pages:** Movie details page
- **Bookmarks:** Save different views
- **Tooltips:** Custom tooltip pages for detailed info

#### Mobile Layout
- Optimize for phone/tablet viewing
- Use mobile-friendly chart types
- Simplify KPI layouts for small screens

### 📱 Mobile Dashboard Design
- Stack KPIs vertically
- Use simplified chart types
- Ensure touch-friendly interactions
- Test on actual mobile devices

### 🚀 Deployment Options

#### Power BI Service
1. Publish to Power BI Service
2. Set up scheduled refresh
3. Share with stakeholders
4. Embed in SharePoint/Teams

#### Power BI Embedded
1. Integrate into custom applications
2. White-label dashboard experience
3. Programmatic access via REST APIs

### 📊 Sample Data Connections

#### Connection Strings
```
# Azure Databricks
Server=your-databricks-workspace.azuredatabricks.net;
HTTPPath=/sql/1.0/warehouses/your-warehouse-endpoint;
AuthMech=11;
UID=token;
PWD=your-databricks-token;
ThriftTransport=2;
SSL=1;

# Azure Data Lake Gen2
abfss://datalake@yourstorageaccount.dfs.core.windows.net/gold/
```

### 🎯 Performance Optimization
- Use DirectQuery for real-time data
- Implement row-level security if needed
- Optimize DAX calculations
- Use aggregations for large datasets
- Set up incremental refresh

### 📋 Checklist for Go-Live
- [ ] Data connections tested
- [ ] All measures working correctly
- [ ] Cross-filtering configured
- [ ] Mobile layout optimized
- [ ] Scheduled refresh set up
- [ ] Users have appropriate permissions
- [ ] Documentation provided to end users

This Power BI integration turns your Databricks analytics into business-ready dashboards that executives and analysts can use for data-driven decision making!