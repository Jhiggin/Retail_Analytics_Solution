# Databricks Genie Instructions: Retail Analytics Star Schema

## Overview

This guide enables Databricks Genie to understand and query your Retail Analytics Solution's **Kimball star schema** in the `gold` layer. The schema consists of **four dimension tables** and **one fact table** designed for efficient analytics queries.

---

## Schema Architecture

### Fact Table: `gold.fact_sales`

**Purpose**: Central fact table containing all sales transactions with foreign keys to dimensions.

**Grain**: One row per transaction (identified by TransactionID)

**Columns**:
- `TransactionID` (STRING) - **Natural key** - Unique transaction identifier
- `DateKey` (DATE) - **Foreign Key** to dim_date
- `StoreKey` (INT) - **Foreign Key** to dim_store
- `ProductKey` (INT) - **Foreign Key** to dim_product
- `PromotionKey` (INT) - **Foreign Key** to dim_promotion (default -1 when no promotion)
- `Quantity` (INT) - **Measure** - Units sold per transaction
- `UnitPrice` (DOUBLE) - **Degenerate dimension** - Price per unit at time of sale
- `DiscountAmount` (DOUBLE) - **Measure** - Discount value in dollars
- `TotalSales` (DOUBLE) - **Measure** - Final revenue amount (quantity × unit price - discount)
- `_created_at` (TIMESTAMP) - Load timestamp

**Example Queries**:
- Total revenue by store
- Units sold by product category
- Transaction count by region
- Promotion ROI analysis

---

### Dimension Tables

#### 1. `gold.dim_date` - Calendar Dimension

**Purpose**: Provides temporal context for all sales transactions.

**Type**: Not a typical SCD (pre-generated dimension, non-changing)

**Grain**: One row per calendar date in the sales date range

**Columns**:
- `DateKey` (DATE) - **Primary Key** - The date value itself
- `Date` (DATE) - Duplicate of DateKey for clarity
- `Year` (INT) - Calendar year (e.g., 2024)
- `Month` (INT) - Month of year (1-12)
- `DayOfMonth` (INT) - Day of month (1-31)
- `DayOfWeek` (INT) - Day of week (1=Sunday, 2=Monday, ..., 7=Saturday)
- `WeekOfYear` (INT) - ISO week number (1-52)
- `Quarter` (INT) - Quarter (1-4)
- `QuarterYear` (STRING) - Quarter label (e.g., "2024-Q1")
- `DayName` (STRING) - Day name (Monday, Tuesday, etc.)
- `MonthName` (STRING) - Month name (January, February, etc.)
- `IsWeekend` (BOOLEAN) - TRUE for Saturday/Sunday

**Common Joins**: `fact_sales.DateKey = dim_date.DateKey`

**Use Cases**:
- Filter by month, quarter, or year
- Compare weekday vs weekend sales
- Identify seasonal trends
- Generate year-over-year comparisons

---

#### 2. `gold.dim_product` - Product Dimension

**Purpose**: Contains product attributes and hierarchies for product analysis.

**Type**: Type 1 SCD (updates overwrite history)

**Grain**: One row per unique product

**Columns**:
- `ProductKey` (INT) - **Primary Key** - Surrogate key (sequential identifier)
- `ProductID` (STRING) - **Natural key** - Source product identifier
- `ProductName` (STRING) - Product name (e.g., "TechBrand Laptop Pro")
- `Category` (STRING) - Product category (Electronics, Accessories, Premium)
- `Brand` (STRING) - Brand name (TechBrand, AccessoryBrand, PremiumBrand)
- `BasePrice` (DOUBLE) - List price in dollars
- `_updated_at` (TIMESTAMP) - Last update timestamp

**Common Joins**: `fact_sales.ProductKey = dim_product.ProductKey`

**Use Cases**:
- Analyze sales by category or brand
- Compare performance across product lines
- Drill down from category → brand → product
- Track price changes (note: Type 1 SCD means history is overwritten)

**Example Analysis Hierarchies**:
```
Category (Electronics, Accessories, Premium)
  ↓
Brand (TechBrand, AccessoryBrand, PremiumBrand)
  ↓
ProductName (specific product)
```

---

#### 3. `gold.dim_store` - Store Dimension

**Purpose**: Contains store location and operational attributes for store-level analysis.

**Type**: Type 1 SCD (updates overwrite history)

**Grain**: One row per store

**Columns**:
- `StoreKey` (INT) - **Primary Key** - Surrogate key (sequential identifier)
- `StoreID` (STRING) - **Natural key** - Source store identifier
- `StoreName` (STRING) - Store name/location
- `Region` (STRING) - Geographic region (Northeast, Midwest, West, South)
- `StoreType` (STRING) - Store classification (Premium, Standard, Outlet, Express)
- `SquareFootage` (INT) - Store size in square feet (3,500 - 10,000 sq ft range)
- `_updated_at` (TIMESTAMP) - Last update timestamp

**Common Joins**: `fact_sales.StoreKey = dim_store.StoreKey`

**Use Cases**:
- Compare sales by region or store type
- Analyze performance by store size
- Regional trend analysis
- Store-level benchmarking

**Example Analysis Hierarchies**:
```
Region (Northeast, Midwest, West, South)
  ↓
StoreType (Premium, Standard, Outlet, Express)
  ↓
StoreName (specific store)
```

---

#### 4. `gold.dim_promotion` - Promotion Dimension

**Purpose**: Tracks promotion attributes and effectiveness metrics.

**Type**: Type 1 SCD (updates overwrite history)

**Grain**: One row per promotion

**Columns**:
- `PromotionKey` (INT) - **Primary Key** - Surrogate key (sequential identifier)
  - **Special Value**: -1 = No promotion applied (default for non-promotional sales)
- `PromotionID` (STRING) - **Natural key** - Source promotion identifier
- `PromotionType` (STRING) - Type of promotion
  - Valid values: Percentage Discount, Bundle Deal, Flash Sale, Seasonal, Loyalty, Clearance, BOGO, etc.
- `StartDate` (DATE) - Campaign start date
- `EndDate` (DATE) - Campaign end date
- `DiscountPercent` (DOUBLE) - Discount percentage (e.g., 15.0 = 15%)
- `MarketingSpend` (DOUBLE) - Budget allocated to promotion
- `_updated_at` (TIMESTAMP) - Last update timestamp

**Common Joins**: `fact_sales.PromotionKey = dim_promotion.PromotionKey`

**Important**: Filter `PromotionKey != -1` when analyzing promoted vs non-promoted sales

**Use Cases**:
- Measure promotion effectiveness (ROI = revenue ÷ marketing spend)
- Compare performance by promotion type
- Calculate incremental revenue from promotions
- Identify best-performing campaigns

**Special Handling**:
- Transactions without a promotion have `PromotionKey = -1`
- Use `WHERE PromotionKey != -1` to analyze only promoted sales
- Use `WHERE PromotionKey = -1` to analyze organic sales

---

## Common Query Patterns

### 1. Sales by Store and Region

```sql
SELECT
  s.StoreName,
  s.Region,
  COUNT(*) as transaction_count,
  SUM(f.Quantity) as total_units,
  SUM(f.TotalSales) as total_revenue,
  ROUND(AVG(f.TotalSales), 2) as avg_transaction_value
FROM gold.fact_sales f
JOIN gold.dim_store s ON f.StoreKey = s.StoreKey
GROUP BY s.StoreName, s.Region
ORDER BY total_revenue DESC
```

### 2. Product Performance by Category

```sql
SELECT
  p.Category,
  p.ProductName,
  COUNT(*) as transactions,
  SUM(f.Quantity) as units_sold,
  SUM(f.TotalSales) as revenue,
  ROUND(AVG(f.UnitPrice), 2) as avg_price
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.ProductKey = p.ProductKey
GROUP BY p.Category, p.ProductName
ORDER BY revenue DESC
```

### 3. Promotion Effectiveness

```sql
SELECT
  pr.PromotionType,
  pr.PromotionID,
  COUNT(*) as transactions,
  SUM(f.Quantity) as units,
  SUM(f.TotalSales) as revenue,
  SUM(f.DiscountAmount) as total_discount,
  ROUND(SUM(f.TotalSales) / pr.MarketingSpend, 2) as roi_multiplier
FROM gold.fact_sales f
JOIN gold.dim_promotion pr ON f.PromotionKey = pr.PromotionKey
WHERE f.PromotionKey != -1  -- Only promoted sales
GROUP BY pr.PromotionType, pr.PromotionID
ORDER BY revenue DESC
```

### 4. Daily Sales Trend

```sql
SELECT
  d.Date,
  d.MonthName,
  COUNT(*) as daily_transactions,
  SUM(f.Quantity) as daily_units,
  SUM(f.TotalSales) as daily_revenue
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.DateKey = d.DateKey
GROUP BY d.Date, d.MonthName
ORDER BY d.Date DESC
```

### 5. Sales by Day of Week

```sql
SELECT
  d.DayName,
  COUNT(*) as transactions,
  SUM(f.Quantity) as units,
  SUM(f.TotalSales) as revenue,
  ROUND(AVG(f.TotalSales), 2) as avg_transaction
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.DateKey = d.DateKey
GROUP BY d.DayName
ORDER BY
  CASE
    WHEN d.DayName = 'Sunday' THEN 1
    WHEN d.DayName = 'Monday' THEN 2
    WHEN d.DayName = 'Tuesday' THEN 3
    WHEN d.DayName = 'Wednesday' THEN 4
    WHEN d.DayName = 'Thursday' THEN 5
    WHEN d.DayName = 'Friday' THEN 6
    WHEN d.DayName = 'Saturday' THEN 7
  END
```

---

## Key Metrics & Measures

When querying the fact table, focus on these **additive measures**:

| Measure | Definition | Use Cases |
|---------|-----------|-----------|
| **TotalSales** | Revenue (quantity × unit price - discount) | Revenue analysis, ROI, profitability |
| **Quantity** | Units sold | Volume trends, inventory impact |
| **DiscountAmount** | Dollar value of discounts | Promotion cost analysis, margin impact |
| **UnitPrice** | Price per unit at transaction time | Pricing analysis, average price trends |
| **Count (*)** | Transaction count | Sales frequency, transaction trends |

---

## Filtering & Aggregation Guidelines

### Standard Filters

```sql
-- By Time Period (Month, Quarter, Year)
WHERE EXTRACT(YEAR FROM d.Date) = 2024
WHERE EXTRACT(MONTH FROM d.Date) IN (1, 2, 3)
WHERE d.Quarter = 1

-- By Store Attributes
WHERE s.Region = 'Northeast'
WHERE s.StoreType = 'Premium'
WHERE s.SquareFootage > 5000

-- By Product Attributes
WHERE p.Category = 'Electronics'
WHERE p.Brand = 'TechBrand'

-- By Promotion Status
WHERE pr.PromotionKey != -1  -- Only promoted sales
WHERE pr.PromotionType = 'Percentage Discount'
WHERE pr.StartDate <= CURRENT_DATE AND pr.EndDate >= CURRENT_DATE

-- By Sales Value
WHERE f.TotalSales > 50
WHERE f.Quantity >= 2
```

### Common Aggregations

```sql
-- By Time
GROUP BY d.Year, d.Quarter, d.MonthName
GROUP BY d.Date, d.DayName
GROUP BY EXTRACT(WEEK FROM d.Date)

-- By Store Hierarchy
GROUP BY s.Region
GROUP BY s.Region, s.StoreType, s.StoreName

-- By Product Hierarchy
GROUP BY p.Category
GROUP BY p.Category, p.Brand, p.ProductName

-- By Promotion
GROUP BY pr.PromotionType
```

---

## Pre-built Analytics Views

For common analytical scenarios, reference these pre-aggregated views:

- **`gold.sales_by_store_product`** - Sales metrics by store and product
- **`gold.daily_sales_by_store`** - Daily trends by store
- **`gold.product_performance`** - Product-level KPIs
- **`gold.store_performance`** - Store-level KPIs
- **`gold.promotion_effectiveness`** - Promotion ROI and performance
- **`gold.sales_trend_by_month`** - Monthly sales trends

These views provide pre-calculated aggregations for faster query performance.

---

## Data Quality & Assumptions

### Important Notes

1. **Promotion Key = -1**: Represents transactions with no promotion applied
2. **Type 1 SCD Dimensions**: Product, Store, and Promotion dimensions use Type 1 (overwrite) logic. Historical changes are not preserved.
3. **Surrogate Keys**: ProductKey, StoreKey, and PromotionKey are sequential integers generated from natural keys
4. **Degenerate Dimension**: UnitPrice is stored in fact table for historical accuracy
5. **Date Grain**: dim_date includes only dates with sales activity in the dataset
6. **Promotion Dates**: StartDate and EndDate in dim_promotion define promotion validity periods

### Data Lineage

```
ADLS Files (retail-in)
  ↓
Bronze Layer (raw data)
  ↓
Silver Layer (validated data)
  ↓
Gold Layer (Kimball star schema)
  ├── dim_date, dim_product, dim_store, dim_promotion
  └── fact_sales
```

---

## Example Questions for Genie

Test Genie's understanding by asking questions like:

1. **Store Analysis**
   - "Show total sales by region"
   - "Which Premium stores generated the most revenue?"
   - "Compare Northeast vs Midwest performance"

2. **Product Analysis**
   - "What are the top 5 products by revenue?"
   - "How many Electronics were sold?"
   - "Which brand has the highest average transaction value?"

3. **Temporal Analysis**
   - "What was revenue by month?"
   - "Did we sell more on weekends or weekdays?"
   - "Show daily sales trend"

4. **Promotion Analysis**
   - "Which promotion type drove the most sales?"
   - "What's the ROI of our Percentage Discount promotions?"
   - "How many transactions had a promotion applied?"

5. **Cross-Dimensional Analysis**
   - "Which region had the most promotion activity?"
   - "Show Electronics sales by store type"
   - "Compare promoted vs non-promoted sales"

---

## Database & Catalog Information

- **Development Catalog**: `retail_dev`
- **Production Catalog**: `retail_prod`
- **Gold Schema**: `gold`
- **Table Prefix**: All tables prefixed with dimension type (dim_*, fact_*)

When querying, use fully qualified table names:
- Development: `retail_dev.gold.fact_sales`
- Production: `retail_prod.gold.fact_sales`

---

## Summary

Your star schema is optimized for:
- ✓ Fast analytical queries with pre-joined dimension attributes
- ✓ Flexible aggregation across multiple dimensions
- ✓ Clear temporal, geographic, product, and promotional analysis
- ✓ Additive measures (TotalSales, Quantity, DiscountAmount)
- ✓ Pre-aggregated views for common scenarios

**Instruct Genie to always join dimensions to the fact table and aggregate measures appropriately for business insights.**
