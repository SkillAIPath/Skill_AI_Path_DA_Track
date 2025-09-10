# 🚨 V1 - Customer Retention Crisis Analysis
## Days 1-5: Python Fundamentals + Business Problem Solving

---

## 📊 **CRISIS OVERVIEW**

**RetailMax is losing customers at an alarming rate!**

- **Current Situation:** Customer retention dropped from 72% to 55% in 6 months
- **Financial Impact:** ₹18 crores revenue at immediate risk
- **Timeline:** 30 days to identify root causes and propose solutions
- **Your Role:** Data Analyst hired to solve this crisis using Python

---

## 🎯 **BUSINESS PROBLEM STATEMENT**

### **The Challenge:**
RetailMax's executive team is panicking. Monthly board meetings show declining customer loyalty, increasing churn rates, and revenue loss. The Marketing Head believes it's a pricing issue, the Customer Success Head thinks it's service quality, and the Product Head suspects it's competition.

**Nobody knows the real reason. That's your job to figure out.**

### **What Business Needs to Know:**
1. **Root Cause Analysis:** Why are customers leaving?
2. **Segment Analysis:** Which customer groups are most affected?
3. **Geographic Impact:** Are specific cities/regions more affected?
4. **Product Analysis:** Which categories are losing customers?
5. **Revenue Recovery:** How much can we recover and how?

### **Success Criteria:**
- **Immediate:** Present findings to CEO within 5 days
- **Actionable:** Provide specific recommendations, not just insights
- **Measurable:** Quantify impact and recovery potential
- **Executive-Ready:** Charts and insights suitable for board presentation

---

## 📋 **TECHNICAL REQUIREMENTS**

### **Data Sources:**
- `customers.csv` - Customer demographic and registration data
- `orders.csv` - Complete transaction history
- `customer_behavior.csv` - Website interaction data
- `support_tickets.csv` - Customer service interactions
- `product_catalog.csv` - Complete product information and category details
- `sessions.csv` - User session data and website engagement metrics
- `transactions.csv` - Detailed payment and billing transaction records
- `billing_events.csv` - Payment failures, refunds, and billing issue tracking
- `campaign_performance.csv` - Marketing campaign effectiveness and ROI data
- `campaigns.csv` - Marketing campaign details and targeting information
- `churn_labels.csv` - Customer churn status and prediction labels
- `customer_feedback.csv` - Customer reviews, ratings, and satisfaction surveys

### **Tools & Environment:**
- **Primary:** Jupyter Notebook + Python 3.11
- **Libraries:** pandas, numpy, matplotlib, seaborn, plotly
- **Output:** Streamlit dashboard for final presentation

### **Deliverables:**
1. **Data Analysis Report:** Comprehensive Python notebook
2. **Executive Summary:** Key findings and recommendations
3. **Interactive Dashboard:** Streamlit app for stakeholder review
4. **Presentation:** 10-slide deck for CEO presentation

---

## 🔍 **DETAILED ANALYSIS REQUIREMENTS**

### **Day 1-2: Data Exploration & Cleaning**
**Learning Focus:** Python basics, pandas fundamentals, data quality assessment

**Tasks:**
- Load and inspect all datasets
- Identify data quality issues (missing values, duplicates, outliers)
- Understand customer journey from registration to churn
- Create data dictionary and basic statistics

**Key Questions:**
- How many customers do we have in total?
- What's the data quality like?
- What time period does our data cover?
- What are the key customer attributes?

**Expected Python Skills:**
```python
# Basic data loading and inspection
import pandas as pd
import numpy as np

# Data quality assessment
customers.info()
customers.describe()
customers.isnull().sum()

# Basic aggregations
customers.groupby('city').size()
orders.groupby('customer_id').agg({'order_value': 'sum'})
```

### **Day 3: Customer Segmentation Analysis**
**Learning Focus:** Advanced pandas operations, groupby, customer analytics

**Tasks:**
- Segment customers by value (High, Medium, Low)
- Analyze churn rates by segment
- Calculate Customer Lifetime Value (CLV)
- Identify most valuable customer groups

**Key Questions:**
- Which customer segments have highest churn?
- How does CLV vary by segment?
- What's the geographic distribution of valuable customers?
- Which segments should we prioritize for retention?

**Expected Python Skills:**
```python
# Customer segmentation
customers['segment'] = pd.cut(customers['total_spent'], 
                             bins=[0, 15000, 50000, float('inf')],
                             labels=['Low', 'Medium', 'High'])

# Churn analysis
churn_by_segment = customers.groupby('segment')['churned'].mean()

# CLV calculation
customers['clv'] = customers['total_spent'] / customers['tenure_months']
```

### **Day 4: Geographic & Product Analysis**
**Learning Focus:** Multi-dimensional analysis, visualization, business insights

**Tasks:**
- Analyze churn rates by city and region
- Identify product categories losing customers
- Examine seasonal patterns in customer behavior
- Calculate revenue impact by geography and product

**Key Questions:**
- Which cities have highest churn rates?
- Are certain product categories driving churn?
- Is there a seasonal pattern to customer loss?
- How much revenue is at risk in each city?

**Expected Python Skills:**
```python
# Geographic analysis
city_churn = customers.groupby('city').agg({
    'churned': 'mean',
    'total_spent': 'sum',
    'customer_id': 'count'
})

# Product analysis
product_churn = orders.merge(customers, on='customer_id')\
                     .groupby('product_category')['churned'].mean()

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns
```

### **Day 5: Insights & Recommendations**
**Learning Focus:** Business storytelling, executive communication, actionable insights

**Tasks:**
- Synthesize findings into executive summary
- Calculate revenue recovery potential
- Develop specific action recommendations
- Create interactive dashboard for presentation

**Key Questions:**
- What's the single biggest driver of churn?
- How much revenue can we recover?
- What are the top 3 actions to take immediately?
- How do we monitor progress?

**Expected Python Skills:**
```python
# Revenue impact calculation
revenue_at_risk = customers[customers['at_risk'] == True]['clv'].sum()

# Streamlit dashboard
import streamlit as st
st.title('RetailMax Customer Retention Crisis')
st.metric('Revenue at Risk', f'₹{revenue_at_risk/10000000:.1f} crores')
```

---

## 📊 **EXPECTED BUSINESS OUTCOMES**

### **Immediate Insights (What you'll discover):**
- **Primary Churn Driver:** One clear reason customers are leaving
- **High-Risk Segments:** Specific customer groups to focus on
- **Geographic Hotspots:** Cities requiring immediate attention
- **Product Issues:** Categories needing urgent fixes

### **Actionable Recommendations:**
1. **Immediate Actions:** What to do in next 7 days
2. **Medium-term Strategy:** 30-day improvement plan
3. **Long-term Prevention:** How to avoid future churn crises
4. **Resource Allocation:** Where to invest retention efforts

### **Measurable Targets:**
- **Churn Reduction:** From 55% to 45% within 30 days
- **Revenue Recovery:** ₹8-12 crores in first quarter
- **Customer Satisfaction:** Improve NPS score by 15 points
- **Operational Efficiency:** Reduce manual reporting by 70%

---

## 🎯 **SUCCESS METRICS**

### **Technical Assessment:**
- **Code Quality:** Professional, well-documented Python code
- **Analysis Depth:** Comprehensive exploration of all data sources
- **Visualization:** Clear, executive-ready charts and graphs
- **Presentation:** Compelling business story with data support

### **Business Impact:**
- **Insight Quality:** Actionable recommendations, not just observations
- **Executive Communication:** Can present findings to CEO confidently
- **Problem-Solving:** Identifies root causes, not just symptoms
- **Strategic Thinking:** Links data insights to business strategy

---

## ⚠️ **IMPORTANT REMINDERS**

1. **This is a real business crisis** - treat it seriously
2. **Focus on business impact** - not just technical skills
3. **Present like a consultant** - your recommendations will be implemented
4. **Think like an executive** - what would you do if this was your company?
5. **Code professionally** - others will read and build upon your work

**Your analysis will determine if RetailMax survives this crisis. Make it count.**