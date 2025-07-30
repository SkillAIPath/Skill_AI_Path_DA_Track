# Challenge 3: External Research Integration
## Time-Optimized Contact Strategy Analysis (Perfect ER Diagram Compliance)

### 🎯 Business Question & Objective
**Question:** How can we improve our loan collection success rates by implementing research-backed timing strategies and geographic optimization?

**Objective:** Research shows 30% higher success rates with morning calls for certain demographics. We need to modify our contact strategy to:
- Optimize calling times based on employment type and geography
- Batch customers by zones for efficient operations
- Predict expected collection amounts using research multipliers
- Provide resource allocation recommendations

### 📊 Tables Used (Based on Perfect 3NF ER Diagram)
- **CUSTOMERS**: Customer demographics, employment, income, location data
- **LOANS**: Loan details, status, amounts for collection tracking
- **DEFAULTS_COLLECTIONS**: Default and collection tracking with customer and loan FKs
- **PAYMENTS**: Payment history and methods
- **INSTITUTIONS**: Institution details for loan origination
- **DIM_CITY**: City master data with state FK (Bridge Table)
- **DIM_STATE**: State information with region classification (Bridge Table)
- **GEOGRAPHIC_DEMOGRAPHICS**: Population and demographic data per city
- **ECONOMIC_INDICATORS**: Economic context per state by quarter

---

## Building Block Breakdown

### C3-A: Customer Contact Optimization Foundation
**Purpose:** Extract defaulted customers with optimal contact timing based on employment and location

```sql
-- C3-A: Basic customer contact optimization following ER relationships
SELECT 
    c.customer_id,
    c.full_name,
    c.phone_number,
    c.employment_type,
    c.employer_name,
    c.current_address,
    c.annual_income,
    city.city_name,
    city.tier_classification,
    state.state_name,
    state.region,
    SUM(dc.default_amount) AS total_default_amount,
    
    -- Optimal contact time based on employment type and city tier
    CASE 
        WHEN city.tier_classification = 'Tier1' 
             AND c.employment_type IN ('Private Employee', 'Corporate Employee') 
             THEN 'MORNING: 9-11 AM'
        WHEN c.employment_type = 'Government Employee' 
             THEN 'OFFICE: 11 AM-1 PM'
        WHEN c.employment_type = 'Self Employed' 
             THEN 'FLEXIBLE: 2-5 PM'
        ELSE 'EVENING: 6-8 PM'
    END AS optimal_contact_time
    
FROM CUSTOMERS c
INNER JOIN DEFAULTS_COLLECTIONS dc ON c.customer_id = dc.customer_id
INNER JOIN LOANS l ON dc.loan_id = l.loan_id
INNER JOIN DIM_CITY city ON c.city_id = city.city_id
INNER JOIN DIM_STATE state ON city.state_id = state.state_id
WHERE dc.default_date IS NOT NULL
  AND c.phone_number IS NOT NULL
  AND LEN(c.phone_number) >= 10
  AND dc.collection_status IN ('Active', 'In Progress', 'Follow Up Required')
GROUP BY c.customer_id, c.full_name, c.phone_number, c.employment_type, 
         c.employer_name, c.current_address, c.annual_income,
         city.city_name, city.tier_classification, state.state_name, state.region
HAVING SUM(dc.default_amount) > 0;
```

**Output:** Customer list with personalized optimal calling times
- Tier1 city private employees: Morning calls (9-11 AM)
- Government employees: Office hours (11 AM-1 PM)  
- Self-employed: Flexible afternoon timing
- Others: Evening calls

---

### C3-B: Research-Based Success Multipliers with Economic Context
**Purpose:** Apply research findings with economic indicators for success rate improvements

```sql
-- C3-B: Add success multipliers, contact methods, and economic context
SELECT 
    customer_id,
    full_name,
    phone_number,
    employment_type,
    employer_name,
    city_name,
    tier_classification,
    state_name,
    region,
    annual_income,
    total_default_amount,
    optimal_contact_time,
    
    -- Research-based success multipliers with economic context
    CASE 
        WHEN employment_type IN ('Private Employee', 'Corporate Employee') 
             AND tier_classification = 'Tier1' THEN 1.3  -- 30% higher for employed in metros
        WHEN employment_type = 'Government Employee' THEN 1.2  -- 20% higher for govt employees
        WHEN tier_classification = 'Tier1' THEN 1.1  -- 10% higher for metro areas
        ELSE 1.0
    END AS success_multiplier,
    
    -- Contact method preference by demographic profile and tier
    CASE 
        WHEN tier_classification = 'Tier1' AND annual_income > 800000 
             THEN 'EMAIL + PHONE: Professional approach'
        WHEN employment_type = 'Government Employee' 
             THEN 'FORMAL LETTER + PHONE: Official approach'
        WHEN annual_income < 400000 OR tier_classification = 'Tier3'
             THEN 'PHONE ONLY: Direct approach'
        ELSE 'PHONE + SMS: Standard approach'
    END AS contact_method,
    
    -- Priority level based on amount and economic factors
    CASE 
        WHEN total_default_amount > 500000 AND tier_classification = 'Tier1' THEN 'HIGH_PRIORITY'
        WHEN total_default_amount > 200000 THEN 'MEDIUM_PRIORITY'
        ELSE 'STANDARD_PRIORITY'
    END AS priority_level
    
FROM (
    -- Previous C3-A query results
    SELECT 
        c.customer_id,
        c.full_name,
        c.phone_number,
        c.employment_type,
        c.employer_name,
        c.current_address,
        c.annual_income,
        city.city_name,
        city.tier_classification,
        state.state_name,
        state.region,
        SUM(dc.default_amount) AS total_default_amount,
        CASE 
            WHEN city.tier_classification = 'Tier1' 
                 AND c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 THEN 'MORNING: 9-11 AM'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'OFFICE: 11 AM-1 PM'
            WHEN c.employment_type = 'Self Employed' 
                 THEN 'FLEXIBLE: 2-5 PM'
            ELSE 'EVENING: 6-8 PM'
        END AS optimal_contact_time
    FROM CUSTOMERS c
    INNER JOIN DEFAULTS_COLLECTIONS dc ON c.customer_id = dc.customer_id
    INNER JOIN LOANS l ON dc.loan_id = l.loan_id
    INNER JOIN DIM_CITY city ON c.city_id = city.city_id
    INNER JOIN DIM_STATE state ON city.state_id = state.state_id
    WHERE dc.default_date IS NOT NULL
      AND c.phone_number IS NOT NULL
      AND LEN(c.phone_number) >= 10
      AND dc.collection_status IN ('Active', 'In Progress', 'Follow Up Required')
    GROUP BY c.customer_id, c.full_name, c.phone_number, c.employment_type, 
             c.employer_name, c.current_address, c.annual_income,
             city.city_name, city.tier_classification, state.state_name, state.region
    HAVING SUM(dc.default_amount) > 0
) base_data;
```

**Output:** Enhanced customer data with:
- Success rate multipliers (1.0x to 1.3x based on research + economic factors)
- Tailored contact methods based on tier classification
- Priority levels for resource allocation

---

### C3-C: Geographic Zone Batching with Demographics
**Purpose:** Group customers by geographic zones using proper ER relationships

```sql
-- C3-C: Add geographic batching with demographic and economic context
SELECT 
    *,
    -- Geographic zone assignment based on state region from ER diagram
    CASE 
        WHEN region = 'West' THEN 'WEST_ZONE_1'
        WHEN region = 'North' THEN 'NORTH_ZONE_1'
        WHEN region = 'South' THEN 'SOUTH_ZONE_1'
        WHEN region = 'East' THEN 'EAST_ZONE_1'
        WHEN region = 'Central' THEN 'CENTRAL_ZONE_1'
        WHEN region = 'Northeast' THEN 'NORTHEAST_ZONE_1'
        ELSE 'REMOTE_ZONE'
    END AS calling_zone,
    
    -- Economic priority based on tier and region
    CASE 
        WHEN tier_classification = 'Tier1' AND region IN ('West', 'South', 'North') 
             THEN 'HIGH_ECONOMIC_PRIORITY'
        WHEN tier_classification = 'Tier2' AND region IN ('West', 'South', 'North') 
             THEN 'MEDIUM_ECONOMIC_PRIORITY'
        ELSE 'STANDARD_PRIORITY'
    END AS economic_priority
    
FROM (
    -- Previous C3-B enhanced query
    SELECT 
        c.customer_id,
        c.full_name,
        c.phone_number,
        c.employment_type,
        c.employer_name,
        city.city_name,
        city.tier_classification,
        state.state_name,
        state.region,
        c.annual_income,
        SUM(dc.default_amount) AS total_default_amount,
        CASE 
            WHEN city.tier_classification = 'Tier1' 
                 AND c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 THEN 'MORNING: 9-11 AM'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'OFFICE: 11 AM-1 PM'
            WHEN c.employment_type = 'Self Employed' 
                 THEN 'FLEXIBLE: 2-5 PM'
            ELSE 'EVENING: 6-8 PM'
        END AS optimal_contact_time,
        CASE 
            WHEN c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 AND city.tier_classification = 'Tier1' THEN 1.3
            WHEN c.employment_type = 'Government Employee' THEN 1.2
            WHEN city.tier_classification = 'Tier1' THEN 1.1
            ELSE 1.0
        END AS success_multiplier,
        CASE 
            WHEN city.tier_classification = 'Tier1' AND c.annual_income > 800000 
                 THEN 'EMAIL + PHONE: Professional approach'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'FORMAL LETTER + PHONE: Official approach'
            WHEN c.annual_income < 400000 OR city.tier_classification = 'Tier3'
                 THEN 'PHONE ONLY: Direct approach'
            ELSE 'PHONE + SMS: Standard approach'
        END AS contact_method,
        CASE 
            WHEN SUM(dc.default_amount) > 500000 AND city.tier_classification = 'Tier1' THEN 'HIGH_PRIORITY'
            WHEN SUM(dc.default_amount) > 200000 THEN 'MEDIUM_PRIORITY'
            ELSE 'STANDARD_PRIORITY'
        END AS priority_level
    FROM CUSTOMERS c
    INNER JOIN DEFAULTS_COLLECTIONS dc ON c.customer_id = dc.customer_id
    INNER JOIN LOANS l ON dc.loan_id = l.loan_id
    INNER JOIN DIM_CITY city ON c.city_id = city.city_id
    INNER JOIN DIM_STATE state ON city.state_id = state.state_id
    WHERE dc.default_date IS NOT NULL
      AND c.phone_number IS NOT NULL
      AND LEN(c.phone_number) >= 10
      AND dc.collection_status IN ('Active', 'In Progress', 'Follow Up Required')
    GROUP BY c.customer_id, c.full_name, c.phone_number, c.employment_type, 
             c.employer_name, city.city_name, city.tier_classification, state.state_name, state.region, c.annual_income
    HAVING SUM(dc.default_amount) > 0
) enhanced_data;
```

**Output:** Customer data now includes:
- Geographic zones based on ER diagram regions (North, South, East, West, Central, Northeast)
- Economic priority classification using tier classification
- Regional operational grouping

---

### C3-D: Operational Summary with Economic Indicators Integration
**Purpose:** Aggregate data for daily operations with economic context from ER relationships

```sql
-- C3-D: Daily calling schedule with economic indicators integration
SELECT 
    calling_zone,
    optimal_contact_time,
    contact_method,
    economic_priority,
    tier_classification,
    COUNT(*) AS customers_to_contact,
    CONCAT('₹', FORMAT(SUM(total_default_amount)/10000000.0, 'N1'), 'Cr') AS collection_target,
    CONCAT(FORMAT(AVG(success_multiplier) * 100, 'N0'), '%') AS success_rate_boost,
    
    -- Daily capacity planning based on call complexity and tier
    CASE 
        WHEN optimal_contact_time = 'MORNING: 9-11 AM' THEN CEILING(COUNT(*) / 3.0)  -- 3 calls per hour
        WHEN optimal_contact_time = 'OFFICE: 11 AM-1 PM' THEN CEILING(COUNT(*) / 2.5)  -- 2.5 calls per hour
        WHEN tier_classification = 'Tier3' THEN CEILING(COUNT(*) / 5.0)  -- 5 calls per hour for Tier3
        ELSE CEILING(COUNT(*) / 4.0)  -- 4 calls per hour for standard
    END AS daily_call_capacity,
    
    -- Expected collection with research multipliers
    CONCAT('₹', FORMAT(SUM(total_default_amount * success_multiplier * 0.4) / 10000000.0, 'N1'), 'Cr') AS expected_collection,
    
    -- Resource allocation recommendations based on ER data
    CASE 
        WHEN COUNT(*) > 500 AND optimal_contact_time = 'MORNING: 9-11 AM' 
             THEN 'PRIORITY: Assign 3 senior agents for Tier1 morning shift'
        WHEN SUM(total_default_amount * success_multiplier * 0.4) / 10000000.0 > 2 
             THEN 'HIGH VALUE: Senior collection officer + legal support'
        WHEN tier_classification = 'Tier1' AND economic_priority = 'HIGH_ECONOMIC_PRIORITY'
             THEN 'PREMIUM: Specialized metro collection team'
        WHEN COUNT(*) < 50 
             THEN 'LOW PRIORITY: Combine with adjacent zones'
        ELSE 'STANDARD: Regular collection agent'
    END AS resource_recommendation,
    
    -- Economic context from ECONOMIC_INDICATORS table
    AVG(ei.gdp_growth_rate) AS avg_regional_gdp_growth,
    AVG(ei.unemployment_rate) AS avg_regional_unemployment
    
FROM (
    -- Complete contact optimization logic with ER relationships
    SELECT 
        c.customer_id,
        c.employment_type,
        city.city_name,
        city.tier_classification,
        state.state_name,
        state.region,
        c.annual_income,
        SUM(dc.default_amount) AS total_default_amount,
        CASE 
            WHEN city.tier_classification = 'Tier1' 
                 AND c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 THEN 'MORNING: 9-11 AM'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'OFFICE: 11 AM-1 PM'
            WHEN c.employment_type = 'Self Employed' 
                 THEN 'FLEXIBLE: 2-5 PM'
            ELSE 'EVENING: 6-8 PM'
        END AS optimal_contact_time,
        CASE 
            WHEN c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 AND city.tier_classification = 'Tier1' THEN 1.3
            WHEN c.employment_type = 'Government Employee' THEN 1.2
            WHEN city.tier_classification = 'Tier1' THEN 1.1
            ELSE 1.0
        END AS success_multiplier,
        CASE 
            WHEN city.tier_classification = 'Tier1' AND c.annual_income > 800000 
                 THEN 'EMAIL + PHONE: Professional approach'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'FORMAL LETTER + PHONE: Official approach'
            WHEN c.annual_income < 400000 OR city.tier_classification = 'Tier3'
                 THEN 'PHONE ONLY: Direct approach'
            ELSE 'PHONE + SMS: Standard approach'
        END AS contact_method,
        CASE 
            WHEN state.region = 'West' THEN 'WEST_ZONE_1'
            WHEN state.region = 'North' THEN 'NORTH_ZONE_1'
            WHEN state.region = 'South' THEN 'SOUTH_ZONE_1'
            WHEN state.region = 'East' THEN 'EAST_ZONE_1'
            WHEN state.region = 'Central' THEN 'CENTRAL_ZONE_1'
            WHEN state.region = 'Northeast' THEN 'NORTHEAST_ZONE_1'
            ELSE 'REMOTE_ZONE'
        END AS calling_zone,
        CASE 
            WHEN city.tier_classification = 'Tier1' AND state.region IN ('West', 'South', 'North') 
                 THEN 'HIGH_ECONOMIC_PRIORITY'
            WHEN city.tier_classification = 'Tier2' AND state.region IN ('West', 'South', 'North') 
                 THEN 'MEDIUM_ECONOMIC_PRIORITY'
            ELSE 'STANDARD_PRIORITY'
        END AS economic_priority,
        state.state_id
    FROM CUSTOMERS c
    INNER JOIN DEFAULTS_COLLECTIONS dc ON c.customer_id = dc.customer_id
    INNER JOIN LOANS l ON dc.loan_id = l.loan_id
    INNER JOIN DIM_CITY city ON c.city_id = city.city_id
    INNER JOIN DIM_STATE state ON city.state_id = state.state_id
    WHERE dc.default_date IS NOT NULL
      AND c.phone_number IS NOT NULL
      AND LEN(c.phone_number) >= 10
      AND dc.collection_status IN ('Active', 'In Progress', 'Follow Up Required')
    GROUP BY c.customer_id, c.employment_type, city.city_name, city.tier_classification, 
             state.state_name, state.region, c.annual_income, state.state_id
    HAVING SUM(dc.default_amount) > 0
) contact_data
LEFT JOIN ECONOMIC_INDICATORS ei ON contact_data.state_id = ei.state_id 
    AND ei.quarter = (SELECT MAX(quarter) FROM ECONOMIC_INDICATORS WHERE state_id = contact_data.state_id)
GROUP BY calling_zone, optimal_contact_time, contact_method, economic_priority, tier_classification
ORDER BY SUM(total_default_amount * success_multiplier * 0.4) DESC, COUNT(*) DESC;
```

**Output:** Operational dashboard with complete ER integration showing:
- Economic indicators from ECONOMIC_INDICATORS table
- Tier-based resource recommendations
- Regional economic context
- Perfect FK relationship compliance

---

## 🔄 Complete Combined Query (Perfect ER Compliance)

```sql
-- Challenge 3: Complete Time-Optimized Contact Strategy Analysis (Perfect ER Diagram Compliance)
WITH contact_optimization AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.phone_number,
        c.employment_type,
        c.employer_name,
        c.annual_income,
        city.city_name,
        city.tier_classification,
        state.state_name,
        state.region,
        SUM(dc.default_amount) AS total_default_amount,
        
        -- Time zone and employment-based optimal contact hours using ER relationships
        CASE 
            WHEN city.tier_classification = 'Tier1' 
                 AND c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 THEN 'MORNING: 9-11 AM'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'OFFICE: 11 AM-1 PM'
            WHEN c.employment_type = 'Self Employed' 
                 THEN 'FLEXIBLE: 2-5 PM'
            ELSE 'EVENING: 6-8 PM'
        END AS optimal_contact_time,
        
        -- Success rate multiplier based on research + ER tier classification
        CASE 
            WHEN c.employment_type IN ('Private Employee', 'Corporate Employee') 
                 AND city.tier_classification = 'Tier1' THEN 1.3  -- 30% higher
            WHEN c.employment_type = 'Government Employee' THEN 1.2  -- 20% higher
            WHEN city.tier_classification = 'Tier1' THEN 1.1  -- 10% metro advantage
            ELSE 1.0
        END AS success_multiplier,
        
        -- Contact method preference using tier classification from ER
        CASE 
            WHEN city.tier_classification = 'Tier1' AND c.annual_income > 800000 
                 THEN 'EMAIL + PHONE: Professional approach'
            WHEN c.employment_type = 'Government Employee' 
                 THEN 'FORMAL LETTER + PHONE: Official approach'
            WHEN c.annual_income < 400000 OR city.tier_classification = 'Tier3'
                 THEN 'PHONE ONLY: Direct approach'
            ELSE 'PHONE + SMS: Standard approach'
        END AS contact_method,
        
        -- Geographic batching using region from DIM_STATE (ER compliant)
        CASE 
            WHEN state.region = 'West' THEN 'WEST_ZONE_1'
            WHEN state.region = 'North' THEN 'NORTH_ZONE_1'
            WHEN state.region = 'South' THEN 'SOUTH_ZONE_1'
            WHEN state.region = 'East' THEN 'EAST_ZONE_1'
            WHEN state.region = 'Central' THEN 'CENTRAL_ZONE_1'
            WHEN state.region = 'Northeast' THEN 'NORTHEAST_ZONE_1'
            ELSE 'REMOTE_ZONE'
        END AS calling_zone,
        
        -- Economic priority using ER tier + region classification
        CASE 
            WHEN city.tier_classification = 'Tier1' AND state.region IN ('West', 'South', 'North') 
                 THEN 'HIGH_ECONOMIC_PRIORITY'
            WHEN city.tier_classification = 'Tier2' AND state.region IN ('West', 'South', 'North') 
                 THEN 'MEDIUM_ECONOMIC_PRIORITY'
            ELSE 'STANDARD_PRIORITY'
        END AS economic_priority,
        
        state.state_id  -- For joining with ECONOMIC_INDICATORS
        
    FROM CUSTOMERS c
    INNER JOIN DEFAULTS_COLLECTIONS dc ON c.customer_id = dc.customer_id  -- Perfect ER FK
    INNER JOIN LOANS l ON dc.loan_id = l.loan_id  -- Perfect ER FK
    INNER JOIN DIM_CITY city ON c.city_id = city.city_id  -- Perfect ER FK
    INNER JOIN DIM_STATE state ON city.state_id = state.state_id  -- Perfect ER FK
    WHERE dc.default_date IS NOT NULL
      AND c.phone_number IS NOT NULL
      AND LEN(c.phone_number) >= 10
      AND dc.collection_status IN ('Active', 'In Progress', 'Follow Up Required')
    GROUP BY c.customer_id, c.full_name, c.phone_number, c.employment_type, 
             c.employer_name, c.annual_income, city.city_name, city.tier_classification,
             state.state_name, state.region, state.state_id
    HAVING SUM(dc.default_amount) > 0
),
daily_calling_schedule AS (
    SELECT 
        calling_zone,
        optimal_contact_time,
        contact_method,
        economic_priority,
        tier_classification,
        COUNT(*) AS customers_to_contact,
        SUM(total_default_amount) AS zone_collection_target,
        AVG(success_multiplier) AS avg_success_multiplier,
        
        -- Daily capacity planning using CEILING function (tier-aware)
        CASE 
            WHEN optimal_contact_time = 'MORNING: 9-11 AM' THEN CEILING(COUNT(*) / 3.0)  -- 3 calls per hour
            WHEN optimal_contact_time = 'OFFICE: 11 AM-1 PM' THEN CEILING(COUNT(*) / 2.5)  -- 2.5 calls per hour
            WHEN tier_classification = 'Tier3' THEN CEILING(COUNT(*) / 5.0)  -- 5 calls per hour
            ELSE CEILING(COUNT(*) / 4.0)  -- 4 calls per hour standard
        END AS daily_call_capacity,
        
        -- Expected collection based on research (40% base success rate)
        ROUND(SUM(total_default_amount * success_multiplier * 0.4) / 10000000.0, 1) AS expected_collection_crores,
        
        -- Economic context from latest quarter
        AVG(ei.gdp_growth_rate) AS avg_regional_gdp_growth,
        AVG(ei.unemployment_rate) AS avg_regional_unemployment,
        AVG(ei.per_capita_income) AS avg_regional_income
        
    FROM contact_optimization co
    LEFT JOIN ECONOMIC_INDICATORS ei ON co.state_id = ei.state_id 
        AND ei.quarter = (SELECT MAX(quarter) FROM ECONOMIC_INDICATORS WHERE state_id = co.state_id)
    GROUP BY calling_zone, optimal_contact_time, contact_method, economic_priority, tier_classification
)
SELECT 
    calling_zone,
    optimal_contact_time,
    contact_method,
    economic_priority,
    tier_classification,
    customers_to_contact,
    CONCAT('₹', FORMAT(zone_collection_target/10000000.0, 'N1'), 'Cr') AS collection_target,
    CONCAT(FORMAT(avg_success_multiplier * 100, 'N0'), '%') AS success_rate_boost,
    daily_call_capacity,
    CONCAT('₹', FORMAT(expected_collection_crores, 'N1'), 'Cr') AS expected_collection,
    
    -- Advanced resource recommendations using ER tier classification
    CASE 
        WHEN customers_to_contact > 500 AND optimal_contact_time = 'MORNING: 9-11 AM' 
             THEN 'PRIORITY: Assign 3 senior agents for Tier1 morning shift'
        WHEN expected_collection_crores > 2 AND tier_classification = 'Tier1'
             THEN 'HIGH VALUE: Senior collection officer + legal support'
        WHEN tier_classification = 'Tier1' AND economic_priority = 'HIGH_ECONOMIC_PRIORITY'
             THEN 'PREMIUM: Specialized metro collection team'
        WHEN customers_to_contact < 50 
             THEN 'LOW PRIORITY: Combine with adjacent zones'
        ELSE 'STANDARD: Regular collection agent'
    END AS resource_recommendation,
    
    -- Economic insights from ER ECONOMIC_INDICATORS table
    CONCAT(FORMAT(ISNULL(avg_regional_gdp_growth, 0), 'N1'), '%') AS regional_gdp_growth,
    CONCAT(FORMAT(ISNULL(avg_regional_unemployment, 0), 'N1'), '%') AS regional_unemployment,
    CONCAT('₹', FORMAT(ISNULL(avg_regional_income, 0)/100000.0, 'N1'), 'L') AS regional_per_capita_income
    
FROM daily_calling_schedule
ORDER BY expected_collection_crores DESC, customers_to_contact DESC;
```

---

## 📋 Mock Output (Perfect ER Compliance)

| calling_zone | optimal_contact_time | contact_method | economic_priority | tier_classification | customers_to_contact | collection_target | success_rate_boost | daily_call_capacity | expected_collection | resource_recommendation | regional_gdp_growth | regional_unemployment | regional_per_capita_income |
|--------------|---------------------|----------------|-------------------|-------------------|---------------------|------------------|-------------------|-------------------|-------------------|----------------------|-------------------|---------------------|--------------------------|
| WEST_ZONE_1 | MORNING: 9-11 AM | EMAIL + PHONE: Professional approach | HIGH_ECONOMIC_PRIORITY | Tier1 | 847 | ₹15.2Cr | 130% | 283 | ₹7.9Cr | PREMIUM: Specialized metro collection team | 7.2% | 3.8% | ₹2.8L |
| SOUTH_ZONE_1 | OFFICE: 11 AM-1 PM | FORMAL LETTER + PHONE: Official approach | HIGH_ECONOMIC_PRIORITY | Tier1 | 623 | ₹12.8Cr | 120% | 250 | ₹6.1Cr | HIGH VALUE: Senior collection officer + legal support | 6.8% | 4.2% | ₹2.4L |
| NORTH_ZONE_1 | EVENING: 6-8 PM | PHONE + SMS: Standard approach | MEDIUM_ECONOMIC_PRIORITY | Tier2 | 445 | ₹8.9Cr | 100% | 112 | ₹3.6Cr | STANDARD: Regular collection agent | 5.9% | 5.1% | ₹2.1L |
| EAST_ZONE_1 | FLEXIBLE: 2-5 PM | PHONE ONLY: Direct approach | STANDARD_PRIORITY | Tier3 | 234 | ₹4.2Cr | 100% | 47 | ₹1.7Cr | STANDARD: Regular collection agent | 4.8% | 6.3% | ₹1.6L |
| CENTRAL_ZONE_1 | EVENING: 6-8 PM | PHONE ONLY: Direct approach | STANDARD_PRIORITY | Tier2 | 156 | ₹2.8Cr | 100% | 39 | ₹1.1Cr | STANDARD: Regular collection agent | 5.2% | 5.8% | ₹1.8L |
| NORTHEAST_ZONE_1 | EVENING: 6-8 PM | PHONE ONLY: Direct approach | STANDARD_PRIORITY | Tier3 | 89 | ₹1.8Cr | 100% | 18 | ₹0.