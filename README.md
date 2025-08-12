# Local Food Wastage Management System

A Streamlit application and SQL-backed workflow that connects surplus food providers with people and NGOs in need, reduces food waste, and surfaces actionable insights from data.

## Why this matters
Food wastage is widespread while food insecurity persists. This project builds a lightweight, local-first platform to:
- Let restaurants/households **list surplus food**
- Let NGOs/individuals **discover and claim food**
- Store data in **SQL** for durability and analytics
- Provide a **Streamlit** UI for browsing, CRUD, and insights

---

## Business Use Cases
- Matchmaking: connect surplus providers to seekers via a structured platform
- Waste reduction: redistribute excess efficiently
- Accessibility: geolocation-aware discovery of nearby food
- Decision support: SQL-powered analysis of trends and bottlenecks

---

## Features
- **Browse & Filter** donations by city/provider
- **CRUD** minimal forms to add/delete donation records
- **Reports & Insights**: 15 prebuilt SQL queries (providers, demand hot-spots, supply–demand gap, wastage trends, retention, etc.)
- **Charts** for key time series (donations, wastage, retention)

---

## Approach

1. **Data Preparation**  
   Use a cleaned dataset of donation records; enforce consistent types and timestamps.

2. **Database Creation**  
   Store food availability, providers, cities in SQL tables and expose CRUD for updates.

3. **Data Analysis**  
   Identify wastage trends by category/location, analyze demand peaks, and compute fill/wastage rates.

4. **Application Development**  
   Streamlit UI with:
   - Output of **15 SQL queries**
   - Filters by city/provider/food type (extend as needed)
   - Contact details display (add a column in your schema)

5. **Deployment**  
   Deploy to Streamlit Community Cloud (public), or Docker → Cloud Run/Render/Railway (private), or Snowflake Streamlit.

---

## Architecture & Data Flow

**Storage:** SQL database (PostgreSQL recommended).  
**Processing:** SQL + pandas for aggregation and reporting.  
**Interface:** Streamlit app for providers and seekers.

Suggested core tables (simplify/extend as needed):
- `food_providers(id, name, city_id, created_at)`
- `cities(id, name)`
- `donations(id, provider_id, city_id, quantity_kg, donated_at)`
- `claims(id, city_id, quantity_kg, claimed_at)`
- `wastage(id, city_id, quantity_kg, reason, reported_at)`
- *(optional)* `items(id, donation_id, category, quantity_kg)`

Indexing tips:
- `donations(city_id, donated_at)`, `claims(city_id, claimed_at)`, `wastage(city_id, reported_at)`, `donations(provider_id)`

---
