#!/bin/bash
# File: setup.sh
#
# Run this once after cloning the repo to prepare your local environment.
# Usage: bash setup.sh

set -e

echo "🐟 Seafood Trade Intelligence — local setup"
echo "============================================"

# 1. Python packages
echo ""
echo "📦 Installing Python packages..."
pip install -r requirements.txt

# 2. .env file
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo ""
    echo "⚠️  Created .env from .env.example"
    echo "   → Open .env and fill in your ANTHROPIC_API_KEY and Google Cloud project details"
else
    echo "✅ .env already exists"
fi

# 3. credentials reminder
if [ ! -f "credentials.json" ]; then
    echo ""
    echo "⚠️  credentials.json not found."
    echo "   → Download your Google Cloud service account JSON key and save it as credentials.json"
fi

# 4. data/raw reminder
echo ""
echo "📂 Make sure your FAO CSV files are in data/raw/:"
echo "   TRADE_VALUE.csv"
echo "   TRADE_QUANTITY.csv"
echo "   CL_FI_COMMODITY_ISSCFC.csv"
echo "   CL_FI_COUNTRY_GROUPS.csv"

echo ""
echo "✅ Setup complete. Next steps:"
echo "   1. Fill in .env and add credentials.json"
echo "   2. Run: python ingestion/load_fao_data.py"
echo "   3. Run: cd dbt_models && dbt run && dbt test"
echo "   4. Run: streamlit run app/main.py"
