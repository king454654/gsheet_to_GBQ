import mysql.connector
import g4f

# Function to establish MySQL connection
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="password",
            database="campaignperformance"
        )
        print("✅ Successfully connected to the database!")
        return connection
    except mysql.connector.Error as err:
        print(f"❌ Error connecting to database: {err}")
        return None

# Fetch all table names dynamically
def fetch_all_tables():
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    db_connection.close()
    return tables

# Fetch column names for each table
def fetch_table_schema():
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    
    schema_info = {}
    tables = fetch_all_tables()

    for table in tables:
        cursor.execute(f"SHOW COLUMNS FROM `{table}`")
        columns = [col[0] for col in cursor.fetchall()]
        schema_info[table] = columns

    cursor.close()
    db_connection.close()
    return schema_info

# Generate campaign insights dynamically
def generate_campaign_insights():
    insights = []
    db_connection = get_db_connection()
    cursor = db_connection.cursor()

    schema = fetch_table_schema()

    for table, columns in schema.items():
        column_map = {
            "campaign": next((col for col in columns if "campaign" in col.lower()), None),
            "revenue": next((col for col in columns if "revenue" in col.lower()), None),
            "clicks": next((col for col in columns if "click" in col.lower()), None),
            "conversions": next((col for col in columns if "conversion" in col.lower()), None)
        }

        if all(column_map.values()):
            campaign_col, revenue_col, clicks_col, conversions_col = column_map.values()

            cursor.execute(f"""
                SELECT `{campaign_col}`, `{revenue_col}`, `{clicks_col}`, `{conversions_col}` 
                FROM `{table}` 
                ORDER BY `{revenue_col}` DESC LIMIT 3
            """)
            top_campaigns = cursor.fetchall()

            insights.append(f"🏆 **Top 3 High Revenue Campaigns from `{table}`:**")
            for campaign in top_campaigns:
                name, revenue, clicks, conversions = campaign
                insights.append(
                    f"- **{name}** generated **${revenue}**, with **{clicks} clicks** and **{conversions} conversions**"
                )

            cursor.execute(f"SELECT SUM(`{revenue_col}`) FROM `{table}`")
            total_revenue = cursor.fetchone()[0]
            insights.append(f"\n💰 **Total revenue from `{table}`:** **${total_revenue}**")

            cursor.execute(f"""
                SELECT `{campaign_col}`, `{revenue_col}`, `{conversions_col}`, (`{revenue_col}` / `{conversions_col}`) AS efficiency
                FROM `{table}`
                WHERE `{conversions_col}` > 0 
                ORDER BY efficiency DESC LIMIT 1
            """)
            efficient_campaign = cursor.fetchone()

            if efficient_campaign:
                name, revenue, conversions, efficiency = efficient_campaign
                insights.append(
                    f"\n🚀 **Most cost-effective campaign:** **{name}**, generating **${revenue}** with an efficiency of **${efficiency:.2f} per conversion**."
                )

    cursor.close()
    db_connection.close()
    return "\n".join(insights)

# Generate AI-enhanced insights using g4f (Free)
def refine_insights_with_g4f(raw_insights):
    prompt = [
        {"role": "system", "content": "Reframe these campaign insights to be more engaging, actionable, and well-worded."},
        {"role": "user", "content": raw_insights}
    ]

    response = g4f.ChatCompletion.create(
        model=g4f.models.gpt_4,  # Using GPT-4 model provided by g4f
        messages=prompt,
    )

    return response

# Run insights generation properly without asyncio
def main():
    raw_insights = generate_campaign_insights()
    enhanced_insights = refine_insights_with_g4f(raw_insights)  # No async needed
    print(enhanced_insights)

if __name__ == "__main__":
    main()
