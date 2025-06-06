import mysql.connector
import g4f
from g4f.client import Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
    if not db_connection:
        return []
    cursor = db_connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    db_connection.close()
    return tables

# Fetch column names for each table
def fetch_table_schema():
    db_connection = get_db_connection()
    if not db_connection:
        return {}
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

# Generate campaign insights
def generate_campaign_insights():
    insights = []
    db_connection = get_db_connection()
    if not db_connection:
        insights.append("❌ Could not connect to the database.")
        return "\n".join(insights)

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

            insights.append(f"🏆 Top 3 High Revenue Campaigns from `{table}`:")
            for campaign in top_campaigns:
                name, revenue, clicks, conversions = campaign
                insights.append(
                    f"- {name}: ${revenue}, {clicks} clicks, {conversions} conversions"
                )

            cursor.execute(f"SELECT SUM(`{revenue_col}`) FROM `{table}`")
            total_revenue = cursor.fetchone()[0]
            insights.append(f"💰 Total revenue from `{table}`: ${total_revenue}")

            cursor.execute(f"""
                SELECT `{campaign_col}`, `{revenue_col}`, `{conversions_col}`, 
                       (`{revenue_col}` / `{conversions_col}`) AS efficiency
                FROM `{table}`
                WHERE `{conversions_col}` > 0 
                ORDER BY efficiency DESC LIMIT 1
            """)
            efficient_campaign = cursor.fetchone()

            if efficient_campaign:
                name, revenue, conversions, efficiency = efficient_campaign
                insights.append(
                    f"🚀 Most cost-effective campaign: {name} – ${efficiency:.2f} per conversion."
                )

    cursor.close()
    db_connection.close()
    return "\n".join(insights)

# Use g4f to reframe insights as a story passage
def refine_insights_with_g4f(raw_insights):
    prompt = [
        {"role": "system", "content": "Rewrite the campaign insights into a concise, engaging story passage of 1 or 2 paragraphs, highlighting key points and actionable insights."},
        {"role": "user", "content": raw_insights}
    ]

    client = Client()
    try:
        # Try GPT-4
        response = client.chat.completions.create(
            model="gpt-4",
            messages=prompt,
            stream=False
        )
        return response.choices[0].message.content
    except:
        try:
            # Fallback to GPT-3.5
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=prompt,
                stream=False
            )
            return "(Fallback to GPT-3.5)\n" + response.choices[0].message.content
        except Exception as e:
            return f"❌ g4f failed completely: {e}"

# Generate HTML email content with basic styling
def generate_html_email_content(text):
    html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f9f9f9;
                color: #333333;
                padding: 20px;
            }}
            .container {{
                max-width: 600px;
                background-color: #ffffff;
                padding: 20px;
                margin: auto;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }}
            h2 {{
                color: #4CAF50;
            }}
            p {{
                line-height: 1.5;
                font-size: 16px;
            }}
            .footer {{
                font-size: 12px;
                color: #777777;
                margin-top: 30px;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>📊 Campaign Performance Insights</h2>
            <p>{text}</p>
            <div class="footer">
                <p>Generated automatically. Please do not reply to this email.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

# Send email to multiple recipients with HTML content
def send_email(subject, html_body, sender_email, sender_password, recipient_emails, smtp_server="smtp.gmail.com", smtp_port=587):
    message = MIMEMultipart("alternative")
    message["From"] = sender_email
    message["To"] = ", ".join(recipient_emails)
    message["Subject"] = subject

    # Attach HTML part
    message.attach(MIMEText(html_body, "html"))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_emails, message.as_string())
        server.quit()
        print(f"📧 Email sent successfully to {len(recipient_emails)} recipients!")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# Main function
def main():
    raw_insights = generate_campaign_insights()
    print("🔍 Raw Insights:\n", raw_insights)

    enhanced_insights = refine_insights_with_g4f(raw_insights)
    print("\n✨ Enhanced Insights:\n", enhanced_insights)

    # Replace new lines with <br> for HTML, or let the email style handle paragraph spacing
    html_body = generate_html_email_content(enhanced_insights.replace('\n', '<br>'))

    # Email credentials - Replace with your actual details
    sender_email = "indrajit.dey@iopex.com"
    # "indrajit054@gmail.com"
    # "indrajitdey054@gmail.com"
    sender_password = "fxha wymq vmqe gpih"
    # "mymw nrly tcae deum"  # Use app password if 2FA enabled

    recipient_emails = ["tenneti.rahul@iopex.com","chelluri.malleshwarrao@iopex.com"
        # "indrajitdey00000@gmail.com",

        # Add more emails here separated by commas
    ]

    subject = "Campaign Performance Insights"

    send_email(subject, html_body, sender_email, sender_password, recipient_emails)

if __name__ == "__main__":
    main()
