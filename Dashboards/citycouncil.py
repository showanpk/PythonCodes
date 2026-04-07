import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import pyodbc
from urllib.parse import quote_plus
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================
# DATABASE CONNECTION (SQL Server)
# ============================================
def get_db_connection():
    server = os.getenv("DB_SERVER", "20.68.160.100,1433")
    database = os.getenv("DB_DATABASE", "SahelihubCRM")
    username = os.getenv("DB_USERNAME", "saheli_app")
    password = os.getenv("DB_PASSWORD", "309183")
    use_trusted = os.getenv("DB_TRUSTED_CONNECTION", "false").strip().lower() in {"1", "true", "yes", "y"}

    available_drivers = set(pyodbc.drivers())
    preferred_drivers = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    driver = next((d for d in preferred_drivers if d in available_drivers), None)
    if not driver:
        raise RuntimeError(
            "No SQL Server ODBC driver found. Install 'ODBC Driver 18 for SQL Server' or 'ODBC Driver 17 for SQL Server'."
        )

    auth_part = "Trusted_Connection=yes;" if use_trusted else f"UID={username};PWD={password};"
    odbc_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "TrustServerCertificate=yes;"
        f"{auth_part}"
    )

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}",
        pool_pre_ping=True,
    )
    return engine

# ============================================
# 1. FETCH DATA FROM DATABASE
# ============================================
def fetch_data():
    engine = get_db_connection()
    
    # Main assessments data
    query_assessments = """
    SELECT 
        SaheliCardNumber,
        AssessmentDate,
        WeightKg,
        Bmivalue,
        SystolicBp,
        DiastolicBp,
        PhysicalActivityMinutes,
        TotalSleepHours,
        StressLevel,
        FeelingOptimistic,
        FeelingRelaxed,
        FeelingConfident,
        Bmicategory
    FROM [SahelihubCRM].[dbo].[vw_Participants_Assessments]
    """
    
    # Participant details
    query_participants = """
    SELECT 
        ParticipantID,
        SaheliCardNumber,
        Ethnicity,
        Age,
        Gender
    FROM vw_Participants_Details
    """
    
    df_assessments = pd.read_sql(query_assessments, engine)
    df_participants = pd.read_sql(query_participants, engine)
    
    # Convert dates
    df_assessments['AssessmentDate'] = pd.to_datetime(df_assessments['AssessmentDate'])
    
    return df_assessments, df_participants

# ============================================
# 2. KPI CARDS (Summary Statistics)
# ============================================
def plot_kpi_cards(df):
    fig, axes = plt.subplots(2, 2, figsize=(12, 6))
    fig.suptitle('Health Metrics Overview', fontsize=16, fontweight='bold')
    
    # KPI 1: Total Participants
    total_participants = df['SaheliCardNumber'].nunique()
    axes[0,0].text(0.5, 0.5, f'Total\nParticipants\n{total_participants}', 
                   ha='center', va='center', fontsize=20, fontweight='bold')
    axes[0,0].axis('off')
    axes[0,0].set_title('Total Participants', fontsize=12)
    
    # KPI 2: Average BMI
    avg_bmi = df['Bmivalue'].mean()
    axes[0,1].text(0.5, 0.5, f'Average BMI\n{avg_bmi:.1f}', 
                   ha='center', va='center', fontsize=20, fontweight='bold')
    axes[0,1].axis('off')
    axes[0,1].set_title('Average BMI', fontsize=12)
    
    # KPI 3: Average Systolic
    avg_systolic = df['SystolicBp'].mean()
    axes[1,0].text(0.5, 0.5, f'Avg Systolic\n{avg_systolic:.0f} mmHg', 
                   ha='center', va='center', fontsize=20, fontweight='bold')
    axes[1,0].axis('off')
    axes[1,0].set_title('Average Systolic BP', fontsize=12)
    
    # KPI 4: Average Diastolic
    avg_diastolic = df['DiastolicBp'].mean()
    axes[1,1].text(0.5, 0.5, f'Avg Diastolic\n{avg_diastolic:.0f} mmHg', 
                   ha='center', va='center', fontsize=20, fontweight='bold')
    axes[1,1].axis('off')
    axes[1,1].set_title('Average Diastolic BP', fontsize=12)
    
    plt.tight_layout()
    plt.show()

# ============================================
# 3. BLOOD PRESSURE TREND (Line Chart)
# ============================================
def plot_blood_pressure_trend(df):
    # Aggregate by date
    bp_trend = df.groupby('AssessmentDate').agg({
        'SystolicBp': 'mean',
        'DiastolicBp': 'mean'
    }).reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=bp_trend['AssessmentDate'], y=bp_trend['SystolicBp'],
                             mode='lines+markers', name='Systolic',
                             line=dict(color='red', width=2)))
    fig.add_trace(go.Scatter(x=bp_trend['AssessmentDate'], y=bp_trend['DiastolicBp'],
                             mode='lines+markers', name='Diastolic',
                             line=dict(color='blue', width=2)))
    
    fig.update_layout(title='Blood Pressure Trend Over Time',
                      xaxis_title='Date',
                      yaxis_title='Blood Pressure (mmHg)',
                      hovermode='x unified')
    fig.show()

# ============================================
# 4. WEIGHT TREND (With Gain/Loss)
# ============================================
def plot_weight_trend(df):
    # Calculate weight change per participant
    weight_changes = df.groupby('SaheliCardNumber')['WeightKg'].agg(['first', 'last'])
    weight_changes['change'] = weight_changes['last'] - weight_changes['first']
    
    # Categorize changes
    gain_loss = pd.DataFrame({
        'Category': ['Weight Loss', 'No Change', 'Weight Gain'],
        'Count': [
            (weight_changes['change'] < 0).sum(),
            (weight_changes['change'] == 0).sum(),
            (weight_changes['change'] > 0).sum()
        ],
        'Color': ['green', 'gray', 'red']
    })
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Pie chart for gain/loss distribution
    axes[0].pie(gain_loss['Count'], labels=gain_loss['Category'], 
                colors=gain_loss['Color'], autopct='%1.1f%%', startangle=90)
    axes[0].set_title('Weight Change Distribution')
    
    # Line chart for overall weight trend
    weight_trend = df.groupby('AssessmentDate')['WeightKg'].mean().reset_index()
    axes[1].plot(weight_trend['AssessmentDate'], weight_trend['WeightKg'], 
                 marker='o', color='purple', linewidth=2)
    axes[1].set_title('Average Weight Trend Over Time')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('Average Weight (kg)')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# ============================================
# 5. BMI DISTRIBUTION (Bar Chart)
# ============================================
def plot_bmi_distribution(df):
    bmi_counts = df['Bmicategory'].value_counts()
    
    # Define order
    category_order = ['Underweight', 'Normal', 'Overweight', 'Obese']
    bmi_counts = bmi_counts.reindex(category_order)
    
    colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Bar chart
    bars = axes[0].bar(bmi_counts.index, bmi_counts.values, color=colors)
    axes[0].set_title('BMI Category Distribution')
    axes[0].set_xlabel('BMI Category')
    axes[0].set_ylabel('Number of Participants')
    
    # Add value labels on bars
    for bar, value in zip(bars, bmi_counts.values):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                     str(value), ha='center', va='bottom')
    
    # Pie chart
    axes[1].pie(bmi_counts.values, labels=bmi_counts.index, colors=colors,
                autopct='%1.1f%%', startangle=90)
    axes[1].set_title('BMI Distribution (%)')
    
    plt.tight_layout()
    plt.show()

# ============================================
# 6. PHYSICAL ACTIVITY (Donut Chart)
# ============================================
def plot_physical_activity(df):
    # Define active as >=30 minutes
    df['is_active'] = df['PhysicalActivityMinutes'] >= 30
    activity_summary = df.groupby('is_active').size()
    
    labels = ['Active Days (≥30 min)', 'Inactive Days (<30 min)']
    values = [activity_summary.get(True, 0), activity_summary.get(False, 0)]
    colors = ['#2ecc71', '#e74c3c']
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Donut chart
    wedges, texts, autotexts = axes[0].pie(values, labels=labels, colors=colors,
                                            autopct='%1.1f%%', startangle=90,
                                            wedgeprops=dict(width=0.3))
    axes[0].set_title('Physical Activity Distribution')
    
    # Average activity over time
    activity_trend = df.groupby('AssessmentDate')['PhysicalActivityMinutes'].mean().reset_index()
    axes[1].fill_between(activity_trend['AssessmentDate'], activity_trend['PhysicalActivityMinutes'],
                         alpha=0.3, color='blue')
    axes[1].plot(activity_trend['AssessmentDate'], activity_trend['PhysicalActivityMinutes'],
                 marker='o', color='darkblue', linewidth=2)
    axes[1].set_title('Average Physical Activity Trend')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('Minutes per Day')
    axes[1].axhline(y=30, color='green', linestyle='--', label='Recommended (30 min)')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# ============================================
# 7. SLEEP DURATION (Bar Chart with Gauge)
# ============================================
def plot_sleep_duration(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Gauge chart for average sleep
    avg_sleep = df['TotalSleepHours'].mean()
    
    # Custom gauge
    gauge_colors = ['#e74c3c', '#f39c12', '#2ecc71']
    gauge_values = [0, 7, 9]  # Less than 7, 7-9, more than 9
    
    axes[0].barh([0], [avg_sleep], color='#3498db', height=0.5)
    axes[0].set_xlim(0, 12)
    axes[0].set_ylim(-1, 1)
    axes[0].axvline(x=7, color='orange', linestyle='--', linewidth=2, label='Minimum (7 hrs)')
    axes[0].axvline(x=9, color='red', linestyle='--', linewidth=2, label='Maximum (9 hrs)')
    axes[0].set_title(f'Average Sleep Duration: {avg_sleep:.1f} hours')
    axes[0].legend()
    axes[0].set_yticks([])
    
    # Sleep trend over time
    sleep_trend = df.groupby('AssessmentDate')['TotalSleepHours'].mean().reset_index()
    axes[1].plot(sleep_trend['AssessmentDate'], sleep_trend['TotalSleepHours'],
                 marker='o', color='purple', linewidth=2)
    axes[1].fill_between(sleep_trend['AssessmentDate'], 7, 9, alpha=0.2, color='green',
                         label='Recommended Range (7-9 hrs)')
    axes[1].set_title('Sleep Duration Trend')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('Hours')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# ============================================
# 8. STRESS LEVELS (Area Chart)
# ============================================
def plot_stress_levels(df):
    stress_trend = df.groupby('AssessmentDate')['StressLevel'].agg(['mean', 'min', 'max']).reset_index()
    
    fig = go.Figure()
    
    # Add area for stress range
    fig.add_trace(go.Scatter(x=stress_trend['AssessmentDate'], y=stress_trend['max'],
                             fill=None, mode='lines', line_color='rgba(231, 76, 60, 0.3)',
                             name='Max Stress'))
    fig.add_trace(go.Scatter(x=stress_trend['AssessmentDate'], y=stress_trend['min'],
                             fill='tonexty', mode='lines', line_color='rgba(231, 76, 60, 0.3)',
                             name='Min Stress'))
    
    # Add average line
    fig.add_trace(go.Scatter(x=stress_trend['AssessmentDate'], y=stress_trend['mean'],
                             mode='lines+markers', name='Average Stress',
                             line=dict(color='darkred', width=3)))
    
    fig.update_layout(title='Stress Levels Over Time',
                      xaxis_title='Date',
                      yaxis_title='Stress Level (1-10)',
                      hovermode='x unified')
    fig.show()

# ============================================
# 9. MENTAL HEALTH METRICS (Radar Chart)
# ============================================
def plot_mental_health(df):
    metrics = {
        'Optimism': df['FeelingOptimistic'].mean(),
        'Relaxed': df['FeelingRelaxed'].mean(),
        'Confidence': df['FeelingConfident'].mean()
    }
    
    categories = list(metrics.keys())
    values = list(metrics.values())
    
    # Radar chart
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values + [values[0]],  # Close the loop
        theta=categories + [categories[0]],
        fill='toself',
        name='Average Scores',
        line_color='blue'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10]
            )),
        showlegend=True,
        title='Mental Health Metrics (1-10 scale)'
    )
    
    fig.show()
    
    # Also show as bar chart
    plt.figure(figsize=(10, 6))
    bars = plt.bar(categories, values, color=['#3498db', '#2ecc71', '#e74c3c'])
    plt.ylim(0, 10)
    plt.title('Mental Health Scores')
    plt.ylabel('Score (1-10)')
    
    # Add value labels
    for bar, value in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                 f'{value:.1f}', ha='center', va='bottom')
    
    plt.show()

# ============================================
# 10. BMI IMPROVEMENT TRACKER
# ============================================
def plot_bmi_improvement(df):
    # Get first and last assessment per participant
    first_bmi = df.groupby('SaheliCardNumber')['Bmivalue'].first()
    last_bmi = df.groupby('SaheliCardNumber')['Bmivalue'].last()
    
    improvement = first_bmi - last_bmi
    
    # Categorize improvement
    improvement_categories = pd.cut(improvement, 
                                     bins=[-float('inf'), -1, -0.1, 0.1, 1, float('inf')],
                                     labels=['Severe Worsening', 'Mild Worsening', 
                                             'Stable', 'Mild Improvement', 'Significant Improvement'])
    
    improvement_counts = improvement_categories.value_counts()
    
    # Heatmap style visualization
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Distribution of improvements
    colors = ['#e74c3c', '#f39c12', '#95a5a6', '#2ecc71', '#27ae60']
    bars = axes[0].bar(improvement_counts.index, improvement_counts.values, color=colors)
    axes[0].set_title('BMI Improvement Distribution')
    axes[0].set_xlabel('Change Category')
    axes[0].set_ylabel('Number of Participants')
    axes[0].tick_params(axis='x', rotation=45)
    
    # Add value labels
    for bar, value in zip(bars, improvement_counts.values):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                     str(value), ha='center', va='bottom')
    
    # Before vs After scatter plot
    axes[1].scatter(first_bmi, last_bmi, alpha=0.5, c='blue')
    axes[1].plot([min(first_bmi), max(first_bmi)], [min(first_bmi), max(first_bmi)], 
                 'r--', label='No Change')
    axes[1].set_xlabel('Starting BMI')
    axes[1].set_ylabel('Current BMI')
    axes[1].set_title('BMI Improvement: Before vs After')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    # Print summary statistics
    print("\n=== BMI Improvement Summary ===")
    print(f"Average Improvement: {improvement.mean():.2f} BMI points")
    print(f"Improved: {(improvement > 0).sum()} participants")
    print(f"Worsened: {(improvement < 0).sum()} participants")
    print(f"Stayed Same: {(improvement == 0).sum()} participants")

# ============================================
# 11. ETHNICITY BREAKDOWN (With BMI overlay)
# ============================================
def plot_ethnicity_breakdown(df_assessments, df_participants):
    # Merge datasets
    df_merged = df_assessments.merge(df_participants, on='SaheliCardNumber', how='left')
    
    # Group by ethnicity
    ethnicity_summary = df_merged.groupby('Ethnicity').agg({
        'SaheliCardNumber': 'nunique',
        'Bmivalue': 'mean'
    }).reset_index()
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Bar chart for participant count by ethnicity
    bars = axes[0].bar(ethnicity_summary['Ethnicity'], ethnicity_summary['SaheliCardNumber'],
                       color='skyblue')
    axes[0].set_title('Participants by Ethnicity')
    axes[0].set_xlabel('Ethnicity')
    axes[0].set_ylabel('Number of Participants')
    axes[0].tick_params(axis='x', rotation=45)
    
    # Add value labels
    for bar, value in zip(bars, ethnicity_summary['SaheliCardNumber']):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                     str(value), ha='center', va='bottom')
    
    # BMI by ethnicity
    axes[1].bar(ethnicity_summary['Ethnicity'], ethnicity_summary['Bmivalue'],
                color='lightcoral')
    axes[1].axhline(y=25, color='red', linestyle='--', label='Overweight Threshold (25)')
    axes[1].axhline(y=30, color='darkred', linestyle='--', label='Obese Threshold (30)')
    axes[1].set_title('Average BMI by Ethnicity')
    axes[1].set_xlabel('Ethnicity')
    axes[1].set_ylabel('Average BMI')
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].legend()
    
    plt.tight_layout()
    plt.show()

# ============================================
# 12. COMPLETE DASHBOARD (Subplots)
# ============================================
def create_complete_dashboard(df, df_participants):
    """Create a comprehensive dashboard with multiple charts"""
    
    fig = make_subplots(
        rows=4, cols=3,
        subplot_titles=('Blood Pressure Trend', 'BMI Distribution', 'Physical Activity',
                       'Weight Trend', 'Sleep Duration', 'Stress Levels',
                       'Mental Health', 'BMI Improvement', 'Ethnicity Breakdown',
                       'Activity Minutes', 'BMI Over Time', 'Health Summary'),
        specs=[[{'type': 'scatter'}, {'type': 'bar'}, {'type': 'pie'}],
               [{'type': 'scatter'}, {'type': 'bar'}, {'type': 'scatter'}],
               [{'type': 'bar'}, {'type': 'scatter'}, {'type': 'bar'}],
               [{'type': 'scatter'}, {'type': 'scatter'}, {'type': 'indicator'}]]
    )
    
    # Row 1, Col 1: Blood Pressure Trend
    bp_trend = df.groupby('AssessmentDate')[['SystolicBp', 'DiastolicBp']].mean().reset_index()
    fig.add_trace(go.Scatter(x=bp_trend['AssessmentDate'], y=bp_trend['SystolicBp'],
                            name='Systolic', line=dict(color='red')), row=1, col=1)
    fig.add_trace(go.Scatter(x=bp_trend['AssessmentDate'], y=bp_trend['DiastolicBp'],
                            name='Diastolic', line=dict(color='blue')), row=1, col=1)
    
    # Row 1, Col 2: BMI Distribution
    bmi_counts = df['Bmicategory'].value_counts().reindex(['Underweight', 'Normal', 'Overweight', 'Obese'])
    fig.add_trace(go.Bar(x=bmi_counts.index, y=bmi_counts.values, name='BMI Categories',
                        marker_color=['#3498db', '#2ecc71', '#f39c12', '#e74c3c']), row=1, col=2)
    
    # Row 1, Col 3: Physical Activity (Donut)
    is_active = df['PhysicalActivityMinutes'] >= 30
    active_counts = is_active.value_counts()
    fig.add_trace(go.Pie(labels=['Active', 'Inactive'], values=active_counts.values,
                        hole=0.4, marker_colors=['#2ecc71', '#e74c3c']), row=1, col=3)
    
    # Update layout
    fig.update_layout(height=1200, showlegend=True, title_text="Health & Wellness Dashboard")
    fig.show()

# ============================================
# MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    # Fetch data
    print("Loading data from database...")
    df_assessments, df_participants = fetch_data()
    
    print(f"Loaded {len(df_assessments)} assessments for {df_assessments['SaheliCardNumber'].nunique()} participants")
    
    # Generate all visualizations
    print("\n1. Generating KPI Cards...")
    plot_kpi_cards(df_assessments)
    
    print("\n2. Generating Blood Pressure Trend...")
    plot_blood_pressure_trend(df_assessments)
    
    print("\n3. Generating Weight Trend...")
    plot_weight_trend(df_assessments)
    
    print("\n4. Generating BMI Distribution...")
    plot_bmi_distribution(df_assessments)
    
    print("\n5. Generating Physical Activity...")
    plot_physical_activity(df_assessments)
    
    print("\n6. Generating Sleep Duration...")
    plot_sleep_duration(df_assessments)
    
    print("\n7. Generating Stress Levels...")
    plot_stress_levels(df_assessments)
    
    print("\n8. Generating Mental Health Metrics...")
    plot_mental_health(df_assessments)
    
    print("\n9. Generating BMI Improvement Tracker...")
    plot_bmi_improvement(df_assessments)
    
    print("\n10. Generating Ethnicity Breakdown...")
    plot_ethnicity_breakdown(df_assessments, df_participants)
    
    print("\n11. Creating Complete Dashboard...")
    create_complete_dashboard(df_assessments, df_participants)
    
    print("\n✅ All visualizations generated successfully!")