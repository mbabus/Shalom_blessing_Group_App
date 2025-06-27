import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text, Column, Integer, String, Float, Boolean, Date
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, backref
from sqlalchemy import ForeignKey
import os
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import io
import json
import calendar

# --- Page Configuration ---
st.set_page_config(
    page_title="Shalom Blessing SHG",
    page_icon="🤝", # Changed icon to handshake emoji
    layout="wide"
)

# --- Enhanced Custom CSS ---
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Main app styling */
    .stApp {
        /* Reverted to the previous brighter gradient as requested */
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%); 
        font-family: 'Inter', sans-serif;
    }
    
    /* Main content area */
    .main .block-container {
        background-color: rgba(255, 255, 255, 0.95);
        border-radius: 20px;
        padding: 2rem;
        margin-top: 1rem;
        box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        backdrop-filter: blur(10px);
    }

    /* Sidebar styling */
    .st-emotion-cache-vk338m {
        background: linear-gradient(180deg, #1e3c72 0%, #2a5298 100%);
        border-radius: 0 20px 20px 0;
    }
    .st-emotion-cache-16txtl3 {
        padding: 2rem 1rem;
    }
    .st-emotion-cache-vk338m h1 {
        color: #ffffff;
        text-align: center;
        font-weight: 700;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }

    /* Navigation styling */
    .stRadio > div {
        gap: 0.5rem;
    }
    .stRadio > div > label {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 0.75rem 1rem;
        margin: 0.25rem 0;
        color: #ffffff;
        border: 1px solid rgba(255, 255, 255, 0.2);
        transition: all 0.3s ease;
        cursor: pointer;
    }
    .stRadio > div > label:hover {
        background: rgba(255, 255, 255, 0.2);
        transform: translateX(5px);
    }
    .stRadio > div > label[data-checked="true"] {
        background: rgba(255, 255, 255, 0.3);
        border-color: rgba(255, 255, 255, 0.5);
        transform: translateX(8px);
    }

    /* Enhanced Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
        border-radius: 16px;
        padding: 2rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        border: 1px solid rgba(255, 255, 255, 0.2);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    div[data-testid="stMetric"]:before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.15);
    }
    div[data-testid="stMetric"] label {
        color: #475569;
        font-size: 0.9rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #1e293b;
        font-size: 2rem;
        font-weight: 700;
    }

    /* Enhanced container styling */
    .stContainer {
        background: linear-gradient(135deg, #ffffff 0%, #f1f5f9 100%);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border: 1px solid rgba(226, 232, 240, 0.8);
    }

    /* Enhanced Expander styling */
    .streamlit-expanderHeader {
        font-size: 1.1rem;
        font-weight: 600;
        background: linear-gradient(135deg, #e2e8f0 0%, #cbd5e1 100%);
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid rgba(148, 163, 184, 0.3);
    }
    .streamlit-expanderContent {
        background: linear-gradient(135deg, #f8fafc 0%, #ffffff 100%);
        border-radius: 0 0 12px 12px;
        padding: 1.5rem;
    }

    /* Enhanced Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(241, 245, 249, 0.5);
        border-radius: 12px;
        padding: 0.5rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background: transparent;
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white !important;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }

    /* Enhanced Button styling */
    .stButton button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 12px;
        border: none;
        padding: 0.75rem 1.5rem;
        font-size: 0.95rem;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
    }
    .stButton button[kind="secondary"] {
        background: linear-gradient(135deg, #64748b 0%, #475569 100%);
        box-shadow: 0 4px 12px rgba(100, 116, 139, 0.3);
    }
    .stButton button[kind="secondary"]:hover {
        box-shadow: 0 6px 20px rgba(100, 116, 139, 0.4);
    }

    /* Alert styling */
    .stAlert {
        border-radius: 12px;
        border: none;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }

    /* Success alert */
    .stAlert[data-baseweb="notification"][kind="success"] {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
    }

    /* Error alert */
    .stAlert[data-baseweb="notification"][kind="error"] {
        background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
        color: white;
    }

    /* Info alert */
    .stAlert[data-baseweb="notification"][kind="info"] {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
    }

    /* Member card styling */
    .member-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border-left: 4px solid #667eea;
        transition: all 0.3s ease;
    }
    .member-card:hover {
        transform: translateY(-2px);
        box_shadow: 0 8px 30px rgba(0,0,0,0.12);
    }

    /* Title styling */
    h1, h2, h3 {
        color: #1e293b;
        font-weight: 700;
    }

    /* Form styling */
    .stTextInput input, .stTextArea textarea, .stSelectbox select {
        border-radius: 8px;
        border: 1px solid #d1d5db;
        transition: all 0.3s ease;
    }
    .stTextInput input:focus, .stTextArea textarea:focus, .stSelectbox select:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
</style>
""", unsafe_allow_html=True)

# --- Database Configuration (SQLite) ---
DATABASE_URL = "sqlite:///shalom_blessing_v2.db"

@st.cache_resource
def get_database_engine():
    """Initializes and returns the SQLAlchemy engine."""
    return create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

engine = get_database_engine()
Base = declarative_base()

# --- Database Models ---
class Member(Base):
    __tablename__ = 'members'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    phone = Column(String(20))
    status = Column(String(20), default='active')
    join_date = Column(Date, default=date.today)
    
class Meeting(Base):
    __tablename__ = 'meetings'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    notes = Column(String(500))
    financial_year = Column(String(10))

class Attendance(Base):
    __tablename__ = 'attendance'
    id = Column(Integer, primary_key=True)
    meeting_id = Column(Integer, ForeignKey('meetings.id', ondelete="CASCADE"))
    member_id = Column(Integer, ForeignKey('members.id', ondelete="CASCADE"))
    present = Column(Boolean, default=False)

class Contribution(Base):
    __tablename__ = 'contributions'
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id', ondelete="CASCADE"))
    meeting_id = Column(Integer, ForeignKey('meetings.id', ondelete="CASCADE"), nullable=True)
    votehead = Column(String(20)) # e.g., 'shares', 'welfare'
    amount = Column(Float)
    date = Column(Date, default=date.today)

class Loan(Base):
    __tablename__ = 'loans'
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id', ondelete="CASCADE"))
    type = Column(String(20)) # e.g., 'development', 'emergency'
    amount = Column(Float)
    interest_rate = Column(Float)
    start_date = Column(Date)
    due_date = Column(Date)
    status = Column(String(20), default='active') # 'active', 'completed', 'defaulted'

class Repayment(Base):
    __tablename__ = 'repayments'
    id = Column(Integer, primary_key=True)
    loan_id = Column(Integer, ForeignKey('loans.id', ondelete="CASCADE"))
    amount = Column(Float)
    date = Column(Date, default=date.today)

class Penalty(Base):
    __tablename__ = 'penalties'
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id', ondelete="CASCADE"))
    amount = Column(Float)
    reason = Column(String(200))
    date = Column(Date, default=date.today)

class Expense(Base):
    __tablename__ = 'expenses'
    id = Column(Integer, primary_key=True)
    category = Column(String(50))
    description = Column(String(200))
    amount = Column(Float)
    date = Column(Date, default=date.today)

class Dividend(Base):
    __tablename__ = 'dividends'
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id', ondelete="CASCADE"))
    amount = Column(Float)
    cycle_year = Column(String(10))
    shares = Column(Integer)
    rate_per_share = Column(Float)
    
class Setting(Base):
    __tablename__ = 'settings'
    key = Column(String(50), primary_key=True)
    value = Column(String(200))

# SMS Reminder class (for future SMS integration)
class SMSReminder(Base):
    __tablename__ = 'sms_reminders'
    id = Column(Integer, primary_key=True)
    meeting_id = Column(Integer, ForeignKey('meetings.id', ondelete="CASCADE"))
    sent_date = Column(Date)
    status = Column(String(20), default='pending')  # pending, sent, failed

Base.metadata.create_all(engine) # Create tables if they don't exist
Session = sessionmaker(bind=engine)

# --- Helper & Utility Functions ---
SHARE_VALUE = 1000 # KSh 1000 per share

def get_setting(key, default=None):
    """Retrieves a setting from the database."""
    session = Session()
    setting = session.query(Setting).filter(Setting.key == key).first()
    session.close()
    return setting.value if setting else default

def save_setting(key, value):
    """Saves a setting to the database."""
    session = Session()
    setting = session.query(Setting).filter(Setting.key == key).first()
    if setting:
        setting.value = value
    else:
        setting = Setting(key=key, value=value)
        session.add(setting)
    session.commit()
    session.close()

def get_financial_year(date_obj=None):
    """Determines the financial year based on a given date."""
    if date_obj is None:
        date_obj = datetime.now().date()
    return f"{date_obj.year}-{date_obj.year + 1}" if date_obj.month >= 3 else f"{date_obj.year - 1}-{date_obj.year}"

def get_third_sunday_of_month(year, month):
    """Calculates the date of the third Sunday of a given month and year."""
    # Find the first day of the month
    first_day = date(year, month, 1)
    
    # Find the first Sunday
    days_after_monday = (6 - first_day.weekday()) % 7  # Sunday is 6
    first_sunday = first_day + timedelta(days=days_after_monday)
    
    # Add 14 days to get the third Sunday
    third_sunday = first_sunday + timedelta(days=14)
    
    # Make sure it's still in the same month
    if third_sunday.month != month:
        # If we went into the next month, use the second Sunday instead
        third_sunday = first_sunday + timedelta(days=7)
    
    return third_sunday

def get_next_meeting_date():
    """Calculates the date of the next third Sunday meeting."""
    today = datetime.now().date()
    current_year = today.year
    current_month = today.month
    
    # Get this month's third Sunday
    this_month_third_sunday = get_third_sunday_of_month(current_year, current_month)
    
    # If we haven't passed this month's third Sunday, return it
    if today <= this_month_third_sunday:
        return this_month_third_sunday
    
    # Otherwise, get next month's third Sunday
    if current_month == 12:
        next_month = 1
        next_year = current_year + 1
    else:
        next_month = current_month + 1
        next_year = current_year
    
    return get_third_sunday_of_month(next_year, next_month)

def get_meeting_reminder_date(meeting_date):
    """Calculates the date for sending meeting reminders (one week before)."""
    return meeting_date - timedelta(days=7)

def calculate_loan_balance(loan_id):
    """Calculates the current balance for a loan, including interest.
    
    Emergency loans: 2% simple interest monthly on the original amount.
    Development loans: Annual simple interest on the original amount.
    """
    session = Session()
    try:
        loan = session.query(Loan).get(loan_id)
        if not loan: return 0
        total_repaid = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM repayments WHERE loan_id = :lid"), {'lid': loan_id}).scalar()
        
        interest = 0
        if loan.type == 'emergency':
            # Calculate full months elapsed since loan start
            # If current month is before start month in the same year, or if year is before start year, months_elapsed is 0.
            if date.today() < loan.start_date:
                months_elapsed = 0
            else:
                months_elapsed = (date.today().year - loan.start_date.year) * 12 + (date.today().month - loan.start_date.month)
            
            # Ensure months elapsed is non-negative
            months_elapsed = max(0, months_elapsed)
            
            monthly_interest_rate = 0.02 # 2% as decimal
            interest = loan.amount * monthly_interest_rate * months_elapsed
        else: # For development loans (or any other type)
            # Annual simple interest
            days_elapsed = (date.today() - loan.start_date).days
            interest = loan.amount * (loan.interest_rate / 100) * (days_elapsed / 365)
           
        total_owed = loan.amount + interest
        
        return max(0, total_owed - total_repaid) # Recalculate based on total_owed, which includes interest
    finally:
        session.close()

def get_member_complete_details(member_id):
    """Retrieves comprehensive member details including all financial records."""
    session = Session()
    try:
        # Basic member info
        member = session.query(Member).get(member_id)
        if not member:
            return None
        
        # Contributions summary
        contributions = pd.read_sql("""
            SELECT c.date, c.votehead, c.amount, m.date as meeting_date
            FROM contributions c 
            LEFT JOIN meetings m ON c.meeting_id = m.id
            WHERE c.member_id = :mid
            ORDER BY c.date DESC
        """, engine, params={'mid': member_id})
        
        # Loans summary
        loans = pd.read_sql("""
            SELECT l.id, l.type, l.amount, l.interest_rate, l.start_date, l.due_date, l.status,
                   COALESCE(SUM(r.amount), 0) as total_repaid
            FROM loans l
            LEFT JOIN repayments r ON l.id = r.loan_id
            WHERE l.member_id = :mid
            GROUP BY l.id
            ORDER BY l.start_date DESC
        """, engine, params={'mid': member_id})
        
        # Calculate loan balances
        if not loans.empty:
            loans['balance'] = loans.apply(lambda row: calculate_loan_balance(row['id']), axis=1)
        
        # Penalties
        penalties = pd.read_sql("""
            SELECT date, amount, reason FROM penalties 
            WHERE member_id = :mid ORDER BY date DESC
        """, engine, params={'mid': member_id})
        
        # Dividends
        dividends = pd.read_sql("""
            SELECT cycle_year, shares, rate_per_share, amount FROM dividends 
            WHERE member_id = :mid ORDER BY cycle_year DESC
        """, engine, params={'mid': member_id})
        
        # Attendance summary
        attendance = pd.read_sql("""
            SELECT m.date, a.present FROM attendance a
            JOIN meetings m ON a.meeting_id = m.id
            WHERE a.member_id = :mid
            ORDER BY m.date DESC
        """, engine, params={'mid': member_id})
        
        # Calculate totals
        shares_total = contributions[contributions['votehead'] == 'shares']['amount'].sum() if not contributions.empty else 0
        welfare_total = contributions[contributions['votehead'] == 'welfare']['amount'].sum() if not contributions.empty else 0
        total_contributions = contributions['amount'].sum() if not contributions.empty else 0
        active_loans_balance = loans[loans['status'] == 'active']['balance'].sum() if not loans.empty else 0
        total_penalties = penalties['amount'].sum() if not penalties.empty else 0
        total_dividends = dividends['amount'].sum() if not dividends.empty else 0
        attendance_rate = (attendance['present'].sum() / len(attendance) * 100) if not attendance.empty else 0
        
        return {
            'member': member,
            'contributions': contributions,
            'loans': loans,
            'penalties': penalties,
            'dividends': dividends,
            'attendance': attendance,
            'totals': {
                'shares_total': shares_total,
                'welfare_total': welfare_total,
                'total_contributions': total_contributions,
                'active_loans_balance': active_loans_balance,
                'total_penalties': total_penalties,
                'total_dividends': total_dividends,
                'attendance_rate': attendance_rate
            }
        }
    finally:
        session.close()

def generate_pdf(story_elements, title="Report"):
    """Generates a PDF report from ReportLab elements."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
    doc.build(story_elements)
    buffer.seek(0)
    return buffer

# --- Enhanced UI Component Functions ---
def show_dashboard():
    """Displays the main dashboard with key financial metrics and recent activities."""
    st.title("📊 Dashboard Overview")
    st.markdown("Welcome to Shalom Blessing SHG - Your complete group management solution")
    
    session = Session()
    try:
        # --- Key Metrics ---
        st.subheader("📈 Financial Health Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            shares_total = session.execute(text('SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = :vh'), {'vh': 'shares'}).scalar()
            st.metric("Total Shares", f"KSh {shares_total:,.2f}", delta="↗️ Growing")
            
        with col2:
            welfare_total = session.execute(text('SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = :vh'), {'vh': 'welfare'}).scalar()
            st.metric("Total Welfare", f"KSh {welfare_total:,.2f}", delta="💚 Strong")
            
        with col3:
            active_loans_amount = session.execute(text('SELECT COALESCE(SUM(amount), 0) FROM loans WHERE status = :st'), {'st': 'active'}).scalar()
            st.metric("Active Loans", f"KSh {active_loans_amount:,.2f}", delta="🏦 Lending")
            
        with col4:
            dividends_paid = session.execute(text('SELECT COALESCE(SUM(amount), 0) FROM dividends')).scalar()
            st.metric("Dividends Paid", f"KSh {dividends_paid:,.2f}", delta="💎 Returns")

        st.markdown("---")

        # --- Quick Stats Row ---
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_members = session.execute(text("SELECT COUNT(*) FROM members WHERE status='active'")).scalar()
            st.info(f"👥 **{total_members}** Active Members")
        with col2:
            total_meetings = session.execute(text("SELECT COUNT(*) FROM meetings")).scalar()
            st.info(f"📅 **{total_meetings}** Total Meetings")
        with col3:
            pending_penalties = session.execute(text("SELECT COUNT(*) FROM penalties")).scalar()
            st.info(f"⚖️ **{pending_penalties}** Total Penalties")
        with col4:
            total_expenses = session.execute(text('SELECT COALESCE(SUM(amount), 0) FROM expenses')).scalar()
            st.info(f"💸 **KSh {total_expenses:,.2f}** Total Expenses")

        st.markdown("---")

        # --- Alerts & Notifications ---
        st.subheader("🔔 Important Alerts")
        
        # Overdue loans alert
        overdue_loans = pd.read_sql("""
            SELECT m.name, l.amount, l.due_date 
            FROM loans l 
            JOIN members m ON l.member_id = m.id 
            WHERE l.status = 'active' AND l.due_date < ?
        """, engine, params=(date.today(),)) 
        
        if not overdue_loans.empty:
            with st.container():
                st.error("🚨 **Overdue Loans Alert!**")
                for _, row in overdue_loans.iterrows():
                    days_overdue = (date.today() - row['due_date']).days
                    st.write(f"• **{row['name']}**: KSh {row['amount']:,.2f} - **{days_overdue} days overdue**")
        else:
            st.success("✅ **No overdue loans!** All members are up to date.")

        # Next meeting info
        next_meeting_date = get_next_meeting_date()
        reminder_date = get_meeting_reminder_date(next_meeting_date)
        days_until_meeting = (next_meeting_date - date.today()).days
        
        if days_until_meeting <= 7:
            st.warning(f"🗓️ **Upcoming Meeting:** {next_meeting_date.strftime('%A, %B %d, %Y')} - **{days_until_meeting} days away**")
            st.info(f"📱 **Reminder Date:** {reminder_date.strftime('%A, %B %d, %Y')} - Send SMS reminders!")
        else:
            st.info(f"🗓️ **Next Meeting:** {next_meeting_date.strftime('%A, %B %d, %Y')} (Third Sunday)")

        st.markdown("---")

        # --- Enhanced Visualizations ---
        st.subheader("📊 Financial Analytics")
        
        # Create two columns for charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 💰 Monthly Contributions Trend")
            contrib_data = pd.read_sql("""
                SELECT strftime('%Y-%m', date) as month, 
                       votehead, 
                       SUM(amount) as total 
                FROM contributions 
                GROUP BY month, votehead 
                ORDER BY month DESC
                LIMIT 24
            """, engine)
            
            if not contrib_data.empty:
                fig = px.line(contrib_data, x='month', y='total', color='votehead', 
                             title="Contributions Trend", markers=True,
                             color_discrete_sequence=['#667eea', '#764ba2', '#f093fb'])
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#1e293b')
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("📊 No contribution data available yet.")
        
        with col2:
            st.markdown("##### 📈 Financial Overview")
            # Create a summary chart
            financial_summary = pd.DataFrame({
                'Category': ['Shares', 'Welfare', 'Loans', 'Expenses'],
                'Amount': [shares_total, welfare_total, active_loans_amount, total_expenses],
                'Color': ['#667eea', '#764ba2', '#f093fb', '#ffeaa7']
            })
            
            if financial_summary['Amount'].sum() > 0:
                fig = px.pie(financial_summary, values='Amount', names='Category',
                           title="Financial Distribution",
                           color_discrete_sequence=['#667eea', '#764ba2', '#f093fb', '#ffeaa7'])
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#1e293b')
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("📊 No financial data available yet.")

        # Recent Activity
        st.subheader("🕒 Recent Activity")
        recent_activities = pd.read_sql("""
            SELECT 'Contribution' as type, m.name, c.amount, c.date, c.votehead as details
            FROM contributions c
            JOIN members m ON c.member_id = m.id
            WHERE c.date > date('now', '-30 days')
            UNION ALL
            SELECT 'Loan' as type, m.name, l.amount, l.start_date as date, l.type as details
            FROM loans l
            JOIN members m ON l.member_id = m.id
            WHERE l.start_date > date('now', '-30 days')
            ORDER BY date DESC
            LIMIT 10
        """, engine)
        
        if not recent_activities.empty:
            for _, activity in recent_activities.iterrows():
                col1, col2, col3, col4 = st.columns([1, 2, 2, 2])
                with col1:
                    if activity['type'] == 'Contribution':
                        st.write("💰")
                    else:
                        st.write("🏦")
                with col2:
                    st.write(f"**{activity['name']}**")
                with col3:
                    st.write(f"{activity['type']}: {activity['details']}")
                with col4:
                    st.write(f"KSh {activity['amount']:,.2f}")
        else:
            st.info("No recent activity in the last 30 days.")

    finally:
        session.close()

def show_members():
    """Displays and manages member information."""
    st.title("👥 Members Management")
    
    # Enhanced search with filters
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        search_query = st.text_input("🔍 Search members by name...", placeholder="Enter member name")
    with col2:
        status_filter = st.selectbox("Status", ["All", "Active", "Inactive"])
    with col3:
        sort_by = st.selectbox("Sort by", ["Name", "Join Date", "Total Contributions"])

    session = Session()
    try:
        # Build query based on filters
        query = "SELECT * FROM members WHERE 1=1"
        params = []
        
        if search_query:
            query += " AND name LIKE ?"
            params.append(f"%{search_query}%")
        
        if status_filter != "All":
            query += " AND status = ?"
            params.append(status_filter.lower())
        
        if sort_by == "Name":
            query += " ORDER BY name"
        elif sort_by == "Join Date":
            query += " ORDER BY join_date DESC"
        else:
            query += " ORDER BY name"  # Default for contributions sorting
        
        members_df = pd.read_sql(query, engine, params=params)
        
        if members_df.empty:
            st.info("No members found matching your criteria.")
            return
        
        # Add/Edit Member Form
        with st.expander("➕ Add New Member", expanded=False):
            with st.form("add_member_form"):
                col1, col2 = st.columns(2)
                with col1:
                    new_name = st.text_input("Full Name*")
                    new_phone = st.text_input("Phone Number")
                with col2:
                    new_join_date = st.date_input("Join Date", value=date.today())
                    new_status = st.selectbox("Status", ["active", "inactive"])
                
                if st.form_submit_button("Add Member", type="primary"):
                    if new_name:
                        try:
                            new_member = Member(
                                name=new_name.strip(),
                                phone=new_phone.strip() if new_phone else None,
                                join_date=new_join_date,
                                status=new_status
                            )
                            session.add(new_member)
                            session.commit()
                            st.success(f"✅ Member '{new_name}' added successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error adding member: {str(e)}")
                    else:
                        st.error("Please enter a member name.")
        
        # Members List with enhanced display
        st.subheader(f"📋 Members List ({len(members_df)} found)")
        
        for _, member in members_df.iterrows():
            # Get member summary stats
            member_stats = get_member_summary_stats(member['id'])
            
            with st.container():
                st.markdown(f"""
                <div class="member-card">
                    <div style="display: flex; justify-content: between; align-items: center;">
                        <div>
                            <h3 style="margin: 0; color: #1e293b;">👤 {member['name']}</h3>
                            <p style="margin: 5px 0; color: #64748b;">
                                📞 {member['phone'] or 'No phone'} | 
                                📅 Joined: {member['join_date']} | 
                                Status: <span style="color: {'#16a34a' if member['status'] == 'active' else '#dc2626'};">
                                    {member['status'].title()}
                                </span>
                            </p>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Quick stats row
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Shares", f"KSh {member_stats['shares']:,.0f}")
                with col2:
                    st.metric("Welfare", f"KSh {member_stats['welfare']:,.0f}")
                with col3:
                    st.metric("Loan Balance", f"KSh {member_stats['loan_balance']:,.0f}")
                with col4:
                    st.metric("Attendance", f"{member_stats['attendance_rate']:.1f}%")
                with col5:
                    if st.button("View Details", key=f"view_{member['id']}", type="secondary"):
                        st.session_state.selected_member_id = member['id']
                        st.session_state.show_member_details = True
        
        # Show member details modal
        if st.session_state.get('show_member_details') and st.session_state.get('selected_member_id'):
            show_member_details_modal(st.session_state.selected_member_id)
    
    finally:
        session.close()

def get_member_summary_stats(member_id):
    """Retrieves quick summary statistics for a member."""
    session = Session()
    try:
        shares = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM contributions 
            WHERE member_id = :mid AND votehead = 'shares'
        """), {'mid': member_id}).scalar()
        
        welfare = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM contributions 
            WHERE member_id = :mid AND votehead = 'welfare'
        """), {'mid': member_id}).scalar()
        
        # Calculate total loan balance
        loans = session.execute(text("""
            SELECT id FROM loans WHERE member_id = :mid AND status = 'active'
        """), {'mid': member_id}).fetchall()
        
        loan_balance = sum(calculate_loan_balance(loan[0]) for loan in loans)
        
        # Attendance rate
        attendance_data = session.execute(text("""
            SELECT COUNT(*) as total, SUM(CASE WHEN present THEN 1 ELSE 0 END) as present
            FROM attendance WHERE member_id = :mid
        """), {'mid': member_id}).fetchone()
        
        attendance_rate = (attendance_data.present / attendance_data.total * 100) if attendance_data.total > 0 else 0
        
        return {
            'shares': shares,
            'welfare': welfare,
            'loan_balance': loan_balance,
            'attendance_rate': attendance_rate
        }
    finally:
        session.close()

def show_member_details_modal(member_id):
    """Displays detailed member information in a modal-like container."""
    member_details = get_member_complete_details(member_id)
    
    if not member_details:
        st.error("Member not found!")
        return
    
    member = member_details['member']
    totals = member_details['totals']
    
    # Modal header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader(f"👤 {member.name} - Detailed Profile")
    with col2:
        if st.button("✖️ Close", key="close_member_details"):
            st.session_state.show_member_details = False
            st.rerun()
    
    # Member overview metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Shares", f"KSh {totals['shares_total']:,.2f}")
    with col2:
        st.metric("Total Welfare", f"KSh {totals['welfare_total']:,.2f}")
    with col3:
        st.metric("Active Loans", f"KSh {totals['active_loans_balance']:,.2f}")
    with col4:
        st.metric("Attendance Rate", f"{totals['attendance_rate']:.1f}%")
    
    # Detailed sections in tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["💰 Contributions", "🏦 Loans", "⚖️ Penalties", "💎 Dividends", "📅 Attendance"])
    
    with tab1:
        if not member_details['contributions'].empty:
            st.dataframe(
                member_details['contributions'][['date', 'votehead', 'amount', 'meeting_date']],
                use_container_width=True
            )
        else:
            st.info("No contributions recorded yet.")
    
    with tab2:
        if not member_details['loans'].empty:
            loans_display = member_details['loans'].copy()
            loans_display['balance'] = loans_display['balance'].round(2)
            st.dataframe(loans_display, use_container_width=True)
        else:
            st.info("No loans recorded yet.")
    
    with tab3:
        if not member_details['penalties'].empty:
            st.dataframe(member_details['penalties'], use_container_width=True)
        else:
            st.info("No penalties recorded.")
    
    with tab4:
        if not member_details['dividends'].empty:
            st.dataframe(member_details['dividends'], use_container_width=True)
        else:
            st.info("No dividends recorded yet.")
    
    with tab5:
        if not member_details['attendance'].empty:
            attendance_display = member_details['attendance'].copy()
            attendance_display['status'] = attendance_display['present'].map({True: '✅ Present', False: '❌ Absent'})
            st.dataframe(attendance_display[['date', 'status']], use_container_width=True)
        else:
            st.info("No attendance records found.")

def show_meetings():
    """Displays and manages meeting information and attendance."""
    st.title("📅 Meetings Management")
    
    # Add new meeting
    with st.expander("➕ Schedule New Meeting", expanded=False):
        with st.form("new_meeting_form"):
            col1, col2 = st.columns(2)
            with col1:
                meeting_date = st.date_input("Meeting Date", value=get_next_meeting_date())
                financial_year = st.text_input("Financial Year", value=get_financial_year(meeting_date))
            with col2:
                notes = st.text_area("Meeting Notes", placeholder="Enter any notes about the meeting...")
            
            if st.form_submit_button("Schedule Meeting", type="primary"):
                session = Session()
                try:
                    # Check if meeting already exists
                    existing = session.query(Meeting).filter(Meeting.date == meeting_date).first()
                    if existing:
                        st.error("❌ A meeting is already scheduled for this date!")
                    else:
                        new_meeting = Meeting(
                            date=meeting_date,
                            notes=notes.strip() if notes else None,
                            financial_year=financial_year
                        )
                        session.add(new_meeting)
                        session.commit()
                        st.success(f"✅ Meeting scheduled for {meeting_date.strftime('%A, %B %d, %Y')}")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Error scheduling meeting: {str(e)}")
                finally:
                    session.close()
    
    # Meetings list
    session = Session()
    try:
        meetings_df = pd.read_sql("""
            SELECT m.*, 
                   COUNT(a.id) as total_attendance,
                   SUM(CASE WHEN a.present THEN 1 ELSE 0 END) as present_count
            FROM meetings m
            LEFT JOIN attendance a ON m.id = a.meeting_id
            GROUP BY m.id
            ORDER BY m.date DESC
        """, engine)
        
        if meetings_df.empty:
            st.info("📅 No meetings scheduled yet.")
            return
        
        st.subheader(f"📋 Meetings History ({len(meetings_df)} meetings)")
        
        for _, meeting in meetings_df.iterrows():
            meeting_date = datetime.strptime(meeting['date'], '%Y-%m-%d').date()
            is_past = meeting_date < date.today()
            is_today = meeting_date == date.today()
            
            # Status indicator
            if is_today:
                status_color = "#f59e0b"
                status_text = "🔔 TODAY"
            elif is_past:
                status_color = "#64748b"
                status_text = "✅ COMPLETED"
            else:
                status_color = "#3b82f6"
                status_text = "📅 UPCOMING"
            
            with st.container():
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.markdown(f"""
                    **📅 {meeting_date.strftime('%A, %B %d, %Y')}**
                    <span style="color: {status_color}; font-weight: bold;">{status_text}</span>
                    
                    📝 *{meeting['notes'] or 'No notes'}*
                    """, unsafe_allow_html=True)
                
                with col2:
                    if meeting['total_attendance'] > 0:
                        attendance_rate = (meeting['present_count'] / meeting['total_attendance']) * 100
                        st.metric("Attendance", f"{attendance_rate:.1f}%", 
                                 f"{meeting['present_count']}/{meeting['total_attendance']}")
                    else:
                        st.write("No attendance recorded")
                
                with col3:
                    if st.button(f"Manage", key=f"manage_{meeting['id']}", type="secondary"):
                        st.session_state.selected_meeting_id = meeting['id']
                        st.session_state.show_meeting_management = True
        
        # Meeting management modal
        if st.session_state.get('show_meeting_management') and st.session_state.get('selected_meeting_id'):
            show_meeting_management_modal(st.session_state.selected_meeting_id)
    
    finally:
        session.close()

def show_meeting_management_modal(meeting_id):
    """Shows the interface for managing a specific meeting's attendance."""
    session = Session()
    try:
        meeting = session.query(Meeting).get(meeting_id)
        if not meeting:
            st.error("Meeting not found!")
            return
        
        # Modal header
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"📅 Meeting: {meeting.date.strftime('%A, %B %d, %Y')}")
        with col2:
            if st.button("✖️ Close", key="close_meeting_mgmt"):
                st.session_state.show_meeting_management = False
                st.rerun()
        
        # Get all members and their attendance for this meeting
        members_attendance = pd.read_sql("""
            SELECT m.id, m.name, 
                   COALESCE(a.present, 0) as present,
                   COALESCE(a.id, 0) as attendance_id
            FROM members m
            LEFT JOIN attendance a ON m.id = a.member_id AND a.meeting_id = ?
            WHERE m.status = 'active'
            ORDER BY m.name
        """, engine, params=[meeting_id])
        
        # Attendance management
        st.markdown("##### ✅ Mark Attendance")
        
        with st.form(f"attendance_form_{meeting_id}"):
            attendance_data = {}
            
            for _, member in members_attendance.iterrows():
                attendance_data[member['id']] = st.checkbox(
                    f"👤 {member['name']}", 
                    value=bool(member['present']),
                    key=f"attendance_{meeting_id}_{member['id']}"
                )
            
            if st.form_submit_button("💾 Save Attendance", type="primary"):
                # Update attendance records
                for member_id, is_present in attendance_data.items():
                    existing_attendance = session.query(Attendance).filter(
                        Attendance.meeting_id == meeting_id,
                        Attendance.member_id == member_id
                    ).first()
                    
                    if existing_attendance:
                        existing_attendance.present = is_present
                    else:
                        new_attendance = Attendance(
                            meeting_id=meeting_id,
                            member_id=member_id,
                            present=is_present
                        )
                        session.add(new_attendance)
                
                session.commit()
                st.success("✅ Attendance updated successfully!")
                st.rerun()
        
        # Show attendance summary
        present_count = sum(1 for _, member in members_attendance.iterrows() if member['present'])
        total_count = len(members_attendance)
        
        if total_count > 0:
            attendance_rate = (present_count / total_count) * 100
            st.success(f"📊 Attendance Summary: {present_count}/{total_count} members present ({attendance_rate:.1f}%)")
    
    finally:
        session.close()

def show_contributions():
    """Displays and manages member contributions, including share conversion, and loan repayments."""
    st.title("💰 Contributions & Repayments Management")
    
    # Quick stats
    session = Session()
    try:
        col1, col2, col3 = st.columns(3)
        with col1:
            shares_total = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = 'shares'")).scalar()
            st.metric("Total Shares Value", f"KSh {shares_total:,.2f}")
        with col2:
            welfare_total = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = 'welfare'")).scalar()
            st.metric("Total Welfare", f"KSh {welfare_total:,.2f}")
        with col3:
            this_month = session.execute(text("""
                SELECT COALESCE(SUM(amount), 0) FROM contributions 
                WHERE strftime('%Y-%m', date) = strftime('%Y-%m', 'now')
            """)).scalar()
            st.metric("This Month", f"KSh {this_month:,.2f}")
    finally:
        session.close()
    
    # Add contribution form
    with st.expander("➕ Record New Contribution", expanded=False):
        with st.form("contribution_form"):
            col1, col2 = st.columns(2)
            with col1:
                # Get active members for dropdown
                members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active' ORDER BY name", engine)
                member_options = {f"{row['name']}": row['id'] for _, row in members_df.iterrows()}
                
                selected_member = st.selectbox("Select Member", options=list(member_options.keys()))
                contribution_date = st.date_input("Date", value=date.today())
            
            with col2:
                votehead = st.selectbox("Vote Head", ["shares", "welfare"])
                amount = st.number_input("Amount (KSh)", min_value=0.0, step=10.0)
            
            # Optional meeting association
            meetings_df = pd.read_sql("SELECT id, date FROM meetings ORDER BY date DESC LIMIT 10", engine)
            meeting_options = {"No meeting": None}
            meeting_options.update({f"Meeting - {row['date']}": row['id'] for _, row in meetings_df.iterrows()})
            
            selected_meeting = st.selectbox("Associate with Meeting (Optional)", options=list(meeting_options.keys()))
            
            if st.form_submit_button("💾 Record Contribution", type="primary"):
                if selected_member and amount > 0:
                    if votehead == 'shares' and amount < SHARE_VALUE:
                        st.error(f"❌ Share contributions must be at least KSh {SHARE_VALUE:,.2f} per share.")
                    else:
                        session = Session()
                        try:
                            member_id = member_options[selected_member]
                            meeting_id = meeting_options[selected_meeting]
                            
                            new_contribution = Contribution(
                                member_id=member_id,
                                meeting_id=meeting_id,
                                votehead=votehead,
                                amount=amount,
                                date=contribution_date
                            )
                            
                            session.add(new_contribution)
                            session.commit()
                            st.success(f"✅ Contribution of KSh {amount:,.2f} recorded for {selected_member}")
                            if votehead == 'shares':
                                shares_gained = int(amount / SHARE_VALUE)
                                st.success(f"🎉 {shares_gained} share(s) added!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error recording contribution: {str(e)}")
                        finally:
                            session.close()
                else:
                    st.error("Please select a member and enter a valid amount.")

    # New section for recording loan repayments on the contributions page
    st.markdown("---")
    with st.expander("💸 Record Loan Repayment", expanded=True): # Expanded by default for visibility
        with st.form("loan_repayment_quick_form"):
            st.subheader("Quick Loan Repayment")
            session = Session()
            
            # Get all active members for the first dropdown
            all_members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active' ORDER BY name", engine)
            member_repay_options = {"-- Select Member --": None}
            member_repay_options.update({f"{row['name']}": row['id'] for _, row in all_members_df.iterrows()})
            
            selected_repay_member_name = st.selectbox(
                "Select Member to Repay Loan For", 
                options=list(member_repay_options.keys()), 
                key="quick_repay_member_select"
            )
            
            selected_repay_member_id = member_repay_options[selected_repay_member_name]
            
            # Filter loans based on selected member
            filtered_loans_df = pd.DataFrame()
            if selected_repay_member_id:
                try:
                    filtered_loans_df = pd.read_sql(f"""
                        SELECT l.id, m.name as member_name, l.amount, l.start_date, l.due_date, l.type
                        FROM loans l
                        JOIN members m ON l.member_id = m.id
                        WHERE l.status = 'active' AND l.member_id = {selected_repay_member_id}
                        ORDER BY l.start_date DESC
                    """, engine)
                except Exception as e:
                    st.error(f"Error fetching loans for selected member: {e}")

            loan_options_display = ["-- Select a Loan --"]
            loan_options_map = {}
            selected_loan_id = None
            current_loan_balance = 0.0

            if not filtered_loans_df.empty:
                filtered_loans_df['current_balance'] = filtered_loans_df.apply(
                    lambda row: calculate_loan_balance(row['id']), axis=1
                )
                
                for _, row in filtered_loans_df.iterrows():
                    display_text = f"{row['type'].title()} Loan (KSh {row['current_balance']:,.2f} balance, Started: {row['start_date']})"
                    loan_options_display.append(display_text)
                    loan_options_map[display_text] = row['id']
            
            selected_loan_display = st.selectbox(
                "Select Loan to Repay", 
                options=loan_options_display, 
                key="quick_repay_loan_select"
            )

            if selected_loan_display != "-- Select a Loan --":
                selected_loan_id = loan_options_map[selected_loan_display]
                # Ensure the balance is for the *selected* loan, not just any active loan
                loan_row = filtered_loans_df[filtered_loans_df['id'] == selected_loan_id]
                if not loan_row.empty:
                    current_loan_balance = loan_row['current_balance'].iloc[0]
                else:
                    current_loan_balance = 0.0 # Fallback if loan not found (shouldn't happen with correct filtering)

            repayment_amount = st.number_input(
                "Repayment Amount (KSh)", 
                min_value=0.0, 
                max_value=float(current_loan_balance) if selected_loan_id else 0.0, # Max value is the selected loan's current balance
                step=50.0, 
                key="quick_repay_amount"
            )
            repayment_date = st.date_input("Date of Repayment", value=date.today(), key="quick_repay_date")

            # The submit button is now unconditionally inside the form
            if st.form_submit_button("Record Repayment", type="primary"):
                if selected_repay_member_id is None:
                    st.error("Please select a member first.")
                elif selected_loan_id is None or selected_loan_display == "-- Select a Loan --":
                    st.error("Please select a valid loan from the dropdown for the selected member.")
                elif repayment_amount <= 0:
                    st.error("Please enter a valid repayment amount greater than zero.")
                elif repayment_amount > current_loan_balance:
                    st.error(f"Repayment amount cannot exceed the current loan balance of KSh {current_loan_balance:,.2f}.")
                else:
                    try:
                        record_loan_repayment(selected_loan_id, repayment_amount, repayment_date)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error recording repayment: {e}")
            session.close() # Ensure session is closed after form submission logic

    st.markdown("---") # Separator between forms and history

    # Contributions history
    st.subheader("📊 Contributions History")
    
    # Filter options
    col1, col2, col3 = st.columns(3)
    with col1:
        date_filter = st.selectbox("Time Period", ["All Time", "This Month", "Last 3 Months", "This Year"])
    with col2:
        votehead_filter = st.selectbox("Vote Head", ["All", "Shares", "Welfare"])
    with col3:
        member_filter = st.text_input("Search Member", placeholder="Enter member name...")
    
    # Build query
    query = """
        SELECT c.date, m.name, c.votehead, c.amount, 
               COALESCE(mt.date, 'N/A') as meeting_date
        FROM contributions c
        JOIN members m ON c.member_id = m.id
        LEFT JOIN meetings mt ON c.meeting_id = mt.id
        WHERE 1=1
    """
    params = []
    
    # Apply filters
    if date_filter == "This Month":
        query += " AND strftime('%Y-%m', c.date) = strftime('%Y-%m', 'now')"
    elif date_filter == "Last 3 Months":
        query += " AND c.date >= date('now', '-3 months')"
    elif date_filter == "This Year":
        query += " AND strftime('%Y', c.date) = strftime('%Y', 'now')"
    
    if votehead_filter != "All":
        query += " AND c.votehead = ?"
        params.append(votehead_filter.lower())
    
    if member_filter:
        query += " AND m.name LIKE ?"
        params.append(f"%{member_filter}%")
    
    query += " ORDER BY c.date DESC, m.name"
    
    contributions_df = pd.read_sql(query, engine, params=params)
    
    if not contributions_df.empty:
        # Summary stats for filtered data
        st.info(f"📈 Showing {len(contributions_df)} contributions totaling KSh {contributions_df['amount'].sum():,.2f}")
        
        # Display contributions table
        st.dataframe(
            contributions_df.rename(columns={
                'date': 'Date',
                'name': 'Member',
                'votehead': 'Vote Head',
                'amount': 'Amount (KSh)',
                'meeting_date': 'Meeting Date'
            }),
            use_container_width=True
        )
        
        # Export option
        if st.button("📊 Export to CSV"):
            csv = contributions_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"contributions_{date.today()}.csv",
                mime="text/csv"
            )
    else:
        st.info("No contributions found matching your criteria.")

def show_loans():
    """Displays and manages loan applications and repayments."""
    st.title("🏦 Loans Management")
    
    # Quick stats
    session = Session()
    try:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            active_loans = session.execute(text("SELECT COUNT(*) FROM loans WHERE status = 'active'")).scalar()
            st.metric("Active Loans", active_loans)
        with col2:
            total_disbursed = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM loans")).scalar()
            st.metric("Total Disbursed", f"KSh {total_disbursed:,.2f}")
        with col3:
            total_repaid = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM repayments")).scalar()
            st.metric("Total Repaid", f"KSh {total_repaid:,.2f}")  
        with col4:
            # Re-calculate outstanding based on current balances of active loans
            loans_df_for_outstanding = pd.read_sql("SELECT id FROM loans WHERE status = 'active'", engine)
            calculated_outstanding = sum(calculate_loan_balance(loan_id[0]) for loan_id in loans_df_for_outstanding.values)
            st.metric("Total Outstanding", f"KSh {calculated_outstanding:,.2f}")
    finally:
        session.close()
    
    # Loan management tabs
    tab1, tab2, tab3 = st.tabs(["💰 New Loan", "📋 Active Loans", "💸 Repayments"])
    
    with tab1:
        show_new_loan_form()
    
    with tab2:
        show_active_loans()
    
    with tab3:
        show_loan_repayments()

def show_new_loan_form():
    """Form for creating new loans."""
    with st.form("new_loan_form"):
        st.subheader("Apply for a New Loan")
        col1, col2 = st.columns(2)
        
        with col1:
            # Get active members
            members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active' ORDER BY name", engine)
            member_options = {f"{row['name']}": row['id'] for _, row in members_df.iterrows()}
            
            selected_member = st.selectbox("Select Member", options=list(member_options.keys()))
            loan_type = st.selectbox("Loan Type", ["development", "emergency"])
            amount = st.number_input("Loan Amount (KSh)", min_value=100.0, step=100.0)
        
        with col2:
            # Set default interest rate and due date based on loan type
            default_interest_rate = 10.0 if loan_type == "development" else 2.0 # 2% for emergency
            interest_rate = st.number_input("Interest Rate (%)", min_value=0.0, max_value=100.0, 
                                            value=default_interest_rate, step=0.5)
            
            start_date = st.date_input("Start Date", value=date.today())
            
            if loan_type == "development":
                default_due = start_date + timedelta(days=365)
            else: # emergency
                default_due = start_date + timedelta(days=30) # 30 days for emergency loans
            
            due_date = st.date_input("Due Date", value=default_due)
        
        if st.form_submit_button("💰 Approve Loan", type="primary"):
            if selected_member and amount > 0:
                session = Session()
                try:
                    member_id = member_options[selected_member]
                    
                    new_loan = Loan(
                        member_id=member_id,
                        type=loan_type,
                        amount=amount,
                        interest_rate=interest_rate,
                        start_date=start_date,
                        due_date=due_date,
                        status='active'
                    )
                    
                    session.add(new_loan)
                    session.commit()
                    st.success(f"✅ Loan of KSh {amount:,.2f} approved for {selected_member}")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error creating loan: {str(e)}")
                finally:
                    session.close()
            else:
                st.error("Please select a member and enter a valid amount.")

def show_active_loans():
    """Displays active loans with management options for repayments."""
    loans_df = pd.read_sql("""
        SELECT l.id, m.name as member_name, l.type, l.amount, l.interest_rate,
               l.start_date, l.due_date, l.status,
               COALESCE(SUM(r.amount), 0) as total_repaid
        FROM loans l
        JOIN members m ON l.member_id = m.id
        LEFT JOIN repayments r ON l.id = r.loan_id
        WHERE l.status = 'active'
        GROUP BY l.id
        ORDER BY l.due_date
    """, engine)
    
    if loans_df.empty:
        st.info("No active loans found.")
        return
    
    # Calculate balances and status
    loans_df['balance'] = loans_df.apply(lambda row: calculate_loan_balance(row['id']), axis=1)
    loans_df['days_to_due'] = loans_df['due_date'].apply(
        lambda x: (datetime.strptime(x, '%Y-%m-%d').date() - date.today()).days
    )
    loans_df['status_icon'] = loans_df['days_to_due'].apply(
        lambda x: "🚨" if x < 0 else "⚠️" if x <= 7 else "✅"
    )
    
    st.subheader(f"📋 Active Loans ({len(loans_df)} loans)")
    
    for _, loan in loans_df.iterrows():
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            
            with col1:
                st.markdown(f"""
                **{loan['status_icon']} {loan['member_name']}**
                *{loan['type'].title()} Loan - {loan['interest_rate']}% interest*
                """)
            
            with col2:
                st.metric("Amount", f"KSh {loan['amount']:,.2f}")
                
            with col3:
                st.metric("Balance", f"KSh {loan['balance']:,.2f}")
                
            with col4:
                days_text = f"{abs(loan['days_to_due'])} days {'overdue' if loan['days_to_due'] < 0 else 'remaining'}"
                st.write(days_text)
            
            # Repayment section (this is already there, but user requested another one on contributions page)
            with st.expander(f"💸 Record Repayment for {loan['member_name']}"):
                with st.form(f"repayment_form_{loan['id']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        repayment_amount = st.number_input(
                            "Repayment Amount", 
                            min_value=0.0, 
                            max_value=float(loan['balance']),
                            step=50.0,
                            key=f"repay_{loan['id']}"
                        )
                    with col2:
                        repayment_date = st.date_input("Date", value=date.today(), key=f"repay_date_{loan['id']}")
                    
                    if st.form_submit_button(f"Record Repayment", key=f"submit_repay_{loan['id']}"):
                        if repayment_amount > 0:
                            record_loan_repayment(loan['id'], repayment_amount, repayment_date)
                            st.rerun()

def record_loan_repayment(loan_id, amount, repayment_date):
    """Records a loan repayment and updates loan status if fully paid."""
    session = Session()
    try:
        # Create repayment record
        new_repayment = Repayment(
            loan_id=loan_id,
            amount=amount,
            date=repayment_date
        )
        session.add(new_repayment)
        
        # Check if loan is fully paid
        current_balance = calculate_loan_balance(loan_id)
        if current_balance <= amount: # If new repayment covers or exceeds the balance
            loan = session.query(Loan).get(loan_id)
            loan.status = 'completed'
        
        session.commit()
        st.success(f"✅ Repayment of KSh {amount:,.2f} recorded successfully!")
        
    except Exception as e:
        session.rollback()
        st.error(f"❌ Error recording repayment: {str(e)}")
    finally:
        session.close()

def show_loan_repayments():
    """Displays loan repayment history with filtering options."""
    st.subheader("💸 Repayment History")
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        date_filter = st.selectbox("Period", ["All Time", "This Month", "Last 3 Months", "This Year"], key="repay_filter")
    with col2:
        member_search = st.text_input("Search Member", placeholder="Enter member name...", key="repay_search")
    
    # Query repayments with filters
    query = """
        SELECT r.date, m.name as member_name, l.type as loan_type, 
               r.amount, l.amount as loan_amount
        FROM repayments r
        JOIN loans l ON r.loan_id = l.id
        JOIN members m ON l.member_id = m.id
        WHERE 1=1
    """
    params = []
    
    if date_filter == "This Month":
        query += " AND strftime('%Y-%m', r.date) = strftime('%Y-%m', 'now')"
    elif date_filter == "Last 3 Months":
        query += " AND r.date >= date('now', '-3 months')"
    elif date_filter == "This Year":
        query += " AND strftime('%Y', r.date) = strftime('%Y', 'now')"
    
    if member_search:
        query += " AND m.name LIKE ?"
        params.append(f"%{member_search}%")
    
    query += " ORDER BY r.date DESC"
    
    repayments_df = pd.read_sql(query, engine, params=params)
    
    if not repayments_df.empty:
        st.info(f"📊 Showing {len(repayments_df)} repayments totaling KSh {repayments_df['amount'].sum():,.2f}")
        
        st.dataframe(
            repayments_df.rename(columns={
                'date': 'Date',
                'member_name': 'Member',
                'loan_type': 'Loan Type',
                'amount': 'Repayment (KSh)',
                'loan_amount': 'Original Loan (KSh)'
            }),
            use_container_width=True
        )
    else:
        st.info("No repayments found matching your criteria.")

def show_reports():
    """Generates and displays various financial and operational reports."""
    st.title("📊 Reports & Analytics")
    
    # Report type selection
    report_type = st.selectbox(
        "Select Report Type",
        ["Financial Summary", "Member Performance", "Loan Analysis", "Attendance Report", "Monthly Statement"]
    )
    
    if report_type == "Financial Summary":
        show_financial_summary_report()
    elif report_type == "Member Performance":
        show_member_performance_report()
    elif report_type == "Loan Analysis":
        show_loan_analysis_report()
    elif report_type == "Attendance Report":
        show_attendance_report()
    elif report_type == "Monthly Statement":
        show_monthly_statement_report()

def show_financial_summary_report():
    """Displays a financial summary report for a selected date range."""
    st.subheader("💰 Financial Summary Report")
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", value=date.today().replace(day=1))
    with col2:
        end_date = st.date_input("To Date", value=date.today())
    
    if start_date > end_date:
        st.error("Start date cannot be after end date!")
        return
    
    session = Session()
    try:
        # Get financial data
        financial_data = session.execute(text("""
            SELECT 
                COALESCE(SUM(CASE WHEN votehead = 'shares' THEN amount END), 0) as total_shares,
                COALESCE(SUM(CASE WHEN votehead = 'welfare' THEN amount END), 0) as total_welfare,
                COUNT(DISTINCT member_id) as contributing_members
            FROM contributions 
            WHERE date BETWEEN :start_date AND :end_date
        """), {'start_date': start_date, 'end_date': end_date}).fetchone()
        
        loans_data = session.execute(text("""
            SELECT 
                COALESCE(SUM(amount), 0) as loans_disbursed,
                COUNT(*) as loans_count
            FROM loans 
            WHERE start_date BETWEEN :start_date AND :end_date
        """), {'start_date': start_date, 'end_date': end_date}).fetchone()
        
        # FIX: Corrected the parameters for repayments_data query to use start_date and end_date
        repayments_data = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) as total_repayments
            FROM repayments 
            WHERE date BETWEEN :start_date AND :end_date
        """), {'start_date': start_date, 'end_date': end_date}).scalar()
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Shares", f"KSh {financial_data.total_shares:,.2f}")
        with col2:
            st.metric("Total Welfare", f"KSh {financial_data.total_welfare:,.2f}")
        with col3:
            st.metric("Loans Disbursed", f"KSh {loans_data.loans_disbursed:,.2f}")
        with col4:
            st.metric("Loan Repayments", f"KSh {repayments_data:,.2f}") # Use repayments_data directly
        
        # Summary table
        st.subheader("📋 Summary")
        summary_data = {
            'Category': ['Member Contributions', 'Loans Disbursed', 'Loan Repayments', 'Net Cash Flow'],
            'Shares (KSh)': [financial_data.total_shares, 0, 0, financial_data.total_shares],
            'Welfare (KSh)': [financial_data.total_welfare, 0, 0, financial_data.total_welfare],
            'Loans (KSh)': [0, -loans_data.loans_disbursed, repayments_data, # Use repayments_data directly
                           repayments_data - loans_data.loans_disbursed],
            'Total (KSh)': [
                financial_data.total_shares + financial_data.total_welfare,
                -loans_data.loans_disbursed,
                repayments_data, # Use repayments_data directly
                financial_data.total_shares + financial_data.total_welfare + 
                repayments_data - loans_data.loans_disbursed
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True)
        
    finally:
        session.close()

def show_member_performance_report():
    """Displays a report on individual member performance based on contributions, loans, and attendance."""
    st.subheader("👥 Member Performance Report")
    
    # Get member performance data
    member_performance = pd.read_sql("""
        SELECT 
            m.name,
            m.join_date,
            COALESCE(SUM(CASE WHEN c.votehead = 'shares' THEN c.amount END), 0) as total_shares,
            COALESCE(SUM(CASE WHEN c.votehead = 'welfare' THEN c.amount END), 0) as total_welfare,
            COUNT(DISTINCT c.id) as contribution_count,
            COUNT(DISTINCT l.id) as loan_count,
            COALESCE(SUM(l.amount), 0) as total_loans,
            (SELECT COUNT(*) FROM attendance a 
             WHERE a.member_id = m.id AND a.present = 1) as meetings_attended,
            (SELECT COUNT(*) FROM attendance a 
             WHERE a.member_id = m.id) as total_meetings
        FROM members m
        LEFT JOIN contributions c ON m.id = c.member_id
        LEFT JOIN loans l ON m.id = l.member_id
        WHERE m.status = 'active'
        GROUP BY m.id
        ORDER BY total_shares + total_welfare DESC
    """, engine)
    
    if member_performance.empty:
        st.info("No member data available.")
        return
    
    # Calculate attendance rate
    member_performance['attendance_rate'] = (
        member_performance['meetings_attended'] / 
        member_performance['total_meetings'].replace(0, 1) * 100
    ).round(1)
    
    # Display top performers
    st.subheader("🏆 Top Contributors")
    top_contributors = member_performance.head(5)
    
    for i, (_, member) in enumerate(top_contributors.iterrows(), 1):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.write(f"**{i}. {member['name']}**")
        with col2:
            st.metric("Total Contributions", f"KSh {member['total_shares'] + member['total_welfare']:,.2f}")
        with col3:
            st.metric("Attendance", f"{member['attendance_rate']:.1f}%")
        with col4:
            st.metric("Loans Taken", int(member['loan_count']))
    
    # Full member table
    st.subheader("📊 All Members Performance")
    display_df = member_performance.copy()
    display_df['total_contributions'] = display_df['total_shares'] + display_df['total_welfare']
    
    st.dataframe(
        display_df[['name', 'total_contributions', 'total_shares', 'total_welfare', 
                   'loan_count', 'attendance_rate']].rename(columns={
            'name': 'Member Name',
            'total_contributions': 'Total Contributions (KSh)',
            'total_shares': 'Shares (KSh)',
            'total_welfare': 'Welfare (KSh)',
            'loan_count': 'Loans Taken',
            'attendance_rate': 'Attendance (%)'
        }),
        use_container_width=True
    )

def show_loan_analysis_report():
    """Displays a detailed loan analysis report."""
    st.subheader("🏦 Loan Analysis Report")
    
    session = Session()
    try:
        # Loan overview metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            active_loans = session.execute(text("SELECT COUNT(*) FROM loans WHERE status = 'active'")).scalar()
            st.metric("Active Loans", active_loans)
        
        with col2:
            overdue_loans = session.execute(text("""
                SELECT COUNT(*) FROM loans 
                WHERE status = 'active' AND due_date < date('now')
            """)).scalar()
            st.metric("Overdue Loans", overdue_loans)
        
        with col3:
            avg_loan_amount = session.execute(text("SELECT AVG(amount) FROM loans")).scalar() or 0
            st.metric("Avg Loan Amount", f"KSh {avg_loan_amount:,.2f}")
        
        with col4:
            # Collection rate based on disbursed vs. repaid
            collection_rate_data = session.execute(text("""
                SELECT 
                    COALESCE(SUM(l.amount), 0) as total_disbursed,
                    COALESCE(SUM(r.amount), 0) as total_repaid
                FROM loans l
                LEFT JOIN repayments r ON l.id = r.loan_id
            """)).fetchone()
            
            total_disbursed_for_rate = collection_rate_data.total_disbursed
            total_repaid_for_rate = collection_rate_data.total_repaid

            collection_rate = (total_repaid_for_rate / total_disbursed_for_rate * 100) if total_disbursed_for_rate > 0 else 0
            st.metric("Collection Rate", f"{collection_rate:.1f}%")
        
        # Loan type breakdown
        st.subheader("📊 Loan Distribution")
        loan_breakdown = pd.read_sql("""
            SELECT 
                type as loan_type,
                COUNT(*) as count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM loans
            GROUP BY type
        """, engine)
        
        if not loan_breakdown.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.write("**By Count:**")
                for _, row in loan_breakdown.iterrows():
                    st.write(f"• {row['loan_type'].title()}: {row['count']} loans")
            
            with col2:
                st.write("**By Amount:**")
                for _, row in loan_breakdown.iterrows():
                    st.write(f"• {row['loan_type'].title()}: KSh {row['total_amount']:,.2f}")
        
        # Overdue loans details
        if overdue_loans > 0:
            st.subheader("🚨 Overdue Loans")
            overdue_details = pd.read_sql("""
                SELECT 
                    m.name as member_name,
                    l.type,
                    l.amount,
                    l.due_date,
                    (julianday('now') - julianday(l.due_date)) as days_overdue
                FROM loans l
                JOIN members m ON l.member_id = m.id
                WHERE l.status = 'active' AND l.due_date < date('now')
                ORDER BY days_overdue DESC
            """, engine)
            
            for _, loan in overdue_details.iterrows():
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**{loan['member_name']}**")
                with col2:
                    st.write(f"KSh {loan['amount']:,.2f} ({loan['type']})")
                with col3:
                    st.write(f"⏰ {int(loan['days_overdue'])} days overdue")
    
    finally:
        session.close()

def show_attendance_report():
    """Displays an attendance analysis report for a selected period."""
    st.subheader("📅 Attendance Report")
    
    # Date range for analysis
    col1, col2 = st.columns(2)
    with col1:
        months_back = st.selectbox("Analysis Period", [3, 6, 12], format_func=lambda x: f"Last {x} months")
    with col2:
        start_analysis = date.today() - timedelta(days=months_back * 30)
        st.write(f"Analyzing from: {start_analysis.strftime('%Y-%m-%d')}")
    
    # Overall attendance statistics
    attendance_stats = pd.read_sql("""
        SELECT 
            m.name,
            COUNT(a.id) as total_meetings,
            SUM(CASE WHEN a.present THEN 1 ELSE 0 END) as attended,
            ROUND(
                (SUM(CASE WHEN a.present THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 1
            ) as attendance_rate
        FROM members m
        LEFT JOIN attendance a ON m.id = a.member_id
        LEFT JOIN meetings mt ON a.meeting_id = mt.id
        WHERE m.status = 'active' 
        AND (mt.date IS NULL OR mt.date >= ?)
        GROUP BY m.id
        HAVING total_meetings > 0
        ORDER BY attendance_rate DESC
    """, engine, params=[start_analysis])
    
    if attendance_stats.empty:
        st.info("No attendance data available for the selected period.")
        return
    
    # Attendance categories
    excellent = attendance_stats[attendance_stats['attendance_rate'] >= 90]
    good = attendance_stats[(attendance_stats['attendance_rate'] >= 70) & (attendance_stats['attendance_rate'] < 90)]
    poor = attendance_stats[attendance_stats['attendance_rate'] < 70]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Excellent (≥90%)", len(excellent), f"{len(excellent)/len(attendance_stats)*100:.1f}%")
    with col2:
        st.metric("Good (70-89%)", len(good), f"{len(good)/len(attendance_stats)*100:.1f}%")
    with col3:
        st.metric("Poor (<70%)", len(poor), f"{len(poor)/len(attendance_stats)*100:.1f}%")
    
    # Member attendance details
    st.subheader("👥 Member Attendance Details")
    st.dataframe(
        attendance_stats.rename(columns={
            'name': 'Member Name',
            'total_meetings': 'Total Meetings',
            'attended': 'Attended',
            'attendance_rate': 'Attendance Rate (%)'
        }),
        use_container_width=True
    )
    
    # Meeting-wise attendance
    st.subheader("📊 Meeting-wise Attendance")
    meeting_attendance = pd.read_sql("""
        SELECT 
            mt.date,
            COUNT(a.id) as total_marked,
            SUM(CASE WHEN a.present THEN 1 ELSE 0 END) as present_count,
            ROUND(
                (SUM(CASE WHEN a.present THEN 1 ELSE 0 END) * 100.0 / COUNT(a.id)), 1
            ) as meeting_attendance_rate
        FROM meetings mt
        LEFT JOIN attendance a ON mt.id = a.meeting_id
        WHERE mt.date >= ?
        GROUP BY mt.id
        ORDER BY mt.date DESC
    """, engine, params=[start_analysis])
    
    if not meeting_attendance.empty:
        st.dataframe(
            meeting_attendance.rename(columns={
                'date': 'Meeting Date',
                'total_marked': 'Members Marked',
                'present_count': 'Present',
                'meeting_attendance_rate': 'Attendance Rate (%)'
            }),
            use_container_width=True
        )

def show_monthly_statement_report():
    """Generates and displays a monthly financial statement."""
    st.subheader("📄 Monthly Statement")
    
    # Month selection
    col1, col2 = st.columns(2)
    with col1:
        selected_year = st.selectbox("Year", range(2020, 2031), index=5)  # Default to 2025
    with col2:
        selected_month = st.selectbox("Month", range(1, 13), format_func=lambda x: calendar.month_name[x])
    
    # Generate statement for selected month
    month_start = date(selected_year, selected_month, 1)
    if selected_month == 12:
        month_end = date(selected_year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(selected_year, selected_month + 1, 1) - timedelta(days=1)
    
    st.write(f"**Statement Period:** {month_start.strftime('%B %Y')}")
    
    session = Session()
    try:
        # Opening balances (previous month)
        prev_month_end = month_start - timedelta(days=1)
        
        opening_shares = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM contributions 
            WHERE votehead = 'shares' AND date <= :end_date
        """), {'end_date': prev_month_end}).scalar()
        
        opening_welfare = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM contributions 
            WHERE votehead = 'welfare' AND date <= :end_date
        """), {'end_date': prev_month_end}).scalar()
        
        # Current month transactions
        month_shares = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM contributions 
            WHERE votehead = 'shares' AND date BETWEEN :start_date AND :end_date
        """), {'start_date': month_start, 'end_date': month_end}).scalar()
        
        month_welfare = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM contributions 
            WHERE votehead = 'welfare' AND date BETWEEN :start_date AND :end_date
        """), {'start_date': month_start, 'end_date': month_end}).scalar()
        
        month_loans_disbursed = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM loans 
            WHERE start_date BETWEEN :start_date AND :end_date
        """), {'start_date': month_start, 'end_date': month_end}).scalar()
        
        month_repayments = session.execute(text("""
            SELECT COALESCE(SUM(amount), 0) FROM repayments 
            WHERE date BETWEEN :start_date AND :end_date
        """), {'start_date': month_start, 'end_date': month_end}).scalar()
        
        # Create statement table
        statement_data = {
            'Description': [
                'Opening Balance - Shares',
                'Opening Balance - Welfare',
                'Monthly Shares Contributions',
                'Monthly Welfare Contributions',
                'Loans Disbursed',
                'Loan Repayments Received',
                'Closing Balance - Shares',
                'Closing Balance - Welfare',
                'Net Cash Position'
            ],
            'Amount (KSh)': [
                f"{opening_shares:,.2f}",
                f"{opening_welfare:,.2f}",
                f"{month_shares:,.2f}",
                f"{month_welfare:,.2f}",
                f"-{month_loans_disbursed:,.2f}",
                f"{month_repayments:,.2f}",
                f"{opening_shares + month_shares:,.2f}",
                f"{opening_welfare + month_welfare:,.2f}",
                f"{opening_shares + opening_welfare + month_shares + month_welfare - month_loans_disbursed + month_repayments:,.2f}"
            ]
        }
        
        statement_df = pd.DataFrame(statement_data)
        st.dataframe(statement_df, use_container_width=True)
        
        # Export option
        if st.button("📊 Export Statement"):
            csv = statement_df.to_csv(index=False)
            st.download_button(
                label="Download Monthly Statement",
                data=csv,
                file_name=f"monthly_statement_{selected_year}_{selected_month:02d}.csv",
                mime="text/csv"
            )
    
    finally:
        session.close()

# Main application logic
def main():
    """Main function to run the Streamlit application."""
    # Initialize session states if they don't exist
    if 'selected_member_id' not in st.session_state:
        st.session_state.selected_member_id = None
    if 'show_member_details' not in st.session_state:
        st.session_state.show_member_details = False
    if 'selected_meeting_id' not in st.session_state:
        st.session_state.selected_meeting_id = None
    if 'show_meeting_management' not in st.session_state:
        st.session_state.show_meeting_management = False

    with st.sidebar:
        st.title("🤝 Shalom Blessing SHG") # Changed title to include handshake emoji
        # Updated st.image to use use_container_width and modified text in placeholder URL
        st.image("https://placehold.co/150x150/84fab0/1e293b?text=SHG", use_container_width=True) 
        st.markdown("---")
        
        page = st.radio("Navigation", 
            ["📊 Dashboard", "👥 Members", "📅 Meetings", "💰 Contributions", "🏦 Loans", "📊 Reports", "⚙️ Settings"])
        
        st.markdown("---")
        st.write("Developed for Shalom Blessing Group")

    if page == "📊 Dashboard":
        show_dashboard()
    elif page == "👥 Members":
        show_members()
    elif page == "📅 Meetings":
        show_meetings()
    elif page == "💰 Contributions":
        show_contributions()
    elif page == "🏦 Loans":
        show_loans()
    elif page == "📊 Reports":
        show_reports()
    elif page == "⚙️ Settings":
        st.title("⚙️ Application Settings")
        st.info("Settings functionality can be added here (e.g., interest rates, group name).")
        # Example setting: share value
        current_share_value = SHARE_VALUE # Use the global constant for display
        new_share_value = st.number_input(
            "Value per Share (KSh)", 
            min_value=1.0, 
            value=float(current_share_value), 
            step=100.0,
            help="Defines the value of one share in KSh. Monthly contributions for 'shares' will be converted based on this value."
        )
        # Note: For this change to persist across sessions, you'd need to save it to the `Setting` table.
        # For simplicity in this merged app, we'll keep it as a constant for now.
        if st.button("Save Share Value"):
            # This would typically save to the database. For now, it's just a display.
            # save_setting("share_value", str(new_share_value)) 
            st.success(f"Share value is currently set to KSh {new_share_value:,.2f}. (Requires backend update for persistence)")


if __name__ == "__main__":
    main()
