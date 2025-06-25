import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import calendar
from sqlalchemy import create_engine, text, Column, Integer, String, Float, Boolean, Date, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import os
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import io
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import hashlib
import json # Added for backup/restore functionality

# --- Database Configuration ---
# This uses an environment variable for the database URL.
# If not set, it defaults to a local PostgreSQL database.
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/shalom_blessing')

# Create SQLAlchemy engine
# @st.cache_resource is used to cache the database engine,
# preventing it from being re-created on every Streamlit rerun.
@st.cache_resource
def get_database_engine():
    return create_engine(DATABASE_URL)

engine = get_database_engine()
Base = declarative_base()

# --- Database Models ---
# These classes define the structure of your database tables using SQLAlchemy.
# Each class corresponds to a table, and each Column corresponds to a field.

class Member(Base):
    __tablename__ = 'members'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    phone = Column(String(20))
    status = Column(String(20), default='active') # e.g., 'active', 'inactive'
    join_date = Column(Date, default=date.today)

class Meeting(Base):
    __tablename__ = 'meetings'
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    notes = Column(String(500))
    financial_year = Column(String(10)) # e.g., '2023-2024'

class Attendance(Base):
    __tablename__ = 'attendance'
    
    id = Column(Integer, primary_key=True)
    meeting_id = Column(Integer, ForeignKey('meetings.id'))
    member_id = Column(Integer, ForeignKey('members.id'))
    present = Column(Boolean, default=False)

class Contribution(Base):
    __tablename__ = 'contributions'
    
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id'))
    meeting_id = Column(Integer, ForeignKey('meetings.id'))
    votehead = Column(String(20))  # 'shares' or 'welfare'
    amount = Column(Float)
    date = Column(Date, default=date.today)

class Loan(Base):
    __tablename__ = 'loans'
    
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id'))
    type = Column(String(20))  # 'normal' or 'emergency'
    amount = Column(Float)
    interest_rate = Column(Float) # Percentage, e.g., 10.0 for 10%
    start_date = Column(Date)
    due_date = Column(Date)
    status = Column(String(20), default='active') # e.g., 'active', 'paid', 'overdue'

class Repayment(Base):
    __tablename__ = 'repayments'
    
    id = Column(Integer, primary_key=True)
    loan_id = Column(Integer, ForeignKey('loans.id'))
    amount = Column(Float)
    date = Column(Date, default=date.today)

class Penalty(Base):
    __tablename__ = 'penalties'
    
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id'))
    amount = Column(Float)
    reason = Column(String(200))
    date = Column(Date, default=date.today)

class Expense(Base):
    __tablename__ = 'expenses'
    
    id = Column(Integer, primary_key=True)
    category = Column(String(50)) # e.g., 'Food', 'Transport', 'Admin'
    description = Column(String(200))
    amount = Column(Float)
    date = Column(Date, default=date.today)

class Dividend(Base):
    __tablename__ = 'dividends'
    
    id = Column(Integer, primary_key=True)
    member_id = Column(Integer, ForeignKey('members.id'))
    amount = Column(Float)
    cycle_year = Column(String(10)) # Financial year, e.g., '2023-2024'
    shares = Column(Integer) # Number of shares member holds
    rate_per_share = Column(Float) # Dividend rate per share

# Create tables in the database if they don't already exist.
# This is called at the application startup.
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# --- Helper Functions ---
# These functions provide reusable logic for various parts of the application.

def get_financial_year(date_obj=None):
    """
    Calculate financial year based on a March-February cycle.
    If no date_obj is provided, it uses the current date.
    """
    if date_obj is None:
        date_obj = datetime.now().date()
    
    if date_obj.month >= 3: # March to December
        return f"{date_obj.year}-{date_obj.year + 1}"
    else: # January to February
        return f"{date_obj.year - 1}-{date_obj.year}"

def get_next_third_sunday():
    """
    Calculates and returns the date of the next third Sunday of the month.
    This is useful for suggesting future meeting dates.
    """
    today = datetime.now().date()
    year, month = today.year, today.month
    
    # Find the first Sunday of the current month
    first_day = date(year, month, 1)
    # Calculate days to add to get to the first Sunday (Sunday is 6 in weekday())
    days_until_sunday = (6 - first_day.weekday()) % 7
    first_sunday = first_day + timedelta(days=days_until_sunday)
    
    # The third Sunday is 14 days after the first Sunday
    third_sunday = first_sunday + timedelta(days=14)
    
    # If the calculated third Sunday has already passed, find the third Sunday of the next month
    if third_sunday <= today:
        if month == 12: # If current month is December, move to January of next year
            year += 1
            month = 1
        else: # Otherwise, just move to the next month
            month += 1
        
        first_day = date(year, month, 1)
        days_until_sunday = (6 - first_day.weekday()) % 7
        first_sunday = first_day + timedelta(days=days_until_sunday)
        third_sunday = first_sunday + timedelta(days=14)
    
    return third_sunday

def calculate_loan_balance(loan_id):
    """
    Calculates the remaining loan balance for a given loan_id,
    including interest based on the loan type.
    """
    session = Session()
    try:
        loan = session.query(Loan).filter(Loan.id == loan_id).first()
        if not loan:
            return 0
        
        repayments = session.query(Repayment).filter(Repayment.loan_id == loan_id).all()
        total_repaid = sum([r.amount for r in repayments])
        
        principal_remaining = loan.amount - total_repaid
        interest = 0

        # This part assumes interest is calculated on the original loan amount for both types
        # and added to the total amount due. For a more sophisticated reducing balance
        # interest, this logic would need to be expanded.
        if loan.type == 'emergency':
            # Emergency loans have a 2% monthly interest on the original amount (simplified)
            months_elapsed = (datetime.now().date() - loan.start_date).days // 30
            interest = loan.amount * (loan.interest_rate / 100) * months_elapsed
        else: # Normal loan
            # Normal loans have a fixed total interest (e.g., 10%) on the original amount
            interest = loan.amount * (loan.interest_rate / 100)
            
        # The remaining balance is the original loan amount plus total interest, minus total repaid.
        return max(0, (loan.amount + interest) - total_repaid)
    finally:
        session.close()

def generate_receipt_pdf(member_name, contributions_data):
    """
    Generates a PDF receipt for selected contributions of a member.
    Takes member's name and a DataFrame of selected contributions.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, 
                           rightMargin=72, leftMargin=72, 
                           topMargin=72, bottomMargin=18)
    
    styles = getSampleStyleSheet()
    story = []
    
    # Custom styles for the PDF
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=colors.darkblue,
        alignment=1, # Center alignment
        spaceAfter=30 # Space after the title
    )
    
    # Header of the receipt
    story.append(Paragraph("SHALOM BLESSING SELF-HELP GROUP", title_style))
    story.append(Paragraph("OFFICIAL RECEIPT", styles['Heading2']))
    story.append(Spacer(1, 12))
    
    # Receipt details (Receipt No, Member Name, Date Issued)
    receipt_no = f"SB-{datetime.now().strftime('%Y%m%d%H%M%S')}" # Unique receipt number
    
    receipt_info = [
        ['Receipt No:', receipt_no],
        ['Member Name:', member_name],
        ['Date Issued:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        ['', ''] # Empty row for spacing
    ]
    
    info_table = Table(receipt_info, colWidths=[2*inch, 3*inch])
    info_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    
    story.append(info_table)
    story.append(Spacer(1, 20))
    
    # Contributions table details
    story.append(Paragraph("CONTRIBUTION DETAILS", styles['Heading3']))
    
    table_data = [['Date', 'Vote Head', 'Amount (KSh)']] # Table headers
    total_amount = 0
    
    # Populate table with contribution data
    for _, row in contributions_data.iterrows():
        table_data.append([
            row['date'].strftime('%Y-%m-%d'),
            row['votehead'].title(), # Capitalize votehead (e.g., Shares, Welfare)
            f'{row["amount"]:,.2f}' # Format amount as currency
        ])
        total_amount += row['amount']
    
    # Add a total row at the end of the table
    table_data.append(['', 'TOTAL', f'{total_amount:,.2f}'])
    
    contributions_table = Table(table_data, colWidths=[1.5*inch, 2*inch, 1.5*inch])
    contributions_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey), # Header row background
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke), # Header text color
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'), # Center align all text
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), # Bold font for header
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -2), colors.beige), # Background for data rows
        ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey), # Background for total row
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'), # Bold font for total row
        ('GRID', (0, 0), (-1, -1), 1, colors.black) # Grid lines
    ]))
    
    story.append(contributions_table)
    story.append(Spacer(1, 30))
    
    # Footer text for the receipt
    footer_text = """
    Thank you for your continued participation in our self-help group.
    This receipt serves as proof of your contributions.
    
    For any queries, please contact the group secretary.
    """
    
    story.append(Paragraph(footer_text, styles['Normal']))
    story.append(Spacer(1, 20))
    
    # Signature line
    story.append(Paragraph("_" * 30 + "         " + "_" * 30, styles['Normal']))
    story.append(Paragraph("Secretary Signature              Date", styles['Normal']))
    
    doc.build(story) # Build the PDF document
    buffer.seek(0) # Reset buffer position to the beginning
    return buffer

def generate_member_statement_pdf(member_name, contribution_history):
    """
    Generates a comprehensive member statement PDF, including summary
    and detailed contribution history.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []
    
    # Header styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.darkblue,
        alignment=1
    )
    
    story.append(Paragraph("SHALOM BLESSING SELF-HELP GROUP", title_style))
    story.append(Paragraph(f"MEMBER STATEMENT - {member_name}", styles['Heading2']))
    story.append(Spacer(1, 12))
    
    # Statement data
    if not contribution_history.empty:
        # Calculate total shares and welfare from history
        total_shares = contribution_history[contribution_history['votehead'] == 'shares']['amount'].sum()
        total_welfare = contribution_history[contribution_history['votehead'] == 'welfare']['amount'].sum()
        
        # Summary table
        summary_data = [
            ['Total Shares:', f'KSh {total_shares:,.2f}'],
            ['Total Welfare:', f'KSh {total_welfare:,.2f}'],
            ['Number of Shares:', f'{int(total_shares/1000)}'], # Assuming 1 share = KSh 1000
            ['Statement Date:', datetime.now().strftime('%Y-%m-%d')]
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('BACKGROUND', (0, 0), (-1, -1), colors.lightgrey),
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 20))
        
        # Contribution history table
        story.append(Paragraph("CONTRIBUTION HISTORY", styles['Heading3']))
        
        table_data = [['Date', 'Vote Head', 'Amount (KSh)']]
        for _, row in contribution_history.iterrows():
            table_data.append([
                row['date'].strftime('%Y-%m-%d'),
                row['votehead'].title(),
                f'KSh {row["amount"]:,.2f}'
            ])
        
        history_table = Table(table_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch])
        history_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(history_table)
    
    doc.build(story)
    buffer.seek(0)
    return buffer

def generate_financial_report_pdf():
    """
    Generates a comprehensive financial report PDF, summarizing
    income, expenses, and overall financial position.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []
    
    session = Session()
    try:
        # Header
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.darkblue,
            alignment=1
        )
        
        story.append(Paragraph("SHALOM BLESSING SELF-HELP GROUP", title_style))
        story.append(Paragraph("FINANCIAL REPORT", styles['Heading2']))
        story.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Financial Summary
        total_shares = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = 'shares'")).scalar()
        total_welfare = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = 'welfare'")).scalar()
        total_loans = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM loans")).scalar()
        total_expenses = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM expenses")).scalar()
        total_penalties = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM penalties")).scalar()
        
        summary_data = [
            ['INCOME', ''],
            ['Total Shares Collected', f'KSh {total_shares:,.2f}'],
            ['Total Welfare Collected', f'KSh {total_welfare:,.2f}'],
            ['Total Penalties', f'KSh {total_penalties:,.2f}'],
            ['TOTAL INCOME', f'KSh {total_shares + total_welfare + total_penalties:,.2f}'],
            ['', ''],
            ['LOANS & EXPENSES', ''],
            ['Total Loans Issued', f'KSh {total_loans:,.2f}'],
            ['Total Expenses', f'KSh {total_expenses:,.2f}'],
            ['', ''],
            ['NET POSITION', f'KSh {total_shares + total_welfare + total_penalties - total_expenses:,.2f}']
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('BACKGROUND', (0, 0), (0, 0), colors.grey),
            ('BACKGROUND', (0, 6), (0, 6), colors.grey),
            ('BACKGROUND', (0, 10), (-1, 10), colors.lightgrey),
            ('FONTNAME', (0, 10), (-1, 10), 'Helvetica-Bold'),
        ]))
        
        story.append(summary_table)
        
    finally:
        session.close()
    
    doc.build(story)
    buffer.seek(0)
    return buffer

# --- Streamlit UI Functions ---
# These functions define the interactive user interface for each section of the application.

def show_dashboard():
    """Displays key financial metrics, charts, and recent activity."""
    st.header("📊 Dashboard")
    
    session = Session()
    try:
        # KPIs (Key Performance Indicators)
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_shares = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = 'shares'")).scalar()
            st.metric("Total Shares", f"KSh {total_shares:,.2f}")
        
        with col2:
            total_welfare = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE votehead = 'welfare'")).scalar()
            st.metric("Total Welfare", f"KSh {total_welfare:,.2f}")
        
        with col3:
            total_loans = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM loans WHERE status = 'active'")).scalar()
            st.metric("Active Loans", f"KSh {total_loans:,.2f}")
        
        with col4:
            total_dividends = session.execute(text("SELECT COALESCE(SUM(amount), 0) FROM dividends")).scalar()
            st.metric("Dividends Paid", f"KSh {total_dividends:,.2f}")
        
        st.markdown("---")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Monthly Contributions Trend")
            contributions_data = pd.read_sql("""
                SELECT 
                    DATE_TRUNC('month', date) as month,
                    votehead,
                    SUM(amount) as total
                FROM contributions 
                GROUP BY DATE_TRUNC('month', date), votehead
                ORDER BY month
            """, engine)
            
            if not contributions_data.empty:
                fig = px.line(contributions_data, x='month', y='total', color='votehead',
                            title="Monthly Contributions by Vote Head")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No contribution data available yet.")
        
        with col2:
            st.subheader("Vote Head Distribution")
            votehead_data = pd.read_sql("""
                SELECT votehead, SUM(amount) as total
                FROM contributions 
                GROUP BY votehead
            """, engine)
            
            if not votehead_data.empty:
                fig = px.pie(votehead_data, values='total', names='votehead',
                           title="Total Contributions by Vote Head")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No contribution data available yet.")
        
        # Recent Activity
        st.subheader("Recent Activity")
        recent_contributions = pd.read_sql("""
            SELECT c.date, m.name, c.votehead, c.amount
            FROM contributions c
            JOIN members m ON c.member_id = m.id
            ORDER BY c.date DESC
            LIMIT 10
        """, engine)
        
        if not recent_contributions.empty:
            st.dataframe(recent_contributions, use_container_width=True)
        else:
            st.info("No recent activity to display.")
            
    finally:
        session.close()

def show_members():
    """Manages member profiles, allows adding new members, and viewing financial history."""
    st.header("👥 Members Management")
    
    tab1, tab2 = st.tabs(["View Members", "Add Member"])
    
    session = Session()
    try:
        with tab1:
            # Filter options
            col1, col2 = st.columns(2)
            with col1:
                status_filter = st.selectbox("Filter by Status", ["All", "Active", "Inactive"])
            
            # Get members data
            if status_filter == "All":
                members_query = "SELECT * FROM members ORDER BY name"
            else:
                members_query = f"SELECT * FROM members WHERE status = '{status_filter.lower()}' ORDER BY name"
            
            members_df = pd.read_sql(members_query, engine)
            
            if not members_df.empty:
                # Display each member in an expander with summary and history button
                for idx, row in members_df.iterrows():
                    member_id = row['id']
                    
                    # Get contributions summary for each member
                    shares = session.execute(text("""
                        SELECT COALESCE(SUM(amount), 0) 
                        FROM contributions 
                        WHERE member_id = :member_id AND votehead = 'shares'
                    """), {"member_id": member_id}).scalar()
                    
                    welfare = session.execute(text("""
                        SELECT COALESCE(SUM(amount), 0) 
                        FROM contributions 
                        WHERE member_id = :member_id AND votehead = 'welfare'
                    """), {"member_id": member_id}).scalar()
                    
                    # Display member card
                    with st.expander(f"{row['name']} - {row['phone']}"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Shares", f"KSh {shares:,.2f}")
                        with col2:
                            st.metric("Welfare", f"KSh {welfare:,.2f}")
                        with col3:
                            st.metric("Join Date", row['join_date'].strftime('%Y-%m-%d'))
                        
                        # Member actions: Button to view detailed history (can be replaced by a dedicated function call)
                        if st.button(f"View History - {row['name']}", key=f"history_{row['id']}"):
                            show_member_history(member_id, row['name'])
            else:
                st.info("No members found.")
        
        with tab2:
            st.subheader("Add New Member")
            with st.form("add_member_form"):
                name = st.text_input("Full Name*")
                phone = st.text_input("Phone Number")
                join_date = st.date_input("Join Date", value=date.today())
                
                if st.form_submit_button("Add Member"):
                    if name:
                        new_member = Member(name=name, phone=phone, join_date=join_date)
                        session.add(new_member)
                        session.commit()
                        st.success(f"Member {name} added successfully!")
                        st.rerun() # Rerun to refresh the 'View Members' tab
                    else:
                        st.error("Please provide a name.")
    finally:
        session.close()

def show_member_history(member_id, member_name):
    """Displays detailed financial history for a selected member."""
    st.subheader(f"Financial History - {member_name}")
    
    # Contributions history
    contributions_df = pd.read_sql("""
        SELECT c.date, c.votehead, c.amount, m.date as meeting_date
        FROM contributions c
        LEFT JOIN meetings m ON c.meeting_id = m.id
        WHERE c.member_id = %s
        ORDER BY c.date DESC
    """, engine, params=[member_id])
    
    if not contributions_df.empty:
        st.dataframe(contributions_df, use_container_width=True)
    else:
        st.info("No contribution history found for this member.")

def show_meetings():
    """Manages meeting creation, viewing, and attendance."""
    st.header("📅 Meetings Management")
    
    tab1, tab2 = st.tabs(["View Meetings", "Create Meeting"])
    
    session = Session()
    try:
        with tab1:
            meetings_df = pd.read_sql("""
                SELECT id, date, notes, financial_year
                FROM meetings
                ORDER BY date DESC
            """, engine)
            
            if not meetings_df.empty:
                for _, meeting in meetings_df.iterrows():
                    with st.expander(f"Meeting - {meeting['date']} ({meeting['financial_year']})"):
                        st.write(f"**Notes:** {meeting['notes'] or 'No notes'}")
                        
                        # Attendance summary for the meeting
                        attendance_df = pd.read_sql("""
                            SELECT m.name, a.present
                            FROM attendance a
                            JOIN members m ON a.member_id = m.id
                            WHERE a.meeting_id = %s
                        """, engine, params=[meeting['id']])
                        
                        if not attendance_df.empty:
                            present_count = attendance_df['present'].sum()
                            total_count = len(attendance_df)
                            st.write(f"**Attendance:** {present_count}/{total_count} members present")
                        
                        # Contributions recorded for this meeting
                        contributions_df = pd.read_sql("""
                            SELECT m.name, c.votehead, c.amount
                            FROM contributions c
                            JOIN members m ON c.member_id = m.id
                            WHERE c.meeting_id = %s
                        """, engine, params=[meeting['id']])
                        
                        if not contributions_df.empty:
                            st.dataframe(contributions_df, use_container_width=True)
            else:
                st.info("No meetings recorded yet.")
        
        with tab2:
            st.subheader("Create New Meeting")
            
            # Suggest next third Sunday as default meeting date
            next_meeting = get_next_third_sunday()
            st.info(f"Suggested next meeting date: {next_meeting}")
            
            with st.form("create_meeting_form"):
                meeting_date = st.date_input("Meeting Date", value=next_meeting)
                notes = st.text_area("Meeting Notes")
                
                if st.form_submit_button("Create Meeting"):
                    financial_year = get_financial_year(meeting_date)
                    
                    new_meeting = Meeting(
                        date=meeting_date,
                        notes=notes,
                        financial_year=financial_year
                    )
                    session.add(new_meeting)
                    session.commit()
                    
                    # Create attendance records for all active members for this new meeting
                    members = session.query(Member).filter(Member.status == 'active').all()
                    for member in members:
                        attendance = Attendance(
                            meeting_id=new_meeting.id,
                            member_id=member.id,
                            present=False # Default to absent, can be updated later
                        )
                        session.add(attendance)
                    
                    session.commit()
                    st.success("Meeting created successfully!")
                    st.rerun() # Rerun to refresh the 'View Meetings' tab
    finally:
        session.close()

def show_contributions():
    """Handles recording of new contributions and viewing contribution history."""
    st.header("💰 Contributions Management")
    
    tab1, tab2 = st.tabs(["View Contributions", "Record Contribution"])
    
    session = Session()
    try:
        with tab1:
            st.subheader("All Contributions")
            contributions_df = pd.read_sql("""
                SELECT c.date, m.name, c.votehead, c.amount, meet.date as meeting_date
                FROM contributions c
                JOIN members m ON c.member_id = m.id
                LEFT JOIN meetings meet ON c.meeting_id = meet.id
                ORDER BY c.date DESC
            """, engine)
            
            if not contributions_df.empty:
                st.dataframe(contributions_df, use_container_width=True)
                
                # Summary metrics for contributions
                total_shares = contributions_df[contributions_df['votehead'] == 'shares']['amount'].sum()
                total_welfare = contributions_df[contributions_df['votehead'] == 'welfare']['amount'].sum()
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Shares Collected", f"KSh {total_shares:,.2f}")
                with col2:
                    st.metric("Total Welfare Collected", f"KSh {total_welfare:,.2f}")
            else:
                st.info("No contributions recorded yet.")
        
        with tab2:
            st.subheader("Record New Contribution")
            
            # Get active meetings for selection
            meetings_df = pd.read_sql("""
                SELECT id, date FROM meetings 
                ORDER BY date DESC LIMIT 10
            """, engine) # Limit to recent 10 meetings for practicality
            
            # Get active members for selection
            members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active' ORDER BY name", engine)
            
            if not members_df.empty and not meetings_df.empty:
                with st.form("contribution_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        selected_meeting = st.selectbox("Select Meeting",
                                                      options=meetings_df['id'].tolist(),
                                                      format_func=lambda x: meetings_df[meetings_df['id'] == x]['date'].iloc[0].strftime('%Y-%m-%d'))
                        
                        selected_member = st.selectbox("Select Member",
                                                     options=members_df['id'].tolist(),
                                                     format_func=lambda x: members_df[members_df['id'] == x]['name'].iloc[0])
                    
                    with col2:
                        votehead = st.selectbox("Vote Head", ["shares", "welfare"])
                        # Default amount based on votehead
                        amount = st.number_input("Amount (KSh)", 
                                               min_value=0.0, 
                                               step=100.0,
                                               value=1000.0 if votehead == "shares" else 300.0)
                    
                    contribution_date = st.date_input("Contribution Date", value=date.today())
                    
                    if st.form_submit_button("Record Contribution"):
                        if amount > 0:
                            new_contribution = Contribution(
                                member_id=selected_member,
                                meeting_id=selected_meeting,
                                votehead=votehead,
                                amount=amount,
                                date=contribution_date
                            )
                            session.add(new_contribution)
                            session.commit()
                            st.success("Contribution recorded successfully!")
                            st.rerun() # Rerun to refresh the 'View Contributions' tab
                        else:
                            st.error("Please provide a valid amount.")
            else:
                st.warning("Please ensure you have active members and meetings before recording contributions.")
    finally:
        session.close()

def show_loans():
    """Manages loan issuance, viewing, and repayments."""
    st.header("💰 Loans Management")
    
    tab1, tab2 = st.tabs(["View Loans", "Issue Loan"])
    
    session = Session()
    try:
        with tab1:
            loans_df = pd.read_sql("""
                SELECT l.id, m.name, l.type, l.amount, l.interest_rate, 
                       l.start_date, l.due_date, l.status
                FROM loans l
                JOIN members m ON l.member_id = m.id
                ORDER BY l.start_date DESC
            """, engine)
            
            if not loans_df.empty:
                for _, loan in loans_df.iterrows():
                    balance = calculate_loan_balance(loan['id'])
                    
                    with st.expander(f"{loan['name']} - {loan['type'].title()} Loan (KSh {loan['amount']:,.2f})"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Principal", f"KSh {loan['amount']:,.2f}")
                        with col2:
                            st.metric("Interest Rate", f"{loan['interest_rate']}%")
                        with col3:
                            st.metric("Balance", f"KSh {balance:,.2f}")
                        
                        st.write(f"**Start Date:** {loan['start_date']}")
                        st.write(f"**Due Date:** {loan['due_date']}")
                        st.write(f"**Status:** {loan['status'].title()}")
                        
                        # Repayment history for this loan
                        repayments_df = pd.read_sql("""
                            SELECT amount, date FROM repayments 
                            WHERE loan_id = %s ORDER BY date DESC
                        """, engine, params=[loan['id']])
                        
                        if not repayments_df.empty:
                            st.subheader("Repayment History")
                            st.dataframe(repayments_df, use_container_width=True)
                        
                        # Add repayment form for this specific loan
                        with st.form(f"repayment_form_{loan['id']}"):
                            repayment_amount = st.number_input("Repayment Amount", min_value=0.0, step=100.0, key=f"repay_amount_{loan['id']}")
                            repayment_date = st.date_input("Repayment Date", value=date.today(), key=f"repay_date_{loan['id']}")
                            
                            if st.form_submit_button("Record Repayment", key=f"record_repayment_{loan['id']}"):
                                if repayment_amount > 0:
                                    new_repayment = Repayment(
                                        loan_id=loan['id'],
                                        amount=repayment_amount,
                                        date=repayment_date
                                    )
                                    session.add(new_repayment)
                                    
                                    # Check if loan is fully paid after this repayment
                                    new_balance = calculate_loan_balance(loan['id'])
                                    if new_balance <= 0:
                                        loan_obj = session.query(Loan).filter(Loan.id == loan['id']).first()
                                        loan_obj.status = 'paid'
                                    
                                    session.commit()
                                    st.success("Repayment recorded successfully!")
                                    st.rerun() # Rerun to update loan balance and status
                                else:
                                    st.error("Please enter a valid repayment amount.")
            else:
                st.info("No loans issued yet.")
        
        with tab2:
            st.subheader("Issue New Loan")
            
            members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active'", engine)
            
            if not members_df.empty:
                with st.form("issue_loan_form"):
                    member_id = st.selectbox("Select Member", 
                                           options=members_df['id'].tolist(),
                                           format_func=lambda x: members_df[members_df['id'] == x]['name'].iloc[0])
                    
                    loan_type = st.selectbox("Loan Type", ["normal", "emergency"])
                    amount = st.number_input("Loan Amount (KSh)", min_value=1000.0, step=1000.0)
                    start_date = st.date_input("Start Date", value=date.today())
                    
                    # Auto-calculate interest rate and due date based on loan type
                    if loan_type == "normal":
                        interest_rate = 10.0 # Example: 10% total interest
                        due_date = start_date + timedelta(days=24*30)  # Example: 24 months repayment period
                        st.info("Normal Loan: 10% total interest, ~24 months to repay")
                    else: # emergency
                        interest_rate = 2.0 # Example: 2% monthly interest
                        due_date = start_date + timedelta(days=30)  # Example: 1 month repayment period
                        st.info("Emergency Loan: 2% monthly interest, ~1 month to repay")
                    
                    st.write(f"**Calculated Due Date:** {due_date.strftime('%Y-%m-%d')}")
                    
                    if st.form_submit_button("Issue Loan"):
                        if amount > 0:
                            new_loan = Loan(
                                member_id=member_id,
                                type=loan_type,
                                amount=amount,
                                interest_rate=interest_rate,
                                start_date=start_date,
                                due_date=due_date
                            )
                            session.add(new_loan)
                            session.commit()
                            st.success("Loan issued successfully!")
                            st.rerun() # Rerun to refresh the 'View Loans' tab
                        else:
                            st.error("Please provide a valid loan amount.")
            else:
                st.info("No active members available for loans.")
    finally:
        session.close()

def show_penalties():
    """Manages penalties, allowing viewing existing penalties and adding new ones."""
    st.header("⚖️ Penalties Management")
    
    tab1, tab2 = st.tabs(["View Penalties", "Add Penalty"])
    
    session = Session()
    try:
        with tab1:
            penalties_df = pd.read_sql("""
                SELECT p.date, m.name, p.amount, p.reason
                FROM penalties p
                JOIN members m ON p.member_id = m.id
                ORDER BY p.date DESC
            """, engine)
            
            if not penalties_df.empty:
                st.dataframe(penalties_df, use_container_width=True)
                
                total_penalties = penalties_df['amount'].sum()
                st.metric("Total Penalties Collected", f"KSh {total_penalties:,.2f}")
            else:
                st.info("No penalties recorded yet.")
        
        with tab2:
            st.subheader("Add New Penalty")
            
            members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active' ORDER BY name", engine)
            
            if not members_df.empty:
                with st.form("add_penalty_form"):
                    member_id = st.selectbox("Select Member",
                                           options=members_df['id'].tolist(),
                                           format_func=lambda x: members_df[members_df['id'] == x]['name'].iloc[0])
                    
                    amount = st.number_input("Penalty Amount (KSh)", min_value=0.0, step=50.0)
                    reason = st.text_input("Reason for Penalty*")
                    penalty_date = st.date_input("Penalty Date", value=date.today())
                    
                    if st.form_submit_button("Add Penalty"):
                        if amount > 0 and reason:
                            new_penalty = Penalty(
                                member_id=member_id,
                                amount=amount,
                                reason=reason,
                                date=penalty_date
                            )
                            session.add(new_penalty)
                            session.commit()
                            st.success("Penalty added successfully!")
                            st.rerun() # Rerun to refresh the 'View Penalties' tab
                        else:
                            st.error("Please provide amount and reason.")
            else:
                st.info("No active members available.")
    finally:
        session.close()

def show_expenses():
    """Manages group expenses, allowing viewing and adding new expense records."""
    st.header("💸 Expenses Management")
    
    tab1, tab2 = st.tabs(["View Expenses", "Add Expense"])
    
    session = Session()
    try:
        with tab1:
            expenses_df = pd.read_sql("""
                SELECT date, category, description, amount
                FROM expenses
                ORDER BY date DESC
            """, engine)
            
            if not expenses_df.empty:
                # Filter by category for easier viewing
                categories = expenses_df['category'].unique().tolist()
                selected_category = st.selectbox("Filter by Category", ["All"] + categories)
                
                if selected_category != "All":
                    filtered_df = expenses_df[expenses_df['category'] == selected_category]
                else:
                    filtered_df = expenses_df
                
                st.dataframe(filtered_df, use_container_width=True)
                
                # Summary metrics for filtered expenses
                col1, col2 = st.columns(2)
                with col1:
                    total_expenses = filtered_df['amount'].sum()
                    st.metric("Total Expenses (Filtered)", f"KSh {total_expenses:,.2f}")
                
                with col2:
                    # Calculate expenses for the current month
                    current_month_expenses = filtered_df[
                        pd.to_datetime(filtered_df['date']).dt.month == datetime.now().month
                    ]['amount'].sum()
                    st.metric("This Month's Expenses", f"KSh {current_month_expenses:,.2f}")
                
                # Expenses by category chart for overall expenses
                if not expenses_df.empty:
                    category_summary = expenses_df.groupby('category')['amount'].sum().reset_index()
                    fig = px.pie(category_summary, values='amount', names='category',
                               title="Overall Expenses by Category")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No expenses recorded yet.")
        
        with tab2:
            st.subheader("Add New Expense")
            
            with st.form("add_expense_form"):
                category = st.selectbox("Category", 
                                      ["Food & Refreshments", "Welfare Support", "Event Support", 
                                       "Stationery", "Transport", "Other"])
                description = st.text_input("Description*")
                amount = st.number_input("Amount (KSh)", min_value=0.0, step=50.0)
                expense_date = st.date_input("Expense Date", value=date.today())
                
                if st.form_submit_button("Add Expense"):
                    if description and amount > 0:
                        new_expense = Expense(
                            category=category,
                            description=description,
                            amount=amount,
                            date=expense_date
                        )
                        session.add(new_expense)
                        session.commit()
                        st.success("Expense added successfully!")
                        st.rerun() # Rerun to refresh the 'View Expenses' tab
                    else:
                        st.error("Please provide description and amount.")
    finally:
        session.close()

def show_dividends():
    """Handles dividend calculation, distribution, and viewing history."""
    st.header("💎 Dividends Management")
    
    tab1, tab2 = st.tabs(["View Dividends", "Calculate & Distribute"])
    
    session = Session()
    try:
        with tab1:
            dividends_df = pd.read_sql("""
                SELECT d.cycle_year, m.name, d.shares, d.rate_per_share, d.amount
                FROM dividends d
                JOIN members m ON d.member_id = m.id
                ORDER BY d.cycle_year DESC, m.name
            """, engine)
            
            if not dividends_df.empty:
                # Allow filtering by financial year
                years = dividends_df['cycle_year'].unique()
                selected_year = st.selectbox("Select Financial Year", years)
                
                year_data = dividends_df[dividends_df['cycle_year'] == selected_year]
                st.dataframe(year_data, use_container_width=True)
                
                total_distributed = year_data['amount'].sum()
                st.metric(f"Total Distributed ({selected_year})", f"KSh {total_distributed:,.2f}")
            else:
                st.info("No dividends distributed yet.")
        
        with tab2:
            st.subheader("Calculate & Distribute Dividends")
            
            # Get current financial year for context
            current_fy = get_financial_year()
            st.info(f"Current Financial Year: {current_fy}")
            
            # Button to trigger dividend preview calculation
            if st.button("Calculate Dividend Preview"):
                # Calculate total income components
                # Note: This is a simplified calculation for demonstration.
                # Actual interest calculation for dividends might be more complex.
                total_loan_interest = session.execute(text("""
                    SELECT COALESCE(SUM(
                        CASE 
                            WHEN l.type = 'normal' THEN l.amount * (l.interest_rate / 100)
                            ELSE (l.amount * (l.interest_rate / 100) * EXTRACT(MONTH FROM AGE(CURRENT_DATE, l.start_date)))
                        END
                    ), 0)
                    FROM loans l
                    WHERE l.status != 'paid' OR l.due_date >= :start_of_fy
                """), {"start_of_fy": datetime.strptime(current_fy.split('-')[0] + '-03-01', '%Y-%m-%d').date()}).scalar() # Interest from loans active/due in this FY
                
                total_penalties = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM penalties 
                    WHERE date >= :start_of_fy
                """), {"start_of_fy": datetime.strptime(current_fy.split('-')[0] + '-03-01', '%Y-%m-%d').date()}).scalar() # Penalties collected in this FY
                
                total_expenses = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM expenses 
                    WHERE date >= :start_of_fy
                """), {"start_of_fy": datetime.strptime(current_fy.split('-')[0] + '-03-01', '%Y-%m-%d').date()}).scalar() # Expenses in this FY
                
                # Total shares as basis for dividend calculation (assuming 1 share = KSh 1000)
                total_shares_amount = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM contributions 
                    WHERE votehead = 'shares'
                """)).scalar()
                total_shares_count = total_shares_amount / 1000 if total_shares_amount > 0 else 0
                
                # Calculate dividend pool
                dividend_pool = max(0, total_loan_interest + total_penalties - total_expenses)
                rate_per_share = dividend_pool / total_shares_count if total_shares_count > 0 else 0
                
                # Display calculated metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Loan Interest Income", f"KSh {total_loan_interest:,.2f}")
                    st.metric("Penalty Income", f"KSh {total_penalties:,.2f}")
                
                with col2:
                    st.metric("Total Expenses", f"KSh {total_expenses:,.2f}")
                    st.metric("Total Shares (Units)", f"{total_shares_count:,.0f}")
                
                with col3:
                    st.metric("Dividend Pool (Net Profit)", f"KSh {dividend_pool:,.2f}")
                    st.metric("Rate per Share", f"KSh {rate_per_share:.2f}")
                
                # Show member-wise dividend calculation if there's a pool
                if rate_per_share > 0:
                    st.subheader("Member-wise Dividend Calculation Preview")
                    
                    member_dividends_preview = pd.read_sql("""
                        SELECT 
                            m.id,
                            m.name,
                            COALESCE(SUM(c.amount)/1000, 0) as shares_held
                        FROM members m
                        LEFT JOIN contributions c ON m.id = c.member_id AND c.votehead = 'shares'
                        WHERE m.status = 'active'
                        GROUP BY m.id, m.name
                        ORDER BY m.name
                    """, engine)
                    
                    member_dividends_preview['dividend_amount'] = member_dividends_preview['shares_held'] * rate_per_share
                    
                    st.dataframe(member_dividends_preview, use_container_width=True)
                    
                    # Button to actually distribute dividends (persists to DB)
                    if st.button("Distribute Dividends Now", type="primary"):
                        for _, row in member_dividends_preview.iterrows():
                            if row['dividend_amount'] > 0:
                                dividend = Dividend(
                                    member_id=row['id'],
                                    amount=row['dividend_amount'],
                                    cycle_year=current_fy,
                                    shares=int(row['shares_held']),
                                    rate_per_share=rate_per_share
                                )
                                session.add(dividend)
                        
                        session.commit()
                        st.success(f"Dividends for {current_fy} distributed successfully!")
                        st.rerun() # Rerun to update view
                else:
                    st.warning("No dividend pool available for distribution for the current financial year. Ensure there's sufficient income and active shares.")
    finally:
        session.close()

def show_reports():
    """Provides various financial and member-related reports and data export options."""
    st.header("📊 Reports & Analytics")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Financial Summary", "Member Reports", "Attendance Reports", "Export Data"])
    
    session = Session()
    try:
        with tab1:
            st.subheader("Financial Summary Report")
            
            # Date range selector for financial summary
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", value=date.today() - timedelta(days=365))
            with col2:
                end_date = st.date_input("End Date", value=date.today())
            
            if start_date <= end_date:
                # Fetch summary data within the selected date range
                shares_total = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM contributions 
                    WHERE votehead = 'shares' AND date BETWEEN :start_date AND :end_date
                """), {"start_date": start_date, "end_date": end_date}).scalar()
                
                welfare_total = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM contributions 
                    WHERE votehead = 'welfare' AND date BETWEEN :start_date AND :end_date
                """), {"start_date": start_date, "end_date": end_date}).scalar()
                
                loans_issued = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM loans 
                    WHERE start_date BETWEEN :start_date AND :end_date
                """), {"start_date": start_date, "end_date": end_date}).scalar()
                
                expenses_total = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM expenses 
                    WHERE date BETWEEN :start_date AND :end_date
                """), {"start_date": start_date, "end_date": end_date}).scalar()
                
                penalties_total = session.execute(text("""
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM penalties 
                    WHERE date BETWEEN :start_date AND :end_date
                """), {"start_date": start_date, "end_date": end_date}).scalar()
                
                # Display metrics
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Shares Collected", f"KSh {shares_total:,.2f}")
                with col2:
                    st.metric("Welfare Collected", f"KSh {welfare_total:,.2f}")
                with col3:
                    st.metric("Loans Issued", f"KSh {loans_issued:,.2f}")
                with col4:
                    st.metric("Total Expenses", f"KSh {expenses_total:,.2f}")
                with col5:
                    st.metric("Penalties", f"KSh {penalties_total:,.2f}")
                
                # Monthly trend analysis chart for contributions
                monthly_data = pd.read_sql("""
                    SELECT 
                        DATE_TRUNC('month', date) as month,
                        'Shares' as category,
                        SUM(amount) as amount
                    FROM contributions 
                    WHERE votehead = 'shares' AND date BETWEEN %s AND %s
                    GROUP BY DATE_TRUNC('month', date)
                    
                    UNION ALL
                    
                    SELECT 
                        DATE_TRUNC('month', date) as month,
                        'Welfare' as category,
                        SUM(amount) as amount
                    FROM contributions 
                    WHERE votehead = 'welfare' AND date BETWEEN %s AND %s
                    GROUP BY DATE_TRUNC('month', date)
                    
                    ORDER BY month
                """, engine, params=[start_date, end_date, start_date, end_date])
                
                if not monthly_data.empty:
                    fig = px.line(monthly_data, x='month', y='amount', color='category',
                                title="Monthly Contributions Trend")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No contribution data in the selected range to display trends.")
            else:
                st.error("Start date cannot be after end date.")
        
        with tab2:
            st.subheader("Member Financial Reports")
            
            # Member selector for individual reports
            members_df = pd.read_sql("SELECT id, name FROM members ORDER BY name", engine)
            if not members_df.empty:
                selected_member = st.selectbox("Select Member",
                                             options=members_df['id'].tolist(),
                                             format_func=lambda x: members_df[members_df['id'] == x]['name'].iloc[0])
                
                if selected_member:
                    member_name = members_df[members_df['id'] == selected_member]['name'].iloc[0]
                    st.subheader(f"Financial Report - {member_name}")
                    
                    # Member's financial summary
                    member_shares = session.execute(text("""
                        SELECT COALESCE(SUM(amount), 0) 
                        FROM contributions 
                        WHERE member_id = :member_id AND votehead = 'shares'
                    """), {"member_id": selected_member}).scalar()
                    
                    member_welfare = session.execute(text("""
                        SELECT COALESCE(SUM(amount), 0) 
                        FROM contributions 
                        WHERE member_id = :member_id AND votehead = 'welfare'
                    """), {"member_id": selected_member}).scalar()
                    
                    member_loans = session.execute(text("""
                        SELECT COALESCE(SUM(amount), 0) 
                        FROM loans 
                        WHERE member_id = :member_id AND status = 'active'
                    """), {"member_id": selected_member}).scalar()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Shares", f"KSh {member_shares:,.2f}")
                        st.metric("Number of Shares", f"{int(member_shares/1000)}")
                    with col2:
                        st.metric("Total Welfare", f"KSh {member_welfare:,.2f}")
                    with col3:
                        st.metric("Active Loans", f"KSh {member_loans:,.2f}")
                    
                    # Contribution history for the selected member
                    contribution_history = pd.read_sql("""
                        SELECT c.date, c.votehead, c.amount, m.date as meeting_date
                        FROM contributions c
                        LEFT JOIN meetings m ON c.meeting_id = m.id
                        WHERE c.member_id = %s
                        ORDER BY c.date DESC
                    """, engine, params=[selected_member])
                    
                    if not contribution_history.empty:
                        st.subheader("Contribution History")
                        st.dataframe(contribution_history, use_container_width=True)
                        
                        # Button to generate PDF statement for the member
                        if st.button(f"Generate Statement for {member_name}"):
                            pdf_buffer = generate_member_statement_pdf(member_name, contribution_history)
                            st.download_button(
                                label="Download Member Statement PDF",
                                data=pdf_buffer,
                                file_name=f"member_statement_{member_name.replace(' ', '_')}.pdf",
                                mime="application/pdf"
                            )
                    else:
                        st.info("No contribution history found for this member.")
            else:
                st.info("No members available to generate reports.")
        
        with tab3:
            st.subheader("Attendance Reports")
            
            # Meeting attendance summary
            attendance_summary = pd.read_sql("""
                SELECT 
                    m.date,
                    COUNT(a.id) as total_members,
                    SUM(CASE WHEN a.present THEN 1 ELSE 0 END) as present_count,
                    ROUND(
                        100.0 * SUM(CASE WHEN a.present THEN 1 ELSE 0 END) / COUNT(a.id), 
                        2
                    ) as attendance_percentage
                FROM meetings m
                LEFT JOIN attendance a ON m.id = a.meeting_id
                GROUP BY m.id, m.date
                ORDER BY m.date DESC
            """, engine)
            
            if not attendance_summary.empty:
                st.dataframe(attendance_summary, use_container_width=True)
                
                # Attendance trend chart
                fig = px.line(attendance_summary, x='date', y='attendance_percentage',
                            title="Overall Meeting Attendance Percentage Trend")
                fig.update_yaxis(range=[0, 100]) # Ensure y-axis from 0 to 100
                st.plotly_chart(fig, use_container_width=True)
                
                # Individual member attendance summary
                member_attendance = pd.read_sql("""
                    SELECT 
                        m.name,
                        COUNT(a.id) as total_meetings_recorded,
                        SUM(CASE WHEN a.present THEN 1 ELSE 0 END) as meetings_attended,
                        ROUND(
                            100.0 * SUM(CASE WHEN a.present THEN 1 ELSE 0 END) / COUNT(a.id), 
                            2
                        ) as attendance_rate
                    FROM members m
                    LEFT JOIN attendance a ON m.id = a.member_id
                    WHERE m.status = 'active'
                    GROUP BY m.id, m.name
                    ORDER BY attendance_rate DESC
                """, engine)
                
                if not member_attendance.empty:
                    st.subheader("Individual Member Attendance Rates")
                    st.dataframe(member_attendance, use_container_width=True)
                else:
                    st.info("No member attendance records found.")
            else:
                st.info("No meeting attendance data available yet.")
        
        with tab4:
            st.subheader("Export Data")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Export Members Data**")
                if st.button("Export All Members to CSV"):
                    members_export = pd.read_sql("""
                        SELECT 
                            m.name,
                            m.phone,
                            m.status,
                            m.join_date,
                            COALESCE(SUM(CASE WHEN c.votehead = 'shares' THEN c.amount ELSE 0 END), 0) as total_shares,
                            COALESCE(SUM(CASE WHEN c.votehead = 'welfare' THEN c.amount ELSE 0 END), 0) as total_welfare
                        FROM members m
                        LEFT JOIN contributions c ON m.id = c.member_id
                        GROUP BY m.id, m.name, m.phone, m.status, m.join_date
                        ORDER BY m.name
                    """, engine)
                    
                    csv = members_export.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Members CSV",
                        data=csv,
                        file_name=f"members_export_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                
                st.write("**Export Contributions Data**")
                if st.button("Export All Contributions to CSV"):
                    contributions_export = pd.read_sql("""
                        SELECT 
                            m.name as member_name,
                            c.date,
                            c.votehead,
                            c.amount,
                            meet.date as meeting_date
                        FROM contributions c
                        JOIN members m ON c.member_id = m.id
                        LEFT JOIN meetings meet ON c.meeting_id = meet.id
                        ORDER BY c.date DESC
                    """, engine)
                    
                    csv = contributions_export.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Contributions CSV",
                        data=csv,
                        file_name=f"contributions_export_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
            
            with col2:
                st.write("**Export Loans & Repayments Data**")
                if st.button("Export Loans to CSV"):
                    loans_export = pd.read_sql("""
                        SELECT 
                            m.name as member_name,
                            l.type,
                            l.amount as loan_amount,
                            l.interest_rate,
                            l.start_date,
                            l.due_date,
                            l.status,
                            COALESCE(SUM(r.amount), 0) as total_repaid,
                            (l.amount + (l.amount * l.interest_rate / 100)) - COALESCE(SUM(r.amount), 0) as current_balance -- Simplified balance
                        FROM loans l
                        JOIN members m ON l.member_id = m.id
                        LEFT JOIN repayments r ON l.id = r.loan_id
                        GROUP BY l.id, m.name, l.type, l.amount, l.interest_rate, l.start_date, l.due_date, l.status
                        ORDER BY l.start_date DESC
                    """, engine)
                    
                    csv = loans_export.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Loans CSV",
                        data=csv,
                        file_name=f"loans_export_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                
                st.write("**Generate Comprehensive Financial Report (PDF)**")
                if st.button("Generate Full Financial Report PDF"):
                    pdf_buffer = generate_financial_report_pdf()
                    st.download_button(
                        label="Download Financial Report PDF",
                        data=pdf_buffer,
                        file_name=f"financial_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf"
                    )
    finally:
        session.close()

def show_receipt_generator():
    """Provides functionality to generate and download receipts for member contributions."""
    st.header("🧾 Receipt Generator")
    
    tab1, tab2 = st.tabs(["Generate Receipt", "Receipt History"])
    
    session = Session()
    try:
        with tab1:
            st.subheader("Generate Member Contribution Receipt")
            
            # Get active members for receipt generation
            members_df = pd.read_sql("SELECT id, name FROM members WHERE status = 'active' ORDER BY name", engine)
            
            if not members_df.empty:
                selected_member = st.selectbox("Select Member",
                                             options=members_df['id'].tolist(),
                                             format_func=lambda x: members_df[members_df['id'] == x]['name'].iloc[0])
                
                # Get member's recent contributions to select for receipt
                recent_contributions = pd.read_sql("""
                    SELECT c.id, c.date, c.votehead, c.amount, m.date as meeting_date
                    FROM contributions c
                    LEFT JOIN meetings m ON c.meeting_id = m.id
                    WHERE c.member_id = %s
                    ORDER BY c.date DESC
                    LIMIT 20 # Limit to recent contributions
                """, engine, params=[selected_member])
                
                if not recent_contributions.empty:
                    st.subheader("Select Contributions for Receipt")
                    
                    # Multiselect to choose which contributions to include
                    selected_contribution_ids = st.multiselect(
                        "Choose contributions (select multiple)",
                        options=recent_contributions['id'].tolist(),
                        format_func=lambda x: f"{recent_contributions[recent_contributions['id'] == x]['date'].iloc[0].strftime('%Y-%m-%d')} - {recent_contributions[recent_contributions['id'] == x]['votehead'].iloc[0].title()} - KSh {recent_contributions[recent_contributions['id'] == x]['amount'].iloc[0]:,.2f}"
                    )
                    
                    if selected_contribution_ids and st.button("Generate Receipt PDF"):
                        member_name = members_df[members_df['id'] == selected_member]['name'].iloc[0]
                        # Filter the DataFrame to include only selected contributions
                        receipt_data_to_pdf = recent_contributions[recent_contributions['id'].isin(selected_contribution_ids)]
                        
                        pdf_buffer = generate_receipt_pdf(member_name, receipt_data_to_pdf)
                        
                        st.download_button(
                            label="Download Generated Receipt",
                            data=pdf_buffer,
                            file_name=f"receipt_{member_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                            mime="application/pdf"
                        )
                        st.success("Receipt generated successfully!")
                    elif not selected_contribution_ids:
                        st.warning("Please select at least one contribution to generate a receipt.")
                else:
                    st.info("No recent contributions found for this member to generate a receipt.")
            else:
                st.info("No active members available to generate receipts.")
        
        with tab2:
            st.subheader("Receipt History")
            st.info("This section would typically list previously generated receipts. Feature to be implemented in future updates.")
    
    finally:
        session.close()

def show_settings():
    """Provides configuration options for the group and backup/restore functionalities."""
    st.header("⚙️ Settings & Configuration")
    
    tab1, tab2, tab3 = st.tabs(["Group Settings", "Backup & Restore", "System Info"])
    
    with tab1:
        st.subheader("Group Configuration")
        
        # Group settings form (these would ideally be persisted, e.g., in a 'settings' table)
        # For this example, they are just local Streamlit state.
        with st.form("group_settings_form"):
            st.write("**Meeting Configuration**")
            meeting_day = st.selectbox("Default Meeting Day", 
                                     options=[0, 1, 2, 3, 4, 5, 6],
                                     format_func=lambda x: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][x],
                                     index=6)  # Default to Sunday (index 6)
            
            meeting_occurrence = st.selectbox("Default Meeting Occurrence", 
                                            ["Every week", "Every 2 weeks", "Every 3 weeks", "Monthly"],
                                            index=2)  # Default to every 3 weeks (index 2)
            
            st.write("**Contribution Defaults**")
            default_share_amount = st.number_input("Default Share Amount (KSh)", value=1000.0, step=100.0, min_value=0.0)
            default_welfare_amount = st.number_input("Default Welfare Amount (KSh)", value=300.0, step=50.0, min_value=0.0)
            
            st.write("**Loan Settings Defaults**")
            # These values could be used when issuing new loans
            normal_loan_rate = st.slider("Default Normal Loan Interest Rate (%)", min_value=5, max_value=20, value=10, step=1)
            emergency_loan_rate = st.slider("Default Emergency Loan Monthly Rate (%)", min_value=1, max_value=5, value=2, step=0.5)
            
            if st.form_submit_button("Save Settings"):
                # In a real application, you would save these to a dedicated 'settings' table in your DB
                st.success("Settings saved successfully! (Note: In this demo, settings are not persisted across sessions without a dedicated settings table.)")
    
    with tab2:
        st.subheader("Database Backup & Restore")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Database Backup**")
            if st.button("Create Full Database Backup (JSON)"):
                try:
                    backup_data_json = create_database_backup()
                    st.download_button(
                        label="Download Backup File",
                        data=backup_data_json,
                        file_name=f"shalom_blessing_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
                    st.success("Backup created successfully! Download initiated.")
                except Exception as e:
                    st.error(f"Backup failed: {str(e)}")
        
        with col2:
            st.write("**Restore Database**")
            uploaded_file = st.file_uploader("Upload Backup File (.json)", type=['json'])
            if uploaded_file is not None:
                if st.button("Restore Database from Uploaded File"):
                    st.warning("Restoring will OVERWRITE ALL CURRENT DATA. Proceed with caution!")
                    if st.checkbox("Confirm database restore (check this box to enable restore)"):
                        try:
                            restore_database_backup(uploaded_file)
                            st.success("Database restored successfully from backup!")
                            st.info("Please refresh your browser page to see the restored data.")
                        except Exception as e:
                            st.error(f"Database restore failed: {str(e)}. Please ensure the file is a valid backup.")
    
    with tab3:
        st.subheader("System Information")
        
        session = Session()
        try:
            # Display database statistics
            total_members = session.execute(text("SELECT COUNT(*) FROM members")).scalar()
            active_members = session.execute(text("SELECT COUNT(*) FROM members WHERE status = 'active'")).scalar()
            total_meetings = session.execute(text("SELECT COUNT(*) FROM meetings")).scalar()
            total_contributions = session.execute(text("SELECT COUNT(*) FROM contributions")).scalar()
            total_loans_issued = session.execute(text("SELECT COUNT(*) FROM loans")).scalar()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Members", total_members)
                st.metric("Active Members", active_members)
                st.metric("Total Meetings Recorded", total_meetings)
            
            with col2:
                st.metric("Total Contributions Recorded", total_contributions)
                st.metric("Total Loans Issued", total_loans_issued)
                st.metric("Database Type", "PostgreSQL (SQLAlchemy)")
            
            # System health check indicators
            st.subheader("System Health Status")
            
            # Check database connection
            try:
                session.execute(text("SELECT 1"))
                st.success("✅ Database Connection: Healthy and accessible.")
            except Exception as e:
                st.error(f"❌ Database Connection: Failed. Error: {str(e)}")
            
            # Check for recent data entry
            seven_days_ago = datetime.now() - timedelta(days=7)
            recent_activity_count = session.execute(text("""
                SELECT COUNT(*) FROM contributions 
                WHERE date >= :seven_days_ago
            """), {"seven_days_ago": seven_days_ago.date()}).scalar()
            
            if recent_activity_count > 0:
                st.success(f"✅ Recent Data Activity: {recent_activity_count} new contributions in the last 7 days.")
            else:
                st.warning("⚠️ No recent data activity (contributions) in the last 7 days. Ensure data entry is happening.")
                
        finally:
            session.close()

def create_database_backup():
    """
    Creates a JSON backup of all data across all defined database tables.
    Data is fetched, converted to dictionary records, and then JSON serialized.
    """
    session = Session()
    backup_data = {}
    
    try:
        # List of all table names to backup
        tables = ['members', 'meetings', 'attendance', 'contributions', 'loans', 'repayments', 'penalties', 'expenses', 'dividends']
        
        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table}", engine)
            # Convert any datetime/date objects to string for JSON serialization compatibility
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S') # Or just '%Y-%m-%d' for dates
                elif pd.api.types.is_object_dtype(df[col]):
                     # Attempt to convert objects that might be dates but not yet recognized
                    try:
                        df[col] = pd.to_datetime(df[col]).dt.date # Convert to date objects
                    except:
                            pass # Keep as string if not convertible
            
            backup_data[table] = df.to_dict('records') # Convert DataFrame to list of dictionaries
        
        return json.dumps(backup_data, indent=2, default=str) # Use default=str for any remaining un-serializable types
    
    finally:
        session.close()

def restore_database_backup(uploaded_file):
    """
    Restores the database from an uploaded JSON backup file.
    It truncates existing tables and then inserts data from the backup.
    WARNING: This operation is destructive and will erase existing data.
    """
    session = Session()
    
    try:
        # Read and parse the JSON backup data
        backup_data = json.loads(uploaded_file.read())
        
        # Order of tables for truncation and restoration to avoid foreign key issues
        # (reverse order of creation, then forward for restoration)
        ordered_tables = [
            'dividends', 'repayments', 'penalties', 'expenses', 'loans', 
            'contributions', 'attendance', 'meetings', 'members'
        ]
        
        # Disable foreign key checks for SQLite if applicable (PostgreSQL handles CASCADE automatically)
        # For PostgreSQL, TRUNCATE with CASCADE is sufficient if tables are ordered correctly.
        
        # --- TRUNCATE all tables ---
        # This will clear all existing data and reset identity columns.
        for table_name in ordered_tables:
            session.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
        session.commit() # Commit truncation first
        
        # --- RESTORE data into tables ---
        # Iterate in the *original* dependency order (members, then meetings, then contributions, etc.)
        # This assumes your `ordered_tables` is effectively reversed of creation already.
        # Let's re-order correctly for insertion to avoid FK errors.
        insertion_order = [
            'members', 'meetings', 'attendance', 'contributions', 'loans', 
            'repayments', 'penalties', 'expenses', 'dividends'
        ]

        for table_name in insertion_order:
            if table_name in backup_data and backup_data[table_name]:
                records = backup_data[table_name]
                df = pd.DataFrame(records)
                
                # Convert string dates back to date objects if necessary before saving
                for col in df.columns:
                    if col in ['date', 'join_date', 'start_date', 'due_date'] and pd.api.types.is_object_dtype(df[col]):
                        try:
                            df[col] = pd.to_datetime(df[col]).dt.date # Convert to date objects
                        except:
                            pass # Keep as string if not convertible
                
                # Use if_exists='append' to add new rows
                df.to_sql(table_name, engine, if_exists='append', index=False)
        
        session.commit() # Commit the restoration of data
        
    except Exception as e:
        session.rollback() # Rollback on any error during restore
        raise e
    finally:
        session.close()


# --- Authentication and Main Application Flow ---

def show_login():
    """Displays the login page for the application."""
    st.title("🏦 Shalom Blessing Self-Help Group")
    st.subheader("Login to Access Group Management System")
    
    # Define credentials and preauthorized emails separately for clarity
    credentials_config = {
        'usernames': {
            'admin': {'email': 'admin@shalomblessing.com', 'name': 'Administrator', 
                      'password': hashlib.sha256("admin123".encode()).hexdigest()}, # Hashed password for 'admin123'
            'member': {'email': 'member@shalomblessing.com', 'name': 'Member User',
                       'password': hashlib.sha256("member123".encode()).hexdigest()} # Hashed password for 'member123'
        }
    }
    
    preauthorized_emails_config = {'emails': ['admin@shalomblessing.com']}

    # Custom authentication using Streamlit-Authenticator for more robust handling
    authenticator = stauth.Authenticate(
        credentials=credentials_config,
        cookie_name='shalom_blessing_auth_cookie',
        key='shalom_blessing_auth_key',
        cookie_expiry_days=30,
        preauthorized=preauthorized_emails_config
    )

    name, authentication_status, username = authenticator.login('Login', 'main')

    if authentication_status == False:
        st.error('Username/password is incorrect')
    elif authentication_status == None:
        st.warning('Please enter your username and password')
    elif authentication_status:
        st.session_state['authenticated'] = True
        st.session_state['username'] = username
        st.session_state['user_name'] = name # Store the display name
        st.session_state['user_type'] = 'admin' if username == 'admin' else 'member'
        st.success(f"Welcome, {name}!")
        st.rerun() # Rerun to switch to the main app dashboard

def show_member_dashboard():
    """Displays a limited dashboard specific to member users."""
    st.title("👤 Member Portal")
    st.write(f"Welcome, {st.session_state.get('user_name', 'Member')}!")
    
    # Get member info using the logged-in username
    session = Session()
    try:
        member_info = session.execute(text("""
            SELECT id, name FROM members 
            WHERE LOWER(name) = LOWER(:username) AND status = 'active'
        """), {"username": st.session_state['username']}).fetchone()
        
        if member_info:
            member_id = member_info[0]
            member_display_name = member_info[1]
            
            st.subheader(f"Your Financial Overview")
            
            # Financial summary for the logged-in member
            member_shares = session.execute(text("""
                SELECT COALESCE(SUM(amount), 0) 
                FROM contributions 
                WHERE member_id = :member_id AND votehead = 'shares'
            """), {"member_id": member_id}).scalar()
            
            member_welfare = session.execute(text("""
                SELECT COALESCE(SUM(amount), 0) 
                FROM contributions 
                WHERE member_id = :member_id AND votehead = 'welfare'
            """), {"member_id": member_id}).scalar()
            
            active_loans_principal = session.execute(text("""
                SELECT COALESCE(SUM(amount), 0) 
                FROM loans 
                WHERE member_id = :member_id AND status = 'active'
            """), {"member_id": member_id}).scalar()

            # Calculate actual loan balance for each active loan
            active_loans_data = session.query(Loan).filter(Loan.member_id == member_id, Loan.status == 'active').all()
            total_active_loan_balance = sum(calculate_loan_balance(loan.id) for loan in active_loans_data)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Shares", f"KSh {member_shares:,.2f}")
                st.metric("Number of Shares", f"{int(member_shares/1000)}")
            with col2:
                st.metric("Total Welfare", f"KSh {member_welfare:,.2f}")
            with col3:
                st.metric("Total Active Loan Balance", f"KSh {total_active_loan_balance:,.2f}")
            
            # Recent contributions of the member
            st.subheader("Your Recent Contributions")
            recent_contributions = pd.read_sql("""
                SELECT c.date, c.votehead, c.amount, m.date as meeting_date
                FROM contributions c
                LEFT JOIN meetings m ON c.meeting_id = m.id
                WHERE c.member_id = %s
                ORDER BY c.date DESC
                LIMIT 10
            """, engine, params=[member_id])
            
            if not recent_contributions.empty:
                st.dataframe(recent_contributions, use_container_width=True)
            else:
                st.info("No contributions recorded for you yet.")
            
            # Loans taken by the member
            st.subheader("Your Loans")
            member_loans = pd.read_sql("""
                SELECT type, amount, interest_rate, start_date, due_date, status
                FROM loans
                WHERE member_id = %s
                ORDER BY start_date DESC
            """, engine, params=[member_id])
            
            if not member_loans.empty:
                st.dataframe(member_loans, use_container_width=True)
            else:
                st.info("No loans recorded for you.")
            
            # Option to generate personal statement
            if st.button("Generate My Financial Statement (PDF)"):
                # Fetch all contribution history for the statement
                full_contribution_history = pd.read_sql("""
                    SELECT c.date, c.votehead, c.amount, m.date as meeting_date
                    FROM contributions c
                    LEFT JOIN meetings m ON c.meeting_id = m.id
                    WHERE c.member_id = %s
                    ORDER BY c.date DESC
                """, engine, params=[member_id])
                
                pdf_buffer = generate_member_statement_pdf(member_display_name, full_contribution_history)
                st.download_button(
                    label="Download My Statement",
                    data=pdf_buffer,
                    file_name=f"my_statement_{member_display_name.replace(' ', '_')}.pdf",
                    mime="application/pdf"
                )
        else:
            st.error("Your member information could not be found.")
    
    finally:
        session.close()

# Main application function that orchestrates the entire Streamlit app
def main():
    st.set_page_config(
        page_title="Shalom Blessing Self-Help Group",
        page_icon="🤝", # Using a handshake emoji
        layout="wide", # Use wide layout for better space utilization
        initial_sidebar_state="expanded" # Sidebar expanded by default
    )
    
    # Custom CSS for better aesthetics and branding
    st.markdown("""
        <style>
        .main-header {
            background: linear-gradient(90deg, #1f4e79, #2e8b57); /* Gradient background */
            color: white;
            padding: 1.5rem;
            border-radius: 10px;
            text-align: center;
            margin-bottom: 2rem;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }
        .main-header h1 {
            color: white; /* Ensure heading is white */
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }
        .main-header p {
            color: #e0e0e0;
            font-size: 1.1rem;
        }
        .st-emotion-cache-1jm69l1 { /* Targeting the main content area */
            padding-top: 1rem;
        }
        /* Metric cards styling */
        div[data-testid="stMetric"] {
            background-color: #f0f2f6; /* Light grey background */
            border-radius: 10px;
            padding: 1rem;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            border-left: 5px solid #2e8b57; /* Accent border */
            margin-bottom: 1rem;
            font-weight: bold;
        }
        div[data-testid="stMetric"] label {
            color: #1f4e79; /* Dark blue for metric labels */
            font-size: 1rem;
        }
        div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
            color: #0c4a6e; /* Even darker blue for values */
            font-size: 1.8rem;
        }
        /* Buttons styling */
        .stButton button {
            background-color: #2e8b57; /* Sea green */
            color: white;
            border-radius: 8px;
            border: none;
            padding: 0.6rem 1.2rem;
            font-size: 1rem;
            cursor: pointer;
            transition: background-color 0.3s ease, transform 0.2s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stButton button:hover {
            background-color: #3cb371; /* Lighter sea green on hover */
            transform: translateY(-2px);
        }
        .stButton button:active {
            transform: translateY(0);
        }
        /* Primary button specific styling */
        .stButton button[kind="primary"] {
            background-color: #1f4e79; /* Dark blue */
        }
        .stButton button[kind="primary"]:hover {
            background-color: #2a6390; /* Lighter dark blue on hover */
        }
        /* Expander styling */
        .streamlit-expanderHeader {
            background-color: #e6f3f9; /* Light blue for expander headers */
            border-radius: 8px;
            padding: 0.5rem 1rem;
            margin-bottom: 0.5rem;
            border: 1px solid #cce7f5;
        }
        .streamlit-expanderContent {
            background-color: #f9fcfe; /* Even lighter blue for content */
            border-radius: 8px;
            padding: 1rem;
            border: 1px solid #cce7f5;
            border-top: none;
        }
        /* Sidebar styling */
        .st-emotion-cache-vk338m { /* Targeting the sidebar */
            background-color: #1f4e79; /* Dark blue for sidebar */
            color: white;
        }
        .st-emotion-cache-vk338m .stSelectbox label, .st-emotion-cache-vk338m .stButton button, .st-emotion-cache-vk338m .stRadio label {
            color: white; /* Ensure text in sidebar is white */
        }
        .st-emotion-cache-vk338m .stSelectbox div[data-baseweb="select"] {
            background-color: #2e8b57; /* Sea green for selectbox */
            color: white;
        }
        .st-emotion-cache-vk338m .stSelectbox div[data-baseweb="select"] div {
            color: white;
        }
        .st-emotion-cache-vk338m .stSelectbox div[data-baseweb="select"]:hover {
            background-color: #3cb371;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # --- Authentication Flow ---
    # Initialize authentication status in session state if not present
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False
        st.session_state['user_type'] = None
        st.session_state['username'] = None
        st.session_state['user_name'] = None
    
    # If not authenticated, show the login page
    if not st.session_state['authenticated']:
        show_login()
        return # Stop execution here until logged in
    
    # --- Authenticated User Interface ---
    # Logout button (always available in sidebar once logged in)
    with st.sidebar:
        st.markdown("---")
        if st.button("Logout", key="sidebar_logout_btn"):
            st.session_state['authenticated'] = False
            st.session_state['user_type'] = None
            st.session_state['username'] = None
            st.session_state['user_name'] = None
            st.rerun() # Rerun to go back to login page
        st.markdown("---")

    # If authenticated as a 'member', show the member-specific dashboard
    if st.session_state.get('user_type') == 'member':
        # Sidebar for member portal
        with st.sidebar:
            st.title("📱 Member Portal")
            st.write(f"Logged in as: **{st.session_state.get('user_name', 'Member')}**")
            st.markdown("---")
            # Member specific navigation could go here if needed, but for now, it's just the dashboard
        
        show_member_dashboard()
        return # Stop execution for member user after showing their dashboard
    
    # --- Admin Interface (if authenticated as 'admin') ---
    with st.sidebar:
        st.title("🏦 Shalom Blessing")
        st.write("Self-Help Group Management")
        st.write(f"Logged in as: **{st.session_state.get('user_name', 'Admin')}**")
        
        st.markdown("---") # Separator
        
        # Navigation menu for admin users
        menu_options = [
            "🏠 Dashboard",
            "👥 Members",
            "📅 Meetings",
            "💰 Contributions",
            "🏦 Loans",
            "⚠️ Penalties",
            "💸 Expenses", 
            "💎 Dividends",
            "📊 Reports",
            "🧾 Receipts",
            "⚙️ Settings"
        ]
        
        selected = st.selectbox("Navigation", menu_options, key="admin_navigation")
    
    # Route to appropriate function based on sidebar selection
    if selected == "🏠 Dashboard":
        show_dashboard()
    elif selected == "👥 Members":
        show_members()
    elif selected == "📅 Meetings":
        show_meetings()
    elif selected == "💰 Contributions":
        show_contributions()
    elif selected == "🏦 Loans":
        show_loans()
    elif selected == "⚠️ Penalties":
        show_penalties()
    elif selected == "💸 Expenses":
        show_expenses()
    elif selected == "💎 Dividends":
        show_dividends()
    elif selected == "📊 Reports":
        show_reports()
    elif selected == "🧾 Receipts":
        show_receipt_generator()
    elif selected == "⚙️ Settings":
        show_settings()

# --- Database Initialization on Application Startup ---
def init_database():
    """Initializes database tables if they don't exist. This function is designed to run once."""
    try:
        # Create all tables defined by Base.metadata if they don't exist
        Base.metadata.create_all(engine)
        st.success("Database initialized successfully (if tables didn't exist).")
    except Exception as e:
        # Catch any errors during database initialization and display them
        st.error(f"Database initialization failed: {str(e)}")

# --- Entry Point of the Streamlit Application ---
if __name__ == "__main__":
    # Use Streamlit's session state to ensure database initialization runs only once
    if 'db_initialized' not in st.session_state:
        init_database()
        st.session_state['db_initialized'] = True # Set flag after initialization
    
    main() # Call the main application function to start the UI
