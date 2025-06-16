import streamlit as st
import pandas as pd
import os
import sqlite3
from datetime import datetime, date
from fpdf import FPDF
import random
from num2words import num2words
import qrcode
import plotly.express as px
import plotly.graph_objects as go
import base64

# MUST BE FIRST STREAMLIT COMMAND
st.set_page_config(page_title="Success Achievers - Finance Management", page_icon="🏫", layout="wide")

# ------------- CONFIG -------------
logo_file = "logo.jpg.jpg"
qr_temp_file = "qrcode_temp.png"
book_fund_target = 2000
db_file = "school_finance.db"

# --- Define paths for old CSV files for migration ---
students_csv_file = "students.csv.xlsx"
payments_csv_file = "payments.csv"
tours_payments_csv_file = "tours_payments.csv"


# Tour Groups Configuration
TOUR_GROUPS = {
    "Group 1": {
        "classes": ["PG", "PP1"],
        "date": "2025-06-14",
        "deadline": "2025-06-12",
        "destination": "Meru - Makutano Aquapark & Nkunga Forest",
        "amount": 1000
    },
    "Group 2": {
        "classes": ["PP2", "Grade 1", "GR1"],
        "date": "2025-06-21",
        "deadline": "2025-06-18",
        "destination": "Nanyuki - Mt. Kenya Lounge, Animal Orphanage, Fun City",
        "amount": 2500
    },
    "Group 3": {
        "classes": ["Grade 2", "Grade 3", "GR2", "GR3"],
        "date": "2025-06-25",
        "deadline": "2025-06-21",
        "destination": "Mwea - Rice Irrigation & Mwea Nice City",
        "amount": 2500
    },
    "Group 4": {
        "classes": ["Grade 4", "Grade 5", "GR4", "GR5"],
        "date": "2025-08-05",
        "deadline": "2025-08-01",
        "destination": "Nairobi - Snake Park, Bomas, Parliament, Giraffe Centre",
        "amount": 3500
    },
    "Group 5": {
        "classes": ["Grade 6", "Grade 7", "Grade 8", "GR6", "GR7", "GR8"],
        "date": "2025-08-08",
        "deadline": "2025-08-05",
        "destination": "Kamburu Dam - Hydroelectric Power Tour",
        "amount": 2500
    }
}

# ------------- DATABASE INITIALIZATION -------------
def init_database():
    """Initialize SQLite database with required tables."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Students table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS students (
            reg_number TEXT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            class_code TEXT NOT NULL,
            date_added DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Book fund payments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS book_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATETIME DEFAULT CURRENT_TIMESTAMP,
            reg_number TEXT,
            full_name TEXT,
            class_code TEXT,
            amount REAL,
            mpesa_ref TEXT,
            FOREIGN KEY (reg_number) REFERENCES students (reg_number)
        )
    ''')
    
    # Tour payments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tour_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATETIME DEFAULT CURRENT_TIMESTAMP,
            reg_number TEXT,
            full_name TEXT,
            class_code TEXT,
            tour_group TEXT,
            amount REAL,
            mpesa_ref TEXT,
            FOREIGN KEY (reg_number) REFERENCES students (reg_number)
        )
    ''')

    # Backup tables for deleted records
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deleted_book_payments (
            id INTEGER,
            date DATETIME,
            reg_number TEXT,
            full_name TEXT,
            class_code TEXT,
            amount REAL,
            mpesa_ref TEXT,
            deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            deleted_reason TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deleted_tour_payments (
            id INTEGER,
            date DATETIME,
            reg_number TEXT,
            full_name TEXT,
            class_code TEXT,
            tour_group TEXT,
            amount REAL,
            mpesa_ref TEXT,
            deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            deleted_reason TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

# Run initialization once
init_database()

# ------------- DATA MIGRATION -------------
def migrate_csv_to_sqlite():
    """Migrate data from old CSV files to the SQLite database."""
    conn = sqlite3.connect(db_file)
    migrated_counts = {"students": 0, "book_payments": 0, "tour_payments": 0}
    
    # Migrate students
    if os.path.exists(students_csv_file):
        try:
            df = pd.read_excel(students_csv_file)
            for _, row in df.iterrows():
                try:
                    conn.execute("INSERT OR IGNORE INTO students (reg_number, first_name, last_name, class_code) VALUES (?, ?, ?, ?)",
                                 (str(row['Reg Number']), row['First Name'], row['Last Name'], row['Class Code']))
                except sqlite3.Error:
                    pass # Ignore rows that fail to insert
            migrated_counts["students"] = len(df)
        except Exception as e:
            st.error(f"Failed to migrate students: {e}")

    # Migrate book fund payments
    if os.path.exists(payments_csv_file):
        try:
            df = pd.read_csv(payments_csv_file)
            for _, row in df.iterrows():
                try:
                    conn.execute("INSERT OR IGNORE INTO book_payments (date, reg_number, full_name, class_code, amount, mpesa_ref) VALUES (?, ?, ?, ?, ?, ?)",
                                 (row['Date'], str(row['Reg Number']), row['Full Name'], row['Class'], row['Amount'], row['MPESA Ref']))
                except sqlite3.Error:
                    pass
            migrated_counts["book_payments"] = len(df)
        except Exception as e:
            st.error(f"Failed to migrate book payments: {e}")

    # Migrate tour payments
    if os.path.exists(tours_payments_csv_file):
        try:
            df = pd.read_csv(tours_payments_csv_file)
            for _, row in df.iterrows():
                try:
                    conn.execute("INSERT OR IGNORE INTO tour_payments (date, reg_number, full_name, class_code, tour_group, amount, mpesa_ref) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                 (row['Date'], str(row['Reg Number']), row['Full Name'], row['Class'], row['Tour Group'], row['Amount'], row['MPESA Ref']))
                except sqlite3.Error:
                    pass
            migrated_counts["tour_payments"] = len(df)
        except Exception as e:
            st.error(f"Failed to migrate tour payments: {e}")

    conn.commit()
    conn.close()
    return migrated_counts

# --- One-time Automatic Migration on Startup ---
@st.cache_resource
def run_one_time_migration():
    """Checks if the database is empty and runs migration from CSV files if needed."""
    conn = sqlite3.connect(db_file)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM students")
        count = cursor.fetchone()[0]
        db_was_empty = (count == 0)
    except sqlite3.Error:
        db_was_empty = True # Table might not exist yet
    finally:
        conn.close()

    # Check if any of the old data files exist
    files_exist = os.path.exists(students_csv_file) or os.path.exists(payments_csv_file) or os.path.exists(tours_payments_csv_file)

    if db_was_empty and files_exist:
        with st.spinner("Performing first-time data migration from old CSV/Excel files... Please wait."):
            counts = migrate_csv_to_sqlite()
        
        st.success("Automatic one-time data migration completed!")
        st.info(f"Imported {counts['students']} students, {counts['book_payments']} book payments, and {counts['tour_payments']} tour payments.")
        st.info("The app will now reload with the imported data.")
        st.rerun() # Rerun to load the new data into the UI
    return True

run_one_time_migration()

# ------------- HELPERS -------------
@st.cache_data
def load_students_from_db():
    """Load students from SQLite database."""
    conn = sqlite3.connect(db_file)
    students_df = pd.read_sql_query("SELECT * FROM students ORDER BY class_code, first_name", conn)
    conn.close()
    return students_df

def add_student_to_db(reg_number, first_name, last_name, class_code):
    """Add a new student to the database."""
    conn = sqlite3.connect(db_file)
    try:
        conn.execute('''
            INSERT INTO students (reg_number, first_name, last_name, class_code)
            VALUES (?, ?, ?, ?)
        ''', (str(reg_number), first_name, last_name, class_code))
        conn.commit()
        success = True
        message = "Student added successfully!"
    except sqlite3.IntegrityError:
        success = False
        message = "Student with this registration number already exists!"
    except Exception as e:
        success = False
        message = f"Error adding student: {e}"
    finally:
        conn.close()
    
    return success, message

def generate_payment_code():
    """Generate a random payment code."""
    return str(random.randint(1000000000, 9999999999))

def create_qr_code(data, path=qr_temp_file):
    """Create a QR code image."""
    try:
        qr = qrcode.QRCode(version=1, box_size=2, border=1)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill='black', back_color='white')
        img.save(path)
        return True
    except Exception as e:
        st.warning(f"Could not generate QR Code: {e}")
        return False

def get_total_paid_from_db(reg_number, payment_type="book_fund"):
    """Get total amount paid by a student from the database."""
    conn = sqlite3.connect(db_file)
    table = "book_payments" if payment_type == "book_fund" else "tour_payments"
    cursor = conn.cursor()
    cursor.execute(f"SELECT SUM(amount) FROM {table} WHERE reg_number = ?", (str(reg_number),))
    result = cursor.fetchone()[0]
    conn.close()
    return result if result else 0

def get_student_tour_group(class_code):
    """Determine a student's tour group based on their class."""
    class_code_upper = str(class_code).upper().strip()
    for group, details in TOUR_GROUPS.items():
        class_variations = [cls.upper() for cls in details["classes"]]
        if class_code_upper in class_variations:
            return group, details
    return None, None

def create_printable_receipt_pdf(payment_data, payment_type="book_fund"):
    """Create a full-featured, single-page A5 receipt."""
    payment_code = generate_payment_code()
    amount_words = num2words(payment_data["amount"], lang='en').replace(",", "").title() + " Shillings Only"
    date_str = datetime.now().strftime("%d %b, %Y %I:%M %p")
    
    qr_data = f"Success Achievers\nCode: {payment_code}\nReg: {payment_data['reg_number']}\nName: {payment_data['full_name']}"
    qr_code_generated = create_qr_code(qr_data)

    class PDF(FPDF):
        def header(self):
            # Logo
            if os.path.exists(logo_file):
                self.image(logo_file, x=10, y=8, w=15)
            # School Name
            self.set_font("Arial", 'B', 12)
            self.cell(0, 10, "SUCCESS ACHIEVERS ACADEMY", ln=True, align='C')
            self.set_font("Arial", '', 8)
            self.cell(0, 5, "In God We Trust", ln=True, align='C')
            # Receipt Title
            self.set_font("Arial", 'B', 10)
            payment_title = "Educational Tour Receipt" if payment_type == "tour" else "Book Fund Receipt"
            self.cell(0, 8, payment_title, ln=True, align='C')
            self.ln(3)
            
        def watermark(self):
            self.set_font('Arial', 'B', 50)
            self.set_text_color(230, 230, 230)
            self.rotate(45, x=74, y=105)
            self.text(35, 110, 'SUCCESS')
            self.rotate(0)
            self.set_text_color(0,0,0)

        def footer(self):
            self.set_y(-15)
            self.set_font("Arial", 'I', 8)
            self.cell(0, 10, "Contact: 0794 451 007 | info@successachievers.ac.ke", 0, 0, 'C')

    # Create PDF object
    pdf = PDF(orientation='P', unit='mm', format='A5')
    pdf.add_page()
    pdf.watermark()
    
    # Set font for the content
    pdf.set_font("Arial", '', 9)
    pdf.set_y(40) # Position content below header

    def add_line(label, value, bold_label=True):
        """Adds a line with label and value to the PDF."""
        if bold_label:
            pdf.set_font("Arial", 'B', 9)
        pdf.cell(40, 6, f"{label}:", ln=0)
        pdf.set_font("Arial", '', 9)
        # multi_cell automatically handles line breaks and does not accept 'ln'
        pdf.multi_cell(0, 6, str(value)) 
        pdf.ln(0.1) # Add a small line break after multi_cell for spacing

    # Content
    add_line("Payment Code", payment_code)
    add_line("Reg Number", payment_data["reg_number"])
    add_line("Student Name", payment_data["full_name"])
    add_line("Class", payment_data["class"])
    
    if payment_type == "tour":
        add_line("Tour Group", payment_data.get("tour_group", "N/A"))
        add_line("Destination", payment_data.get("destination", "N/A"))
    
    add_line("Amount Paid", f"KES {payment_data['amount']:,.2f}")
    if payment_type == "book_fund":
        add_line("Balance", f"KES {payment_data.get('outstanding', 0):,.2f}")
    
    # Amount in words (on multiple lines if necessary)
    pdf.set_font("Arial", 'B', 9)
    pdf.cell(40, 6, "In Words:", ln=0)
    pdf.set_font("Arial", 'I', 9)
    pdf.multi_cell(0, 6, amount_words)

    pdf.ln(4) # Spacer

    add_line("Paid On", date_str)
    add_line("Reference", payment_data.get("mpesa_ref", "N/A"))
    
    # QR Code
    if qr_code_generated and os.path.exists(qr_temp_file):
        pdf.image(qr_temp_file, x=115, y=100, w=25)

    return pdf, payment_code

def create_printable_pdf_link(pdf_content):
    """Create a link that opens a PDF in a new tab for direct printing."""
    b64 = base64.b64encode(pdf_content).decode()
    return f'''
    <a href="data:application/pdf;base64,{b64}" target="_blank" 
       style="display: inline-block; padding: 10px 20px; background-color: #4CAF50; color: white; 
              text-decoration: none; border-radius: 5px; font-weight: bold; margin: 5px;">
       🖨️ View & Print Receipt
    </a>'''

def save_payment_to_db(payment_data, payment_type="book_fund"):
    """Save payment to the database."""
    conn = sqlite3.connect(db_file)
    try:
        if payment_type == "book_fund":
            conn.execute('''
                INSERT INTO book_payments (reg_number, full_name, class_code, amount, mpesa_ref)
                VALUES (?, ?, ?, ?, ?)
            ''', (payment_data['reg_number'], payment_data['full_name'], payment_data['class'], 
                  payment_data['amount'], payment_data.get('mpesa_ref', 'N/A')))
        else: # tour payment
            conn.execute('''
                INSERT INTO tour_payments (reg_number, full_name, class_code, tour_group, amount, mpesa_ref)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (payment_data['reg_number'], payment_data['full_name'], payment_data['class'],
                  payment_data.get('tour_group', 'N/A'), payment_data['amount'], payment_data.get('mpesa_ref', 'N/A')))
        conn.commit()
        return True, "Payment saved successfully!"
    except Exception as e:
        return False, f"Error saving payment: {e}"
    finally:
        conn.close()

def create_payment_list_pdf(df, title):
    """Generates a PDF list of payments from a dataframe."""
    class PDF(FPDF):
        def header(self):
            if os.path.exists(logo_file):
                self.image(logo_file, 10, 8, 25)
            self.set_font('Arial', 'B', 15)
            self.cell(0, 10, 'Success Achievers Academy', 0, 1, 'C')
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, title, 0, 1, 'C')
            self.ln(10)

        def footer(self):
            self.set_y(-15)
            self.set_font('Arial', 'I', 8)
            self.cell(0, 10, 'Page ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

    pdf = PDF(orientation='P', unit='mm', format='A4')
    pdf.alias_nb_pages()
    pdf.add_page()
    
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(10, 10, '#', 1, 0, 'C')
    pdf.cell(70, 10, 'Student Name', 1, 0, 'C')
    pdf.cell(25, 10, 'Reg Number', 1, 0, 'C')
    pdf.cell(25, 10, 'Class', 1, 0, 'C')
    pdf.cell(30, 10, 'Amount (KES)', 1, 0, 'C')
    pdf.cell(30, 10, 'Date', 1, 1, 'C')

    pdf.set_font('Arial', '', 9)
    for i, row in df.iterrows():
        pdf.cell(10, 10, str(i + 1), 1, 0, 'C')
        pdf.cell(70, 10, str(row['full_name']), 1, 0)
        pdf.cell(25, 10, str(row['reg_number']), 1, 0)
        pdf.cell(25, 10, str(row['class_code']), 1, 0)
        pdf.cell(30, 10, f"{row['amount']:,.0f}", 1, 0, 'R')
        pdf.cell(30, 10, pd.to_datetime(row['date']).strftime('%Y-%m-%d'), 1, 1)
        
    return pdf.output(dest='S').encode('latin-1')

def get_payments_from_db(payment_type="book_fund", class_filter=None):
    """Get payments from the database with an optional class filter."""
    conn = sqlite3.connect(db_file)
    table = "book_payments" if payment_type == "book_fund" else "tour_payments"
    
    if class_filter and class_filter != "All Classes":
        query = f"SELECT * FROM {table} WHERE class_code = ? ORDER BY date DESC"
        df = pd.read_sql_query(query, conn, params=(class_filter,))
    else:
        query = f"SELECT * FROM {table} ORDER BY date DESC"
        df = pd.read_sql_query(query, conn)
    
    conn.close()
    return df

# ------------- ENHANCED HELPER FUNCTIONS (FROM enhanced_school_app.py and part2) -------------
def edit_student_in_db(old_reg_number, new_reg_number, first_name, last_name, class_code):
    """Edit an existing student's information."""
    conn = sqlite3.connect(db_file)
    try:
        # Update student record
        conn.execute('''
            UPDATE students 
            SET reg_number = ?, first_name = ?, last_name = ?, class_code = ?
            WHERE reg_number = ?
        ''', (str(new_reg_number), first_name, last_name, class_code, str(old_reg_number)))
        
        # Update payment records if reg number changed
        if old_reg_number != new_reg_number:
            conn.execute('''
                UPDATE book_payments SET reg_number = ? WHERE reg_number = ?
            ''', (str(new_reg_number), str(old_reg_number)))
            
            conn.execute('''
                UPDATE tour_payments SET reg_number = ? WHERE reg_number = ?
            ''', (str(new_reg_number), str(old_reg_number)))
        
        conn.commit()
        return True, "Student information updated successfully!"
    except sqlite3.IntegrityError as e:
        return False, f"Error: Registration number already exists or other constraint violation: {e}"
    except Exception as e:
        return False, f"Error updating student: {e}"
    finally:
        conn.close()

def delete_student_from_db(reg_number):
    """Delete a student and all their payment records."""
    conn = sqlite3.connect(db_file)
    try:
        # Check if student has any payments
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM book_payments WHERE reg_number = ?", (str(reg_number),))
        book_payments_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tour_payments WHERE reg_number = ?", (str(reg_number),))
        tour_payments_count = cursor.fetchone()[0]
        
        if book_payments_count > 0 or tour_payments_count > 0:
            return False, f"Cannot delete student with existing payments. Student has {book_payments_count} book payments and {tour_payments_count} tour payments."
        
        # Delete student
        cursor.execute("DELETE FROM students WHERE reg_number = ?", (str(reg_number),))
        conn.commit()
        
        if cursor.rowcount > 0:
            return True, "Student deleted successfully!"
        else:
            return False, "Student not found!"
    except Exception as e:
        return False, f"Error deleting student: {e}"
    finally:
        conn.close()

def get_payment_by_id(payment_id, payment_type="book_fund"):
    """Get a specific payment record by ID."""
    conn = sqlite3.connect(db_file)
    table = "book_payments" if payment_type == "book_fund" else "tour_payments"
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table} WHERE id = ?", (payment_id,))
        result = cursor.fetchone()
        if result:
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, result))
        return None
    finally:
        conn.close()

def edit_payment_in_db(payment_id, amount, mpesa_ref, payment_type="book_fund"):
    """Edit an existing payment record."""
    conn = sqlite3.connect(db_file)
    table = "book_payments" if payment_type == "book_fund" else "tour_payments"
    try:
        conn.execute(f'''
            UPDATE {table} 
            SET amount = ?, mpesa_ref = ?
            WHERE id = ?
        ''', (amount, mpesa_ref, payment_id))
        conn.commit()
        
        if conn.total_changes > 0:
            return True, "Payment updated successfully!"
        else:
            return False, "Payment not found!"
    except Exception as e:
        return False, f"Error updating payment: {e}"
    finally:
        conn.close()

def delete_payment_from_db(payment_id, payment_type="book_fund", reason="Manual deletion"):
    """Delete a payment record and backup to deleted_payments table."""
    conn = sqlite3.connect(db_file)
    table = "book_payments" if payment_type == "book_fund" else "tour_payments"
    backup_table = f"deleted_{table}"
    
    try:
        # Get the payment record first
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table} WHERE id = ?", (payment_id,))
        payment_record = cursor.fetchone()
        
        if not payment_record:
            return False, "Payment not found!"
        
        # Backup to deleted table
        if payment_type == "book_fund":
            conn.execute('''
                INSERT INTO deleted_book_payments 
                (id, date, reg_number, full_name, class_code, amount, mpesa_ref, deleted_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*payment_record, reason))
        else:
            conn.execute('''
                INSERT INTO deleted_tour_payments 
                (id, date, reg_number, full_name, class_code, tour_group, amount, mpesa_ref, deleted_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*payment_record, reason))
        
        # Delete the original record
        cursor.execute(f"DELETE FROM {table} WHERE id = ?", (payment_id,))
        conn.commit()
        
        return True, "Payment deleted and backed up successfully!"
    except Exception as e:
        return False, f"Error deleting payment: {e}"
    finally:
        conn.close()

def check_duplicate_payments(reg_number, amount, payment_type="book_fund", days_threshold=1):
    """Check for potential duplicate payments within a time threshold."""
    conn = sqlite3.connect(db_file)
    table = "book_payments" if payment_type == "book_fund" else "tour_payments"
    try:
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT * FROM {table} 
            WHERE reg_number = ? AND amount = ? 
            AND date >= datetime('now', '-{days_threshold} days')
            ORDER BY date DESC
        ''', (str(reg_number), amount))
        
        duplicates = cursor.fetchall()
        return len(duplicates) > 0, duplicates
    finally:
        conn.close()

def get_student_payment_summary(reg_number):
    """Get comprehensive payment summary for a student."""
    conn = sqlite3.connect(db_file)
    try:
        # Book fund payments
        book_df = pd.read_sql_query(
            "SELECT * FROM book_payments WHERE reg_number = ? ORDER BY date DESC", 
            conn, params=(str(reg_number),)
        )
        
        # Tour payments
        tour_df = pd.read_sql_query(
            "SELECT * FROM tour_payments WHERE reg_number = ? ORDER BY date DESC", 
            conn, params=(str(reg_number),)
        )
        
        book_total = book_df['amount'].sum() if not book_df.empty else 0
        tour_total = tour_df['amount'].sum() if not tour_df.empty else 0
        
        return {
            'book_payments': book_df,
            'tour_payments': tour_df,
            'book_total': book_total,
            'tour_total': tour_total,
            'grand_total': book_total + tour_total
        }
    finally:
        conn.close()

def backup_database():
    """Create a backup of the database."""
    try:
        backup_filename = f"school_finance_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        # Copy database file
        source_conn = sqlite3.connect(db_file)
        backup_conn = sqlite3.connect(backup_filename)
        source_conn.backup(backup_conn)
        source_conn.close()
        backup_conn.close()
        
        return True, backup_filename
    except Exception as e:
        return False, str(e)

def get_overdue_students():
    """Get students who haven't made payments recently."""
    conn = sqlite3.connect(db_file)
    try:
        # Students with no payments in last 30 days
        query = '''
            SELECT s.reg_number, s.first_name, s.last_name, s.class_code,
                   COALESCE(b.last_payment, 'Never') as last_book_payment,
                   COALESCE(t.last_payment, 'Never') as last_tour_payment
            FROM students s
            LEFT JOIN (
                SELECT reg_number, MAX(date) as last_payment
                FROM book_payments
                GROUP BY reg_number
            ) b ON s.reg_number = b.reg_number
            LEFT JOIN (
                SELECT reg_number, MAX(date) as last_payment
                FROM tour_payments
                GROUP BY reg_number
            ) t ON s.reg_number = t.reg_number
            WHERE (b.last_payment IS NULL OR b.last_payment < datetime('now', '-30 days'))
               OR (t.last_payment IS NULL OR t.last_payment < datetime('now', '-30 days'))
        '''
        
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()

# ------------- ENHANCED INTERFACE FUNCTIONS (FROM enhanced_school_app_part2.py) -------------

def show_student_management():
    """Enhanced student management interface."""
    st.header("👥 Student Management")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Add Student", "Edit Student", "Delete Student", "View All Students"])
    
    with tab1:
        st.subheader("Add New Student")
        with st.form("add_student_form_enhanced"): # Changed form key to avoid conflict
            col1, col2 = st.columns(2)
            with col1:
                reg_number = st.text_input("Registration Number* (e.g., 12345)")
                first_name = st.text_input("First Name*")
            with col2:
                last_name = st.text_input("Last Name*")
                class_code = st.selectbox("Class*", ["PG", "PP1", "PP2", "Grade 1", "Grade 2", "Grade 3", 
                                                   "Grade 4", "Grade 5", "Grade 6", "Grade 7", "Grade 8"])
            
            submitted = st.form_submit_button("Add Student")
            
            if submitted:
                if reg_number and first_name and last_name:
                    success, message = add_student_to_db(reg_number, first_name, last_name, class_code)
                    if success:
                        st.success(message)
                        st.cache_data.clear()  # Clear cache to refresh data
                        st.rerun() # Rerun to update the student list
                    else:
                        st.error(message)
                else:
                    st.error("Please fill in all required fields!")
    
    with tab2:
        st.subheader("Edit Student Information")
        students_df = load_students_from_db()
        
        if not students_df.empty:
            # Student selection
            student_options = [f"{row['reg_number']} - {row['first_name']} {row['last_name']} ({row['class_code']})" 
                             for _, row in students_df.iterrows()]
            selected_student = st.selectbox("Select Student to Edit", student_options)
            
            if selected_student:
                # Get selected student data
                selected_reg = selected_student.split(" - ")[0]
                student_data = students_df[students_df['reg_number'] == selected_reg].iloc[0]
                
                with st.form("edit_student_form"):
                    col1, col2 = st.columns(2)
                    with col1:
                        new_reg_number = st.text_input("Registration Number", value=student_data['reg_number'])
                        new_first_name = st.text_input("First Name", value=student_data['first_name'])
                    with col2:
                        new_last_name = st.text_input("Last Name", value=student_data['last_name'])
                        new_class_code = st.selectbox("Class", 
                                                    ["PG", "PP1", "PP2", "Grade 1", "Grade 2", "Grade 3", 
                                                     "Grade 4", "Grade 5", "Grade 6", "Grade 7", "Grade 8"],
                                                    index=["PG", "PP1", "PP2", "Grade 1", "Grade 2", "Grade 3", 
                                                           "Grade 4", "Grade 5", "Grade 6", "Grade 7", "Grade 8"].index(student_data['class_code']) if student_data['class_code'] in ["PG", "PP1", "PP2", "Grade 1", "Grade 2", "Grade 3", "Grade 4", "Grade 5", "Grade 6", "Grade 7", "Grade 8"] else 0)
                    
                    submitted = st.form_submit_button("Update Student")
                    
                    if submitted:
                        success, message = edit_student_in_db(selected_reg, new_reg_number, new_first_name, new_last_name, new_class_code)
                        if success:
                            st.success(message)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(message)
        else:
            st.info("No students found. Please add students first.")
    
    with tab3:
        st.subheader("Delete Student")
        st.warning("⚠️ Warning: This action cannot be undone!")
        
        students_df = load_students_from_db()
        
        if not students_df.empty:
            student_options = [f"{row['reg_number']} - {row['first_name']} {row['last_name']} ({row['class_code']})" 
                             for _, row in students_df.iterrows()]
            selected_student = st.selectbox("Select Student to Delete", student_options, key="delete_student")
            
            if selected_student:
                selected_reg = selected_student.split(" - ")[0]
                
                # Show student payment summary before deletion
                payment_summary = get_student_payment_summary(selected_reg)
                if payment_summary['book_total'] > 0 or payment_summary['tour_total'] > 0:
                    st.error(f"Cannot delete student with existing payments!")
                    st.info(f"Book Fund Payments: KES {payment_summary['book_total']:,.2f}")
                    st.info(f"Tour Payments: KES {payment_summary['tour_total']:,.2f}")
                    st.info("Please remove all payments before deleting the student.")
                else:
                    confirm = st.checkbox("I confirm I want to delete this student")
                    if st.button("Delete Student", disabled=not confirm):
                        success, message = delete_student_from_db(selected_reg)
                        if success:
                            st.success(message)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(message)
        else:
            st.info("No students found.")
    
    with tab4:
        st.subheader("All Students")
        students_df = load_students_from_db()
        
        if not students_df.empty:
            # Add search functionality
            search_term = st.text_input("🔍 Search students (name or reg number)")
            
            if search_term:
                mask = (students_df['first_name'].str.contains(search_term, case=False, na=False) |
                       students_df['last_name'].str.contains(search_term, case=False, na=False) |
                       students_df['reg_number'].str.contains(search_term, case=False, na=False))
                filtered_df = students_df[mask]
            else:
                filtered_df = students_df
            
            # Class filter
            classes = ["All Classes"] + sorted(students_df['class_code'].unique().tolist())
            selected_class = st.selectbox("Filter by Class", classes)
            
            if selected_class != "All Classes":
                filtered_df = filtered_df[filtered_df['class_code'] == selected_class]
            
            st.dataframe(filtered_df, use_container_width=True)
            st.info(f"Total Students: {len(filtered_df)}")
        else:
            st.info("No students found. Please add students first.")

def show_payment_management():
    """Enhanced payment management interface."""
    st.header("💰 Payment Management")
    
    tab1, tab2 = st.tabs(["Book Fund Payments", "Tour Payments"])
    
    with tab1:
        show_enhanced_payment_interface("book_fund")
    
    with tab2:
        show_enhanced_payment_interface("tour")

def show_enhanced_payment_interface(payment_type):
    """Enhanced payment interface with edit/delete capabilities."""
    payment_title = "Book Fund" if payment_type == "book_fund" else "Educational Tour"
    
    subtab1, subtab2, subtab3, subtab4 = st.tabs([f"Add {payment_title} Payment", "Edit Payment", "Delete Payment", "View Payments"])
    
    with subtab1:
        show_add_payment_form(payment_type)
    
    with subtab2:
        show_edit_payment_interface(payment_type)
    
    with subtab3:
        show_delete_payment_interface(payment_type)
    
    with subtab4:
        show_payments_list(payment_type)

def show_add_payment_form(payment_type):
    """Show the add payment form."""
    students_df = load_students_from_db()
    
    if students_df.empty:
        st.warning("No students found. Please add students first.")
        return
    
    payment_title = "Book Fund" if payment_type == "book_fund" else "Educational Tour"
    st.subheader(f"Add {payment_title} Payment")
    
    with st.form(f"add_{payment_type}_payment_form_enhanced"): # Changed form key
        # Student selection
        student_options = [f"{row['reg_number']} - {row['first_name']} {row['last_name']} ({row['class_code']})" 
                          for _, row in students_df.iterrows()]
        selected_student = st.selectbox("Select Student", student_options)
        
        if selected_student:
            selected_reg = selected_student.split(" - ")[0]
            student_data = students_df[students_df['reg_number'] == selected_reg].iloc[0]
            
            # Show current payment status
            if payment_type == "book_fund":
                total_paid = get_total_paid_from_db(selected_reg, "book_fund")
                outstanding = max(0, book_fund_target - total_paid)
                st.info(f"Current Status: Paid KES {total_paid:,.2f} | Outstanding: KES {outstanding:,.2f}")
                
                if outstanding == 0:
                    st.success("✅ Book Fund payment is complete!")
            else:
                tour_group, tour_details = get_student_tour_group(student_data['class_code'])
                if tour_group:
                    total_paid = get_total_paid_from_db(selected_reg, "tour")
                    required = tour_details['amount']
                    outstanding = max(0, required - total_paid)
                    st.info(f"Tour Group: {tour_group}")
                    st.info(f"Destination: {tour_details['destination']}")
                    st.info(f"Current Status: Paid KES {total_paid:,.2f} | Outstanding: KES {outstanding:,.2f}")
                    
                    if outstanding == 0:
                        st.success("✅ Tour  payment is complete!")
                else:
                    st.warning("No tour group found for this student's class.")
            
            # Payment form
            col1, col2 = st.columns(2)
            with col1:
                amount = st.number_input("Amount (KES)", min_value=1.0, step=1.0)
            with col2:
                mpesa_ref = st.text_input("MPESA Reference")
            
            # Check for duplicates
            submitted = st.form_submit_button(f"Add {payment_title} Payment")
            
            if submitted and amount > 0:
                # Check for potential duplicates
                has_duplicates, duplicates = check_duplicate_payments(selected_reg, amount, payment_type)
                
                if has_duplicates:
                    st.warning("⚠️ Potential duplicate payment detected!")
                    st.write("Recent similar payments:")
                    for dup in duplicates:
                        st.write(f"Amount: KES {dup[5]:,.2f}, Date: {dup[1]}, Ref: {dup[6]}")
                    
                    confirm = st.checkbox("I confirm this is not a duplicate payment")
                    if not confirm:
                        st.stop()
                
                # Prepare payment data
                payment_data = {
                    'reg_number': selected_reg,
                    'full_name': f"{student_data['first_name']} {student_data['last_name']}",
                    'class': student_data['class_code'],
                    'amount': amount,
                    'mpesa_ref': mpesa_ref or 'N/A'
                }
                
                if payment_type == "tour":
                    tour_group, tour_details = get_student_tour_group(student_data['class_code'])
                    payment_data['tour_group'] = tour_group
                    payment_data['destination'] = tour_details['destination'] if tour_details else 'N/A'
                elif payment_type == "book_fund":
                    current_total = get_total_paid_from_db(selected_reg, "book_fund")
                    payment_data['outstanding'] = max(0, book_fund_target - (current_total + amount))
                
                # Save payment
                success, message = save_payment_to_db(payment_data, payment_type)
                
                if success:
                    st.success(message)
                    
                    # Generate receipt
                    pdf, payment_code = create_printable_receipt_pdf(payment_data, payment_type)
                    pdf_content = pdf.output(dest='S').encode('latin-1')
                    
                    st.markdown(create_printable_pdf_link(pdf_content), unsafe_allow_html=True)
                    
                    st.cache_data.clear()
                else:
                    st.error(message)

def show_edit_payment_interface(payment_type):
    """Show interface for editing payments."""
    payment_title = "Book Fund" if payment_type == "book_fund" else "Tour"
    st.subheader(f"Edit {payment_title} Payment")
    
    payments_df = get_payments_from_db(payment_type)
    
    if payments_df.empty:
        st.info(f"No {payment_title.lower()} payments found.")
        return
    
    # Payment selection
    payment_options = []
    for _, payment in payments_df.iterrows():
        date_str = pd.to_datetime(payment['date']).strftime('%Y-%m-%d')
        payment_options.append(f"ID: {payment['id']} | {payment['full_name']} | KES {payment['amount']:,.2f} | {date_str}")
    
    selected_payment = st.selectbox("Select Payment to Edit", payment_options)
    
    if selected_payment:
        payment_id = int(selected_payment.split(" | ")[0].replace("ID: ", ""))
        payment_data = get_payment_by_id(payment_id, payment_type)
        
        if payment_data:
            with st.form(f"edit_payment_form_{payment_type}"): # Changed form key
                st.info(f"Editing payment for: {payment_data['full_name']} ({payment_data['reg_number']})")
                
                col1, col2 = st.columns(2)
                with col1:
                    new_amount = st.number_input("Amount (KES)", value=float(payment_data['amount']), min_value=1.0)
                with col2:
                    new_mpesa_ref = st.text_input("MPESA Reference", value=payment_data.get('mpesa_ref', ''))
                
                submitted = st.form_submit_button("Update Payment")
                
                if submitted:
                    success, message = edit_payment_in_db(payment_id, new_amount, new_mpesa_ref, payment_type)
                    if success:
                        st.success(message)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(message)

def show_delete_payment_interface(payment_type):
    """Show interface for deleting payments."""
    st.subheader(f"Delete {payment_type.replace('_', ' ').title()} Payment")
    st.warning("⚠️ Warning: Deleted payments will be backed up but removed from active records!")
    
    payments_df = get_payments_from_db(payment_type)
    
    if payments_df.empty:
        st.info(f"No {payment_type.replace('_', ' ').lower()} payments found.")
        return
    
    # Payment selection
    payment_options = []
    for _, payment in payments_df.iterrows():
        date_str = pd.to_datetime(payment['date']).strftime('%Y-%m-%d')
        payment_options.append(f"ID: {payment['id']} | {payment['full_name']} | KES {payment['amount']:,.2f} | {date_str}")
    
    selected_payment = st.selectbox("Select Payment to Delete", payment_options, key=f"delete_payment_select_{payment_type}") # Changed key
    
    if selected_payment:
        payment_id = int(selected_payment.split(" | ")[0].replace("ID: ", ""))
        payment_data = get_payment_by_id(payment_id, payment_type)
        
        if payment_data:
            st.error(f"⚠️ You are about to delete:")
            st.write(f"- Student: {payment_data['full_name']} ({payment_data['reg_number']})")
            st.write(f"- Amount: KES {payment_data['amount']:,.2f}")
            st.write(f"- Date: {payment_data['date']}")
            st.write(f"- Reference: {payment_data.get('mpesa_ref', 'N/A')}")
            
            # Added a unique key for the text_input to resolve the error
            reason = st.text_input("Reason for deletion (required)", placeholder="e.g., Duplicate payment, Error in entry", key=f"delete_reason_{payment_type}")
            confirm = st.checkbox("I confirm I want to delete this payment", key=f"confirm_delete_{payment_type}")
            
            if st.button("Delete Payment", disabled=not (confirm and reason.strip()), key=f"delete_button_{payment_type}"):
                success, message = delete_payment_from_db(payment_id, payment_type, reason.strip())
                if success:
                    st.success(message)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(message)

def show_payments_list(payment_type):
    """Show payments list with advanced filtering."""
    payment_title = "Book Fund" if payment_type == "book_fund" else "Tour"
    st.subheader(f"{payment_title} Payments")
    
    payments_df = get_payments_from_db(payment_type)
    
    if payments_df.empty:
        st.info(f"No {payment_title.lower()} payments found.")
        return
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Class filter
        classes = ["All Classes"] + sorted(payments_df['class_code'].unique().tolist())
        selected_class = st.selectbox("Filter by Class", classes, key=f"class_filter_{payment_type}") # Changed key
    
    with col2:
        # Date range filter
        date_filter = st.selectbox("Date Range", ["All Time", "Today", "This Week", "This Month", "Custom"], key=f"date_filter_{payment_type}") # Changed key
    
    with col3:
        # Amount filter
        amount_filter = st.selectbox("Amount Filter", ["All Amounts", "Above 1000", "Above 2000", "Custom"], key=f"amount_filter_{payment_type}") # Changed key
    
    # Apply filters
    filtered_df = payments_df.copy()
    
    if selected_class != "All Classes":
        filtered_df = filtered_df[filtered_df['class_code'] == selected_class]
    
    # Date filtering
    if date_filter != "All Time":
        filtered_df['date'] = pd.to_datetime(filtered_df['date'])
        today = pd.Timestamp.now().normalize()
        
        if date_filter == "Today":
            filtered_df = filtered_df[filtered_df['date'].dt.date == today.date()]
        elif date_filter == "This Week":
            week_start = today - pd.Timedelta(days=today.weekday())
            filtered_df = filtered_df[filtered_df['date'] >= week_start]
        elif date_filter == "This Month":
            month_start = today.replace(day=1)
            filtered_df = filtered_df[filtered_df['date'] >= month_start]
        elif date_filter == "Custom":
            col_start, col_end = st.columns(2)
            with col_start:
                start_date = st.date_input("From Date", key=f"start_date_{payment_type}") # Changed key
            with col_end:
                end_date = st.date_input("To Date", key=f"end_date_{payment_type}") # Changed key
            
            if start_date and end_date:
                filtered_df = filtered_df[
                    (filtered_df['date'].dt.date >= start_date) & 
                    (filtered_df['date'].dt.date <= end_date)
                ]
    
    # Amount filtering
    if amount_filter == "Above 1000":
        filtered_df = filtered_df[filtered_df['amount'] > 1000]
    elif amount_filter == "Above 2000":
        filtered_df = filtered_df[filtered_df['amount'] > 2000]
    elif amount_filter == "Custom":
        min_amount = st.number_input("Minimum Amount", min_value=0.0, key=f"min_amount_{payment_type}") # Changed key
        max_amount = st.number_input("Maximum Amount", min_value=0.0, key=f"max_amount_{payment_type}") # Changed key
        if min_amount or max_amount:
            if min_amount:
                filtered_df = filtered_df[filtered_df['amount'] >= min_amount]
            if max_amount:
                filtered_df = filtered_df[filtered_df['amount'] <= max_amount]
    
    # Display results
    if not filtered_df.empty:
        st.dataframe(filtered_df, use_container_width=True)
        
        # Summary statistics
        total_amount = filtered_df['amount'].sum()
        avg_amount = filtered_df['amount'].mean()
        count = len(filtered_df)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Payments", f"KES {total_amount:,.2f}")
        with col2:
            st.metric("Average Payment", f"KES {avg_amount:,.2f}")
        with col3:
            st.metric("Number of Payments", count)
        
        # Export functionality
        if st.button(f"📥 Download {payment_title} Payments Report", key=f"download_payments_{payment_type}"): # Changed key
            pdf_content = create_payment_list_pdf(filtered_df, f"{payment_title} Payments Report")
            st.download_button(
                label="Download PDF Report",
                data=pdf_content,
                file_name=f"{payment_title.lower()}_payments_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf"
            )
    else:
        st.info("No payments match the selected filters.")

def show_analytics_dashboard():
    """Enhanced analytics dashboard."""
    st.header("📊 Analytics Dashboard")
    
    # Load data
    students_df = load_students_from_db()
    book_payments_df = get_payments_from_db("book_fund")
    tour_payments_df = get_payments_from_db("tour")
    
    if students_df.empty:
        st.warning("No data available. Please add students and payments first.")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Students", len(students_df))
    
    with col2:
        book_total = book_payments_df['amount'].sum() if not book_payments_df.empty else 0
        st.metric("Book Fund Collected", f"KES {book_total:,.0f}")
    
    with col3:
        tour_total = tour_payments_df['amount'].sum() if not tour_payments_df.empty else 0
        st.metric("Tour Payments Collected", f"KES {tour_total:,.0f}")
    
    with col4:
        grand_total = book_total + tour_total
        st.metric("Total Collections", f"KES {grand_total:,.0f}")
    
    # Charts
    tab1, tab2, tab3, tab4 = st.tabs(["Payment Trends", "Class Analysis", "Payment Status", "Collection Goals"])
    
    with tab1:
        st.subheader("Payment Trends Over Time")
        
        if not book_payments_df.empty or not tour_payments_df.empty:
            # Combine payment data
            all_payments = []
            
            if not book_payments_df.empty:
                book_copy = book_payments_df.copy()
                book_copy['payment_type'] = 'Book Fund'
                all_payments.append(book_copy[['date', 'amount', 'payment_type']])
            
            if not tour_payments_df.empty:
                tour_copy = tour_payments_df.copy()
                tour_copy['payment_type'] = 'Tour'
                all_payments.append(tour_copy[['date', 'amount', 'payment_type']])
            
            if all_payments:
                combined_df = pd.concat(all_payments, ignore_index=True)
                combined_df['date'] = pd.to_datetime(combined_df['date'])
                combined_df['month'] = combined_df['date'].dt.to_period('M')
                
                monthly_summary = combined_df.groupby(['month', 'payment_type'])['amount'].sum().reset_index()
                monthly_summary['month'] = monthly_summary['month'].astype(str)
                
                fig = px.line(monthly_summary, x='month', y='amount', color='payment_type',
                             title='Monthly Payment Collections',
                             labels={'amount': 'Amount (KES)', 'month': 'Month'})
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No payment data available for trend analysis.")
    
    with tab2:
        st.subheader("Analysis by Class")
        
        # Students by class
        class_distribution = students_df['class_code'].value_counts()
        fig1 = px.bar(x=class_distribution.index, y=class_distribution.values,
                     title='Students by Class',
                     labels={'x': 'Class', 'y': 'Number of Students'})
        st.plotly_chart(fig1, use_container_width=True)
        
        # Payments by class
        if not book_payments_df.empty:
            class_payments = book_payments_df.groupby('class_code')['amount'].sum().sort_values(ascending=False)
            fig2 = px.bar(x=class_payments.index, y=class_payments.values,
                         title='Book Fund Collections by Class',
                         labels={'x': 'Class', 'y': 'Amount (KES)'})
            st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        st.subheader("Payment Status Overview")
        
        if not students_df.empty:
            payment_status = []
            
            for _, student in students_df.iterrows():
                reg_number = student['reg_number']
                book_paid = get_total_paid_from_db(reg_number, "book_fund")
                tour_paid = get_total_paid_from_db(reg_number, "tour")
                
                # Book fund status
                book_status = "Complete" if book_paid >= book_fund_target else "Incomplete"
                book_outstanding = max(0, book_fund_target - book_paid)
                
                # Tour status
                tour_group, tour_details = get_student_tour_group(student['class_code'])
                if tour_group and tour_details:
                    tour_required = tour_details['amount']
                    tour_status = "Complete" if tour_paid >= tour_required else "Incomplete"
                    tour_outstanding = max(0, tour_required - tour_paid)
                else:
                    tour_status = "N/A"
                    tour_outstanding = 0
                
                payment_status.append({
                    'reg_number': reg_number,
                    'name': f"{student['first_name']} {student['last_name']}",
                    'class': student['class_code'],
                    'book_paid': book_paid,
                    'book_status': book_status,
                    'book_outstanding': book_outstanding,
                    'tour_paid': tour_paid,
                    'tour_status': tour_status,
                    'tour_outstanding': tour_outstanding
                })
            
            status_df = pd.DataFrame(payment_status)
            
            # Summary cards
            col1, col2 = st.columns(2)
            
            with col1:
                book_complete = len(status_df[status_df['book_status'] == 'Complete'])
                book_incomplete = len(status_df[status_df['book_status'] == 'Incomplete'])
                
                st.write("**Book Fund Status:**")
                st.success(f"✅ Complete: {book_complete} students")
                st.warning(f"⏳ Incomplete: {book_incomplete} students")
            
            with col2:
                tour_complete = len(status_df[status_df['tour_status'] == 'Complete'])
                tour_incomplete = len(status_df[status_df['tour_status'] == 'Incomplete'])
                tour_na = len(status_df[status_df['tour_status'] == 'N/A'])
                
                st.write("**Tour Payment Status:**")
                st.success(f"✅ Complete: {tour_complete} students")
                st.warning(f"⏳ Incomplete: {tour_incomplete} students")
                st.info(f"ℹ️ Not Applicable: {tour_na} students")
            
            # Outstanding payments
            outstanding_book = status_df[status_df['book_outstanding'] > 0]
            outstanding_tour = status_df[status_df['tour_outstanding'] > 0]
            
            if not outstanding_book.empty:
                st.subheader("Outstanding Book Fund Payments")
                st.dataframe(outstanding_book[['name', 'class', 'book_paid', 'book_outstanding']], use_container_width=True)
            
            if not outstanding_tour.empty:
                st.subheader("Outstanding Tour Payments")
                st.dataframe(outstanding_tour[['name', 'class', 'tour_paid', 'tour_outstanding']], use_container_width=True)
    
    with tab4:
        st.subheader("Collection Goals")
        
        # Book fund progress
        total_students = len(students_df)
        book_fund_goal = total_students * book_fund_target
        book_collected = book_payments_df['amount'].sum() if not book_payments_df.empty else 0
        book_progress = (book_collected / book_fund_goal) * 100 if book_fund_goal > 0 else 0
        
        st.write("**Book Fund Collection Goal**")
        st.progress(min(book_progress / 100, 1.0))
        st.write(f"Collected: KES {book_collected:,.0f} / KES {book_fund_goal:,.0f} ({book_progress:.1f}%)")
        
        # Tour payments by group
        st.write("**Tour Payments by Group**")
        for group, details in TOUR_GROUPS.items():
            group_students_df = students_df[students_df['class_code'].isin(details['classes'])]
            total_students_in_group = len(group_students_df)
            
            if total_students_in_group > 0:
                group_tour_goal = total_students_in_group * details['amount']
                group_collected = tour_payments_df[
                    tour_payments_df['class_code'].isin(details['classes'])
                ]['amount'].sum() if not tour_payments_df.empty else 0
                
                group_progress = (group_collected / group_tour_goal) * 100 if group_tour_goal > 0 else 0
                
                st.write(f"**{group} - {details['destination']}**")
                st.progress(min(group_progress / 100, 1.0))
                st.write(f"Collected: KES {group_collected:,.0f} / KES {group_tour_goal:,.0f} ({group_progress:.1f}%)")
            else:
                st.write(f"**{group}:** No students in this group.")


# ------------- LOAD DATA -------------
# Moved this section to here to ensure data is loaded before it's used by any widgets.
students_df = load_students_from_db()
if not students_df.empty:
    students_df["Display"] = students_df["first_name"] + " " + students_df["last_name"] + " (" + students_df["reg_number"].astype(str) + ")"
    student_display_list = students_df["Display"].tolist()
else:
    student_display_list = []

# Initialize page variable with a default value to prevent NameError
page = "📚 Book Fund Payments" # Set a default that is one of the valid options

# ------------- MAIN APP -------------
st.sidebar.title("🏫 Success Achievers")
if os.path.exists(logo_file):
    st.sidebar.image(logo_file, width=150)

# This will then update the 'page' variable based on user selection
page = st.sidebar.selectbox("Navigate to:", [
    "📚 Book Fund Payments",
    "🚌 Educational Tours",
    "📊 Financial Reports",
    "👥 Student Management",
    "💰 Payment Management" # Added new page for centralized payment management
])


# The rest of the if/elif blocks will now safely access 'page'
if page == "📚 Book Fund Payments":
    # The original "Book Fund Payments" section
    st.title("📚 Book Fund Payment System")
    st.markdown("*Secure payment processing with instant PDF receipts*")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        selected_display = st.selectbox("Search and select student:", options=["Select"] + student_display_list)
        
        if selected_display != "Select":
            reg_number = selected_display.split("(")[-1].replace(")", "").strip()
            matched = students_df[students_df["reg_number"].astype(str) == reg_number]
            
            if not matched.empty:
                student = matched.iloc[0]
                full_name = f"{student['first_name']} {student['last_name']}"
                class_code = student["class_code"]
                total_paid = get_total_paid_from_db(reg_number, "book_fund")
                outstanding_balance = max(0, book_fund_target - total_paid)
                
                with st.form("book_payment_form"):
                    st.subheader("💳 Payment Details")
                    st.write(f"**Name:** {full_name}")
                    st.write(f"**Class:** {class_code}")
                    st.write(f"**Target Amount:** KES {book_fund_target:,.0f}")
                    st.write(f"**Already Paid:** KES {total_paid:,.0f}")
                    st.write(f"**Outstanding Balance:** KES {outstanding_balance:,.0f}")
                    
                    amount_paid = st.number_input("Enter Amount Paid (KES):", min_value=0.0, step=50.0, value=float(min(outstanding_balance, 500)))
                    mpesa_ref = st.text_input("MPESA Reference Code (optional)")
                    pay_button = st.form_submit_button("💾 Record Payment & Generate Receipt")
                
                if pay_button and amount_paid > 0:
                    payment_data = {
                        "reg_number": reg_number, "full_name": full_name, "class": class_code,
                        "amount": amount_paid, "outstanding": outstanding_balance - amount_paid, "mpesa_ref": mpesa_ref
                    }
                    success, message = save_payment_to_db(payment_data, "book_fund")
                    
                    if success:
                        pdf, payment_code = create_printable_receipt_pdf(payment_data, "book_fund")
                        pdf_content = pdf.output(dest='S').encode('latin-1')
                        st.success("✅ Payment recorded successfully!")
                        btn_col1, btn_col2 = st.columns(2)
                        with btn_col1:
                            st.markdown(create_printable_pdf_link(pdf_content), unsafe_allow_html=True)
                        with btn_col2:
                            st.download_button("📥 Download Receipt", data=pdf_content,
                                               file_name=f"book_fund_receipt_{reg_number}.pdf", mime="application/pdf")
                        st.cache_data.clear() # Clear cache to refresh data after payment
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")
            else:
                st.info("Please select a student to record a payment.")
    
    with col2:
        st.subheader("📈 Collection Summary")
        book_payments_df = get_payments_from_db("book_fund")
        if not book_payments_df.empty:
            total_collected = book_payments_df["amount"].sum()
            num_students = len(book_payments_df["reg_number"].unique())
            collection_rate = (total_collected / (book_fund_target * len(students_df)) * 100) if not students_df.empty else 0
            st.metric("Total Collected", f"KES {total_collected:,.0f}")
            st.metric("Students Paid", num_students)
            st.metric("Collection Rate", f"{collection_rate:.1f}%")
        else:
            st.info("No book fund payments recorded yet for summary.")


elif page == "🚌 Educational Tours":
    # The original "Educational Tours" section
    st.title("🚌 Educational Tours 2025")
    st.markdown("*Term 2 Educational Tours - Enriching Learning Through Experience*")
    
    st.subheader("📅 Tour Schedule Overview")
    cols = st.columns(2)
    today = date.today()
    for i, (group, details) in enumerate(TOUR_GROUPS.items()):
        col = cols[i % 2]
        deadline_date = datetime.strptime(details["deadline"], "%Y-%m-%d").date()
        status = "🟢 Open" if today <= deadline_date else "🔴 Closed"
        with col:
            with st.expander(f"{group}: {', '.join(details['classes'])} - {status}"):
                st.write(f"**📍 Destination:** {details['destination']}")
                st.write(f"**📅 Tour Date:** {details['date']}")
                st.write(f"**💰 Amount:** KES {details['amount']:,.0f}")

    st.subheader("💳 Tour Payment")
    selected_display = st.selectbox("Select student for tour payment:", options=["Select"] + student_display_list)
    
    if selected_display != "Select":
        reg_number = selected_display.split("(")[-1].replace(")", "").strip()
        matched = students_df[students_df["reg_number"].astype(str) == reg_number]
        
        if not matched.empty:
            student = matched.iloc[0]
            full_name = f"{student['first_name']} {student['last_name']}"
            class_code = student["class_code"]
            tour_group, tour_details = get_student_tour_group(class_code)
            
            if tour_group:
                total_paid = get_total_paid_from_db(reg_number, "tour")
                outstanding = max(0, tour_details["amount"] - total_paid)
                with st.form("tour_payment_form"):
                    st.write(f"**Student:** {full_name} ({class_code}) | **Tour Group:** {tour_group}")
                    st.write(f"**Total Fee:** KES {tour_details['amount']:,.0f} | **Paid:** KES {total_paid:,.0f} | **Balance:** KES {outstanding:,.0f}")
                    
                    amount_paid = st.number_input("Payment Amount (KES):", min_value=0.0, step=100.0, value=float(min(outstanding, tour_details["amount"])))
                    mpesa_ref = st.text_input("MPESA Reference (optional)")
                    pay_button = st.form_submit_button("💾 Record Tour Payment")
                
                if pay_button and amount_paid > 0:
                    payment_data = {
                        "reg_number": reg_number, "full_name": full_name, "class": class_code, "tour_group": tour_group,
                        "tour_date": tour_details["date"], "destination": tour_details["destination"],
                        "amount": amount_paid, "mpesa_ref": mpesa_ref
                    }
                    success, message = save_payment_to_db(payment_data, "tour")
                    
                    if success:
                        pdf, payment_code = create_printable_receipt_pdf(payment_data, "tour")
                        pdf_content = pdf.output(dest='S').encode('latin-1')
                        st.success("✅ Tour payment recorded successfully!")
                        btn_col1, btn_col2 = st.columns(2)
                        with btn_col1:
                            st.markdown(create_printable_pdf_link(pdf_content), unsafe_allow_html=True)
                        with btn_col2:
                            st.download_button("📥 Download Tour Receipt", data=pdf_content, 
                                               file_name=f"tour_receipt_{reg_number}.pdf", mime="application/pdf")
                        st.cache_data.clear() # Clear cache to refresh data after payment
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")
            else:
                st.warning(f"⚠️ No tour group found for class {class_code}.")
        else:
            st.info("Please select a student to record a payment.")


elif page == "📊 Financial Reports":
    # The original "Financial Reports" section
    st.title("📊 Financial Reports & Analytics")
    st.markdown("*Comprehensive financial overview and payment tracking*")
    
    report_type = st.selectbox("Select Report Type:", ["Book Fund Summary", "Tour Payments Summary", "Class-wise Analysis", "Payment History"])
    
    if report_type == "Book Fund Summary":
        st.subheader("📚 Book Fund Collection Report")
        book_payments_df = get_payments_from_db("book_fund")
        if not book_payments_df.empty:
            class_filter = st.selectbox("Filter by Class:", ["All Classes"] + sorted(book_payments_df["class_code"].unique().tolist()), key="book_fund_class_filter") # Added key
            filtered_df = get_payments_from_db("book_fund", class_filter)
            st.dataframe(filtered_df[["date", "full_name", "reg_number", "class_code", "amount", "mpesa_ref"]], use_container_width=True)
            if st.button("📥 Generate Book Fund Report PDF", key="download_book_fund_report"): # Added key
                pdf_content = create_payment_list_pdf(filtered_df, f"Book Fund Payments - {class_filter}")
                st.download_button("📥 Download PDF Report", data=pdf_content, 
                                   file_name=f"book_fund_report_{class_filter}.pdf", mime="application/pdf")
        else:
            st.info("No book fund payments recorded yet.")

    elif report_type == "Tour Payments Summary":
        st.subheader("🚌 Educational Tours Payment Report")
        tour_payments_df = get_payments_from_db("tour")
        if not tour_payments_df.empty:
            group_filter = st.selectbox("Filter by Tour Group:", ["All Groups"] + sorted(tour_payments_df["tour_group"].unique().tolist()), key="tour_group_filter") # Added key
            if group_filter != "All Groups":
                filtered_df = tour_payments_df[tour_payments_df["tour_group"] == group_filter]
            else:
                filtered_df = tour_payments_df
            st.dataframe(filtered_df[["date", "full_name", "reg_number", "class_code", "tour_group", "amount", "mpesa_ref"]], use_container_width=True)
            if st.button("📥 Generate Tour Report PDF", key="download_tour_report"): # Added key
                pdf_content = create_payment_list_pdf(filtered_df, f"Tour Payments Report - {group_filter}")
                st.download_button("📥 Download PDF Report", data=pdf_content,
                                   file_name=f"tour_payments_report_{group_filter}.pdf", mime="application/pdf")
        else:
            st.info("No tour payments recorded yet.")

    elif report_type == "Class-wise Analysis":
        st.subheader("📊 Class-wise Financial Analysis")
        if not students_df.empty:
            class_analysis = []
            for class_code in sorted(students_df["class_code"].unique()):
                book_total = get_payments_from_db("book_fund", class_code)["amount"].sum()
                tour_total = get_payments_from_db("tour", class_code)["amount"].sum()
                class_analysis.append({"Class": class_code, "Book Fund": book_total, "Tour Payments": tour_total, "Total": book_total + tour_total})
            analysis_df = pd.DataFrame(class_analysis)
            st.dataframe(analysis_df, use_container_width=True)
            fig = px.bar(analysis_df, x="Class", y=["Book Fund", "Tour Payments"], title="Collections by Class")
            st.plotly_chart(fig, use_container_width=True)
    
    elif report_type == "Payment History":
        st.subheader("📈 Payment History & Trends")
        book_payments = get_payments_from_db("book_fund")
        tour_payments = get_payments_from_db("tour")
        if not book_payments.empty or not tour_payments.empty:
            book_payments["type"] = "Book Fund"
            tour_payments["type"] = "Tour"
            combined = pd.concat([book_payments, tour_payments])
            combined["date"] = pd.to_datetime(combined["date"])
            daily_summary = combined.resample('D', on='date')['amount'].sum().reset_index()
            fig = px.line(daily_summary, x="date", y="amount", title="Daily Payment Trends")
            st.plotly_chart(fig, use_container_width=True)

# ------------- STUDENT MANAGEMENT (CALLS ENHANCED FUNCTION) -------------
elif page == "👥 Student Management":
    show_student_management()
    
# ------------- CENTRALIZED PAYMENT MANAGEMENT (NEW PAGE) -------------
elif page == "💰 Payment Management":
    show_payment_management()

# ------------- FOOTER -------------
st.sidebar.markdown("---")
st.sidebar.markdown("Made with ❤️ by Success Achievers Academy")
st.sidebar.markdown("📞 0794 451 007")
st.sidebar.markdown("✉️ info@successachievers.ac.ke")

# Clean up temp file
if os.path.exists(qr_temp_file):
    try:
        os.remove(qr_temp_file)
    except Exception:
        pass
