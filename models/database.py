from flask import g
import sqlite3
from datetime import datetime
from decimal import Decimal
import logging

class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def get_db(self):
        if 'db' not in g:
            g.db = sqlite3.connect(self.db_path)
            g.db.row_factory = sqlite3.Row
            # Enable foreign key constraints
            g.db.execute('PRAGMA foreign_keys = ON')
        return g.db
    
    def close_db(self):
        db = g.pop('db', None)
        if db is not None:
            db.close()
    
    def init_db(self):
        """Initialize the database schema."""
        try:
            db = self.get_db()
            cursor = db.cursor()
            
            # Suppliers table
            cursor.execute('''CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                contact TEXT,
                address TEXT,
                balance DECIMAL(15,2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Customers table
            cursor.execute('''CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                contact TEXT,
                address TEXT,
                balance DECIMAL(15,2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Purchases table
            cursor.execute('''CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                supplier_id INTEGER NOT NULL,
                bill_no TEXT,
                amount DECIMAL(15,2) NOT NULL CHECK(amount >= 0),
                paid_amount DECIMAL(15,2) DEFAULT 0.00 CHECK(paid_amount >= 0),
                items TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (supplier_id) REFERENCES suppliers (id) ON DELETE RESTRICT
            )''')
            
            # Sales table
            cursor.execute('''CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                customer_id INTEGER NOT NULL,
                invoice_no TEXT,
                amount DECIMAL(15,2) NOT NULL CHECK(amount >= 0),
                received_amount DECIMAL(15,2) DEFAULT 0.00 CHECK(received_amount >= 0),
                items TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE RESTRICT
            )''')
            
            # Payments table (money out to suppliers)
            cursor.execute('''CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                supplier_id INTEGER NOT NULL,
                purchase_id INTEGER,
                amount DECIMAL(15,2) NOT NULL CHECK(amount > 0),
                payment_mode TEXT NOT NULL CHECK(payment_mode IN ('cash', 'bank', 'cheque', 'upi', 'card')),
                reference_no TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (supplier_id) REFERENCES suppliers (id) ON DELETE RESTRICT,
                FOREIGN KEY (purchase_id) REFERENCES purchases (id) ON DELETE SET NULL
            )''')
            
            # Receipts table (money in from customers)
            cursor.execute('''CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                customer_id INTEGER NOT NULL,
                sale_id INTEGER,
                amount DECIMAL(15,2) NOT NULL CHECK(amount > 0),
                payment_mode TEXT NOT NULL CHECK(payment_mode IN ('cash', 'bank', 'cheque', 'upi', 'card')),
                reference_no TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE RESTRICT,
                FOREIGN KEY (sale_id) REFERENCES sales (id) ON DELETE SET NULL
            )''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_purchases_supplier_id ON purchases(supplier_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_purchases_date ON purchases(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_customer_id ON sales(customer_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_supplier_id ON payments(supplier_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_receipts_customer_id ON receipts(customer_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_receipts_date ON receipts(date)')
            
            # Create triggers to update balances automatically
            cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_supplier_balance_on_purchase
                AFTER INSERT ON purchases
                BEGIN
                    UPDATE suppliers 
                    SET balance = balance + NEW.amount - NEW.paid_amount,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.supplier_id;
                END
            ''')
            
            cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_supplier_balance_on_payment
                AFTER INSERT ON payments
                BEGIN
                    UPDATE suppliers 
                    SET balance = balance - NEW.amount,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.supplier_id;
                END
            ''')
            
            cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_customer_balance_on_sale
                AFTER INSERT ON sales
                BEGIN
                    UPDATE customers 
                    SET balance = balance + NEW.amount - NEW.received_amount,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.customer_id;
                END
            ''')
            
            cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_customer_balance_on_receipt
                AFTER INSERT ON receipts
                BEGIN
                    UPDATE customers 
                    SET balance = balance - NEW.amount,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = NEW.customer_id;
                END
            ''')
            
            # Create triggers to update updated_at timestamps
            for table in ['suppliers', 'customers', 'purchases', 'sales']:
                cursor.execute(f'''
                    CREATE TRIGGER IF NOT EXISTS update_{table}_timestamp
                    AFTER UPDATE ON {table}
                    BEGIN
                        UPDATE {table} SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                    END
                ''')
            
            db.commit()
            logging.info("Database initialized successfully")
            
        except sqlite3.Error as e:
            logging.error(f"Database initialization error: {e}")
            if db:
                db.rollback()
            raise
    
    def get_supplier_balance(self, supplier_id):
        """Get current balance for a supplier"""
        db = self.get_db()
        result = db.execute(
            'SELECT balance FROM suppliers WHERE id = ?', 
            (supplier_id,)
        ).fetchone()
        return float(result['balance']) if result else 0.0
    
    def get_customer_balance(self, customer_id):
        """Get current balance for a customer"""
        db = self.get_db()
        result = db.execute(
            'SELECT balance FROM customers WHERE id = ?', 
            (customer_id,)
        ).fetchone()
        return float(result['balance']) if result else 0.0
    
    def get_outstanding_purchases(self, supplier_id=None):
        """Get purchases with outstanding amounts"""
        db = self.get_db()
        query = '''
            SELECT p.*, s.name as supplier_name,
                   (p.amount - p.paid_amount) as outstanding
            FROM purchases p
            JOIN suppliers s ON p.supplier_id = s.id
            WHERE p.amount > p.paid_amount
        '''
        params = ()
        
        if supplier_id:
            query += ' AND p.supplier_id = ?'
            params = (supplier_id,)
            
        query += ' ORDER BY p.date'
        
        return db.execute(query, params).fetchall()
    
    def get_outstanding_sales(self, customer_id=None):
        """Get sales with outstanding amounts"""
        db = self.get_db()
        query = '''
            SELECT s.*, c.name as customer_name,
                   (s.amount - s.received_amount) as outstanding
            FROM sales s
            JOIN customers c ON s.customer_id = c.id
            WHERE s.amount > s.received_amount
        '''
        params = ()
        
        if customer_id:
            query += ' AND s.customer_id = ?'
            params = (customer_id,)
            
        query += ' ORDER BY s.date'
        
        return db.execute(query, params).fetchall()