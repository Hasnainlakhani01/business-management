from datetime import datetime, date
import sqlite3
import logging
from decimal import Decimal

class Sale:
    def __init__(self, db):
        self.db = db
    
    def get_all(self, limit=None, offset=None):
        """Get all sales with customer details."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT s.*, c.name as customer_name,
                       (s.amount - s.received_amount) as outstanding,
                       CASE
                           WHEN s.received_amount >= s.amount THEN 'Paid'
                           WHEN s.received_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM sales s
                JOIN customers c ON s.customer_id = c.id
                ORDER BY s.date DESC, s.created_at DESC
            '''
            
            params = ()
            if limit:
                query += ' LIMIT ?'
                params = (limit,)
                if offset:
                    query += ' OFFSET ?'
                    params = (limit, offset)
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching sales: {e}")
            return []
    
    def get_by_id(self, id):
        """Get sale by ID with customer details."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT s.*, c.name as customer_name, c.contact as customer_contact,
                       (s.amount - s.received_amount) as outstanding,
                       CASE
                           WHEN s.received_amount >= s.amount THEN 'Paid'
                           WHEN s.received_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM sales s
                JOIN customers c ON s.customer_id = c.id
                WHERE s.id = ?
            ''', (id,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching sale {id}: {e}")
            return None
    
    def get_by_customer(self, customer_id, limit=None):
        """Get sales for a specific customer."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT s.*, c.name as customer_name,
                       (s.amount - s.received_amount) as outstanding,
                       CASE
                           WHEN s.received_amount >= s.amount THEN 'Paid'
                           WHEN s.received_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM sales s
                JOIN customers c ON s.customer_id = c.id
                WHERE s.customer_id = ?
                ORDER BY s.date DESC
            '''
            
            params = (customer_id,)
            if limit:
                query += ' LIMIT ?'
                params = (customer_id, limit)
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching sales for customer {customer_id}: {e}")
            return []
    
    def get_by_date_range(self, start_date, end_date):
        """Get sales within a date range."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT s.*, c.name as customer_name,
                       (s.amount - s.received_amount) as outstanding,
                       CASE
                           WHEN s.received_amount >= s.amount THEN 'Paid'
                           WHEN s.received_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM sales s
                JOIN customers c ON s.customer_id = c.id
                WHERE s.date BETWEEN ? AND ?
                ORDER BY s.date DESC
            ''', (start_date, end_date))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching sales by date range: {e}")
            return []
    
    def create(self, date, customer_id, amount, invoice_no=None, received_amount=0.00, items=None, notes=None):
        """Create a new sale."""
        try:
            # Validate inputs
            if not isinstance(date, (str, date)):
                raise ValueError("Date must be a string or date object")
            if amount <= 0:
                raise ValueError("Amount must be positive")
            if received_amount < 0:
                raise ValueError("Received amount cannot be negative")
            if received_amount > amount:
                raise ValueError("Received amount cannot exceed total amount")
            
            cursor = self.db.get_db().cursor()
            
            # Check if customer exists
            cursor.execute('SELECT id FROM customers WHERE id = ?', (customer_id,))
            if not cursor.fetchone():
                raise ValueError(f"Customer with ID {customer_id} does not exist")
            
            cursor.execute('''
                INSERT INTO sales (date, customer_id, invoice_no, amount, received_amount, items, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, customer_id, invoice_no, amount, received_amount, items, notes))
            
            self.db.get_db().commit()
            sale_id = cursor.lastrowid
            logging.info(f"Created sale ID: {sale_id} for customer {customer_id}")
            return sale_id
            
        except sqlite3.Error as e:
            logging.error(f"Error creating sale: {e}")
            self.db.get_db().rollback()
            raise
    
    def update(self, id, date=None, customer_id=None, invoice_no=None, amount=None, received_amount=None, items=None, notes=None):
        """Update sale information."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get current sale data
            current = self.get_by_id(id)
            if not current:
                raise ValueError(f"Sale with ID {id} does not exist")
            
            # Build dynamic update query
            updates = []
            params = []
            
            if date is not None:
                updates.append('date = ?')
                params.append(date)
            if customer_id is not None:
                # Check if new customer exists
                cursor.execute('SELECT id FROM customers WHERE id = ?', (customer_id,))
                if not cursor.fetchone():
                    raise ValueError(f"Customer with ID {customer_id} does not exist")
                updates.append('customer_id = ?')
                params.append(customer_id)
            if invoice_no is not None:
                updates.append('invoice_no = ?')
                params.append(invoice_no)
            if amount is not None:
                if amount <= 0:
                    raise ValueError("Amount must be positive")
                updates.append('amount = ?')
                params.append(amount)
            if received_amount is not None:
                if received_amount < 0:
                    raise ValueError("Received amount cannot be negative")
                final_amount = amount if amount is not None else current['amount']
                if received_amount > final_amount:
                    raise ValueError("Received amount cannot exceed total amount")
                updates.append('received_amount = ?')
                params.append(received_amount)
            if items is not None:
                updates.append('items = ?')
                params.append(items)
            if notes is not None:
                updates.append('notes = ?')
                params.append(notes)
            
            if not updates:
                return False
            
            params.append(id)
            query = f"UPDATE sales SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, params)
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Updated sale ID: {id}")
                return True
            else:
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error updating sale: {e}")
            self.db.get_db().rollback()
            raise
    
    def delete(self, id):
        """Delete a sale (only if no associated receipts)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Check if sale has any receipts
            cursor.execute('SELECT COUNT(*) as receipt_count FROM receipts WHERE sale_id = ?', (id,))
            receipt_count = cursor.fetchone()['receipt_count']
            
            if receipt_count > 0:
                raise ValueError("Cannot delete sale with associated receipts")
            
            cursor.execute('DELETE FROM sales WHERE id = ?', (id,))
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Deleted sale ID: {id}")
                return True
            else:
                logging.warning(f"No sale found with ID: {id}")
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error deleting sale: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_outstanding(self):
        """Get all sales with outstanding amounts."""
        return self.db.get_outstanding_sales()
    
    def get_receipts(self, id):
        """Get all receipts made against this sale."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT r.*, c.name as customer_name
                FROM receipts r
                JOIN customers c ON r.customer_id = c.id
                WHERE r.sale_id = ?
                ORDER BY r.date DESC
            ''', (id,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching receipts for sale {id}: {e}")
            return []
    
    def add_receipt(self, id, amount):
        """Add receipt amount to this sale (updates received_amount)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get current sale
            sale = self.get_by_id(id)
            if not sale:
                raise ValueError(f"Sale with ID {id} does not exist")
            
            new_received_amount = float(sale['received_amount']) + float(amount)
            if new_received_amount > float(sale['amount']):
                raise ValueError("Receipt amount exceeds outstanding balance")
            
            cursor.execute('''
                UPDATE sales SET received_amount = ? WHERE id = ?
            ''', (new_received_amount, id))
            
            self.db.get_db().commit()
            logging.info(f"Added receipt of {amount} to sale {id}")
            return True
            
        except sqlite3.Error as e:
            logging.error(f"Error adding receipt to sale: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_summary_stats(self, start_date=None, end_date=None):
        """Get summary statistics for sales."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT 
                    COUNT(*) as total_sales,
                    COALESCE(SUM(amount), 0) as total_amount,
                    COALESCE(SUM(received_amount), 0) as total_received,
                    COALESCE(SUM(amount - received_amount), 0) as total_outstanding,
                    COUNT(CASE WHEN received_amount >= amount THEN 1 END) as paid_sales,
                    COUNT(CASE WHEN received_amount > 0 AND received_amount < amount THEN 1 END) as partial_sales,
                    COUNT(CASE WHEN received_amount = 0 THEN 1 END) as unpaid_sales
                FROM sales
            '''
            
            params = ()
            if start_date and end_date:
                base_query += ' WHERE date BETWEEN ? AND ?'
                params = (start_date, end_date)
            elif start_date:
                base_query += ' WHERE date >= ?'
                params = (start_date,)
            elif end_date:
                base_query += ' WHERE date <= ?'
                params = (end_date,)
            
            cursor.execute(base_query, params)
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching sale summary: {e}")
            return None