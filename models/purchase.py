from datetime import datetime, date
import sqlite3
import logging
from decimal import Decimal

class Purchase:
    def __init__(self, db):
        self.db = db
    
    def get_all(self, limit=None, offset=None):
        """Get all purchases with supplier details."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT p.*, s.name as supplier_name,
                       (p.amount - p.paid_amount) as outstanding,
                       CASE
                           WHEN p.paid_amount >= p.amount THEN 'Paid'
                           WHEN p.paid_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM purchases p
                JOIN suppliers s ON p.supplier_id = s.id
                ORDER BY p.date DESC, p.created_at DESC
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
            logging.error(f"Error fetching purchases: {e}")
            return []
    
    def get_by_id(self, id):
        """Get purchase by ID with supplier details."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT p.*, s.name as supplier_name, s.contact as supplier_contact,
                       (p.amount - p.paid_amount) as outstanding,
                       CASE
                           WHEN p.paid_amount >= p.amount THEN 'Paid'
                           WHEN p.paid_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM purchases p
                JOIN suppliers s ON p.supplier_id = s.id
                WHERE p.id = ?
            ''', (id,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching purchase {id}: {e}")
            return None
    
    def get_by_supplier(self, supplier_id, limit=None):
        """Get purchases for a specific supplier."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT p.*, s.name as supplier_name,
                       (p.amount - p.paid_amount) as outstanding,
                       CASE
                           WHEN p.paid_amount >= p.amount THEN 'Paid'
                           WHEN p.paid_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM purchases p
                JOIN suppliers s ON p.supplier_id = s.id
                WHERE p.supplier_id = ?
                ORDER BY p.date DESC
            '''
            
            params = (supplier_id,)
            if limit:
                query += ' LIMIT ?'
                params = (supplier_id, limit)
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching purchases for supplier {supplier_id}: {e}")
            return []
    
    def get_by_date_range(self, start_date, end_date):
        """Get purchases within a date range."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT p.*, s.name as supplier_name,
                       (p.amount - p.paid_amount) as outstanding,
                       CASE
                           WHEN p.paid_amount >= p.amount THEN 'Paid'
                           WHEN p.paid_amount > 0 THEN 'Partial'
                           ELSE 'Unpaid'
                       END as payment_status
                FROM purchases p
                JOIN suppliers s ON p.supplier_id = s.id
                WHERE p.date BETWEEN ? AND ?
                ORDER BY p.date DESC
            ''', (start_date, end_date))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching purchases by date range: {e}")
            return []
    
    def create(self, date, supplier_id, amount, bill_no=None, paid_amount=0.00, items=None, notes=None):
        """Create a new purchase."""
        try:
            # Validate inputs
            if not isinstance(date, (str, date)):
                raise ValueError("Date must be a string or date object")
            if amount <= 0:
                raise ValueError("Amount must be positive")
            if paid_amount < 0:
                raise ValueError("Paid amount cannot be negative")
            if paid_amount > amount:
                raise ValueError("Paid amount cannot exceed total amount")
            
            cursor = self.db.get_db().cursor()
            
            # Check if supplier exists
            cursor.execute('SELECT id FROM suppliers WHERE id = ?', (supplier_id,))
            if not cursor.fetchone():
                raise ValueError(f"Supplier with ID {supplier_id} does not exist")
            
            cursor.execute('''
                INSERT INTO purchases (date, supplier_id, bill_no, amount, paid_amount, items, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, supplier_id, bill_no, amount, paid_amount, items, notes))
            
            self.db.get_db().commit()
            purchase_id = cursor.lastrowid
            logging.info(f"Created purchase ID: {purchase_id} for supplier {supplier_id}")
            return purchase_id
            
        except sqlite3.Error as e:
            logging.error(f"Error creating purchase: {e}")
            self.db.get_db().rollback()
            raise
    
    def update(self, id, date=None, supplier_id=None, bill_no=None, amount=None, paid_amount=None, items=None, notes=None):
        """Update purchase information."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get current purchase data
            current = self.get_by_id(id)
            if not current:
                raise ValueError(f"Purchase with ID {id} does not exist")
            
            # Build dynamic update query
            updates = []
            params = []
            
            if date is not None:
                updates.append('date = ?')
                params.append(date)
            if supplier_id is not None:
                # Check if new supplier exists
                cursor.execute('SELECT id FROM suppliers WHERE id = ?', (supplier_id,))
                if not cursor.fetchone():
                    raise ValueError(f"Supplier with ID {supplier_id} does not exist")
                updates.append('supplier_id = ?')
                params.append(supplier_id)
            if bill_no is not None:
                updates.append('bill_no = ?')
                params.append(bill_no)
            if amount is not None:
                if amount <= 0:
                    raise ValueError("Amount must be positive")
                updates.append('amount = ?')
                params.append(amount)
            if paid_amount is not None:
                if paid_amount < 0:
                    raise ValueError("Paid amount cannot be negative")
                final_amount = amount if amount is not None else current['amount']
                if paid_amount > final_amount:
                    raise ValueError("Paid amount cannot exceed total amount")
                updates.append('paid_amount = ?')
                params.append(paid_amount)
            if items is not None:
                updates.append('items = ?')
                params.append(items)
            if notes is not None:
                updates.append('notes = ?')
                params.append(notes)
            
            if not updates:
                return False
            
            params.append(id)
            query = f"UPDATE purchases SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, params)
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Updated purchase ID: {id}")
                return True
            else:
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error updating purchase: {e}")
            self.db.get_db().rollback()
            raise
    
    def delete(self, id):
        """Delete a purchase (only if no associated payments)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Check if purchase has any payments
            cursor.execute('SELECT COUNT(*) as payment_count FROM payments WHERE purchase_id = ?', (id,))
            payment_count = cursor.fetchone()['payment_count']
            
            if payment_count > 0:
                raise ValueError("Cannot delete purchase with associated payments")
            
            cursor.execute('DELETE FROM purchases WHERE id = ?', (id,))
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Deleted purchase ID: {id}")
                return True
            else:
                logging.warning(f"No purchase found with ID: {id}")
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error deleting purchase: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_outstanding(self):
        """Get all purchases with outstanding amounts."""
        return self.db.get_outstanding_purchases()
    
    def get_payments(self, id):
        """Get all payments made against this purchase."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT p.*, s.name as supplier_name
                FROM payments p
                JOIN suppliers s ON p.supplier_id = s.id
                WHERE p.purchase_id = ?
                ORDER BY p.date DESC
            ''', (id,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching payments for purchase {id}: {e}")
            return []
    
    def add_payment(self, id, amount):
        """Add payment amount to this purchase (updates paid_amount)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get current purchase
            purchase = self.get_by_id(id)
            if not purchase:
                raise ValueError(f"Purchase with ID {id} does not exist")
            
            new_paid_amount = float(purchase['paid_amount']) + float(amount)
            if new_paid_amount > float(purchase['amount']):
                raise ValueError("Payment amount exceeds outstanding balance")
            
            cursor.execute('''
                UPDATE purchases SET paid_amount = ? WHERE id = ?
            ''', (new_paid_amount, id))
            
            self.db.get_db().commit()
            logging.info(f"Added payment of {amount} to purchase {id}")
            return True
            
        except sqlite3.Error as e:
            logging.error(f"Error adding payment to purchase: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_summary_stats(self, start_date=None, end_date=None):
        """Get summary statistics for purchases."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT 
                    COUNT(*) as total_purchases,
                    COALESCE(SUM(amount), 0) as total_amount,
                    COALESCE(SUM(paid_amount), 0) as total_paid,
                    COALESCE(SUM(amount - paid_amount), 0) as total_outstanding,
                    COUNT(CASE WHEN paid_amount >= amount THEN 1 END) as paid_purchases,
                    COUNT(CASE WHEN paid_amount > 0 AND paid_amount < amount THEN 1 END) as partial_purchases,
                    COUNT(CASE WHEN paid_amount = 0 THEN 1 END) as unpaid_purchases
                FROM purchases
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
            logging.error(f"Error fetching purchase summary: {e}")
            return None