from datetime import datetime, date
import sqlite3
import logging

class Payment:
    def __init__(self, db):
        self.db = db
    
    def get_all(self, limit=None, offset=None):
        """Get all payments with supplier details."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT p.*, s.name as supplier_name,
                       CASE 
                           WHEN p.purchase_id IS NOT NULL 
                           THEN 'Purchase Payment'
                           ELSE 'Advance Payment'
                       END as payment_type
                FROM payments p
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
            logging.error(f"Error fetching payments: {e}")
            return []
    
    def get_by_id(self, id):
        """Get payment by ID with supplier and purchase details."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT p.*, s.name as supplier_name, s.contact as supplier_contact,
                       pur.bill_no as purchase_bill_no, pur.amount as purchase_amount,
                       CASE 
                           WHEN p.purchase_id IS NOT NULL 
                           THEN 'Purchase Payment'
                           ELSE 'Advance Payment'
                       END as payment_type
                FROM payments p
                JOIN suppliers s ON p.supplier_id = s.id
                LEFT JOIN purchases pur ON p.purchase_id = pur.id
                WHERE p.id = ?
            ''', (id,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching payment {id}: {e}")
            return None
    
    def get_by_supplier(self, supplier_id, limit=None):
        """Get payments for a specific supplier."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT p.*, s.name as supplier_name,
                       CASE 
                           WHEN p.purchase_id IS NOT NULL 
                           THEN 'Purchase Payment'
                           ELSE 'Advance Payment'
                       END as payment_type
                FROM payments p
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
            logging.error(f"Error fetching payments for supplier {supplier_id}: {e}")
            return []
    
    def get_by_date_range(self, start_date, end_date):
        """Get payments within a date range."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT p.*, s.name as supplier_name,
                       CASE 
                           WHEN p.purchase_id IS NOT NULL 
                           THEN 'Purchase Payment'
                           ELSE 'Advance Payment'
                       END as payment_type
                FROM payments p
                JOIN suppliers s ON p.supplier_id = s.id
                WHERE p.date BETWEEN ? AND ?
                ORDER BY p.date DESC
            ''', (start_date, end_date))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching payments by date range: {e}")
            return []
    
    def get_by_payment_mode(self, payment_mode):
        """Get payments by payment mode."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT p.*, s.name as supplier_name
                FROM payments p
                JOIN suppliers s ON p.supplier_id = s.id
                WHERE p.payment_mode = ?
                ORDER BY p.date DESC
            ''', (payment_mode,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching payments by mode {payment_mode}: {e}")
            return []
    
    def create(self, date, supplier_id, amount, payment_mode, purchase_id=None, reference_no=None, notes=None):
        """Create a new payment."""
        try:
            # Validate inputs
            if not isinstance(date, (str, date)):
                raise ValueError("Date must be a string or date object")
            if amount <= 0:
                raise ValueError("Amount must be positive")
            if payment_mode not in ['cash', 'bank', 'cheque', 'upi', 'card']:
                raise ValueError("Invalid payment mode")
            
            cursor = self.db.get_db().cursor()
            
            # Check if supplier exists
            cursor.execute('SELECT id FROM suppliers WHERE id = ?', (supplier_id,))
            if not cursor.fetchone():
                raise ValueError(f"Supplier with ID {supplier_id} does not exist")
            
            # If purchase_id is provided, validate it and check outstanding amount
            if purchase_id:
                cursor.execute('''
                    SELECT amount, paid_amount FROM purchases 
                    WHERE id = ? AND supplier_id = ?
                ''', (purchase_id, supplier_id))
                purchase = cursor.fetchone()
                
                if not purchase:
                    raise ValueError(f"Purchase with ID {purchase_id} does not exist for this supplier")
                
                outstanding = float(purchase['amount']) - float(purchase['paid_amount'])
                if amount > outstanding:
                    raise ValueError(f"Payment amount ({amount}) exceeds outstanding balance ({outstanding})")
            
            # Create the payment
            cursor.execute('''
                INSERT INTO payments (date, supplier_id, purchase_id, amount, payment_mode, reference_no, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, supplier_id, purchase_id, amount, payment_mode, reference_no, notes))
            
            payment_id = cursor.lastrowid
            
            # If this is a purchase payment, update the purchase's paid_amount
            if purchase_id:
                cursor.execute('''
                    UPDATE purchases 
                    SET paid_amount = paid_amount + ? 
                    WHERE id = ?
                ''', (amount, purchase_id))
            
            self.db.get_db().commit()
            logging.info(f"Created payment ID: {payment_id} for supplier {supplier_id}")
            return payment_id
            
        except sqlite3.Error as e:
            logging.error(f"Error creating payment: {e}")
            self.db.get_db().rollback()
            raise
    
    def update(self, id, date=None, amount=None, payment_mode=None, reference_no=None, notes=None):
        """Update payment information (amount changes will affect purchase paid_amount)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get current payment data
            current = self.get_by_id(id)
            if not current:
                raise ValueError(f"Payment with ID {id} does not exist")
            
            old_amount = float(current['amount'])
            
            # Build dynamic update query
            updates = []
            params = []
            
            if date is not None:
                updates.append('date = ?')
                params.append(date)
            if amount is not None:
                if amount <= 0:
                    raise ValueError("Amount must be positive")
                
                # If this payment is linked to a purchase, validate the new amount
                if current['purchase_id']:
                    cursor.execute('''
                        SELECT amount, paid_amount FROM purchases WHERE id = ?
                    ''', (current['purchase_id'],))
                    purchase = cursor.fetchone()
                    
                    # Calculate what the new outstanding would be
                    other_payments = float(purchase['paid_amount']) - old_amount
                    new_total_paid = other_payments + amount
                    
                    if new_total_paid > float(purchase['amount']):
                        raise ValueError("Updated payment amount would exceed purchase amount")
                
                updates.append('amount = ?')
                params.append(amount)
            if payment_mode is not None:
                if payment_mode not in ['cash', 'bank', 'cheque', 'upi', 'card']:
                    raise ValueError("Invalid payment mode")
                updates.append('payment_mode = ?')
                params.append(payment_mode)
            if reference_no is not None:
                updates.append('reference_no = ?')
                params.append(reference_no)
            if notes is not None:
                updates.append('notes = ?')
                params.append(notes)
            
            if not updates:
                return False
            
            params.append(id)
            query = f"UPDATE payments SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, params)
            
            # If amount was updated and this is a purchase payment, update the purchase
            if amount is not None and current['purchase_id']:
                amount_difference = amount - old_amount
                cursor.execute('''
                    UPDATE purchases 
                    SET paid_amount = paid_amount + ? 
                    WHERE id = ?
                ''', (amount_difference, current['purchase_id']))
            
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Updated payment ID: {id}")
                return True
            else:
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error updating payment: {e}")
            self.db.get_db().rollback()
            raise
    
    def delete(self, id):
        """Delete a payment (will affect purchase paid_amount if applicable)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get payment details before deletion
            payment = self.get_by_id(id)
            if not payment:
                raise ValueError(f"Payment with ID {id} does not exist")
            
            # If this payment is linked to a purchase, update the purchase's paid_amount
            if payment['purchase_id']:
                cursor.execute('''
                    UPDATE purchases 
                    SET paid_amount = paid_amount - ? 
                    WHERE id = ?
                ''', (payment['amount'], payment['purchase_id']))
            
            cursor.execute('DELETE FROM payments WHERE id = ?', (id,))
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Deleted payment ID: {id}")
                return True
            else:
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error deleting payment: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_summary_stats(self, start_date=None, end_date=None):
        """Get summary statistics for payments."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT 
                    COUNT(*) as total_payments,
                    COALESCE(SUM(amount), 0) as total_amount,
                    COUNT(CASE WHEN purchase_id IS NOT NULL THEN 1 END) as purchase_payments,
                    COUNT(CASE WHEN purchase_id IS NULL THEN 1 END) as advance_payments,
                    COALESCE(SUM(CASE WHEN purchase_id IS NOT NULL THEN amount ELSE 0 END), 0) as purchase_payment_amount,
                    COALESCE(SUM(CASE WHEN purchase_id IS NULL THEN amount ELSE 0 END), 0) as advance_payment_amount
                FROM payments
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
            logging.error(f"Error fetching payment summary: {e}")
            return None
    
    def get_by_mode_summary(self, start_date=None, end_date=None):
        """Get payment summary grouped by payment mode."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT payment_mode, 
                       COUNT(*) as count,
                       COALESCE(SUM(amount), 0) as total_amount
                FROM payments
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
            
            base_query += ' GROUP BY payment_mode ORDER BY total_amount DESC'
            
            cursor.execute(base_query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching payment mode summary: {e}")
            return []