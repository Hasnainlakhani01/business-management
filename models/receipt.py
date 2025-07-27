from datetime import datetime, date
import sqlite3
import logging

class Receipt:
    def __init__(self, db):
        self.db = db
    
    def get_all(self, limit=None, offset=None):
        """Get all receipts with customer details."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT r.*, c.name as customer_name,
                       CASE 
                           WHEN r.sale_id IS NOT NULL 
                           THEN 'Sale Receipt'
                           ELSE 'Advance Receipt'
                       END as receipt_type
                FROM receipts r
                JOIN customers c ON r.customer_id = c.id
                ORDER BY r.date DESC, r.created_at DESC
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
            logging.error(f"Error fetching receipts: {e}")
            return []
    
    def get_by_id(self, id):
        """Get receipt by ID with customer and sale details."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT r.*, c.name as customer_name, c.contact as customer_contact,
                       s.invoice_no as sale_invoice_no, s.amount as sale_amount,
                       CASE 
                           WHEN r.sale_id IS NOT NULL 
                           THEN 'Sale Receipt'
                           ELSE 'Advance Receipt'
                       END as receipt_type
                FROM receipts r
                JOIN customers c ON r.customer_id = c.id
                LEFT JOIN sales s ON r.sale_id = s.id
                WHERE r.id = ?
            ''', (id,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching receipt {id}: {e}")
            return None
    
    def get_by_customer(self, customer_id, limit=None):
        """Get receipts for a specific customer."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = '''
                SELECT r.*, c.name as customer_name,
                       CASE 
                           WHEN r.sale_id IS NOT NULL 
                           THEN 'Sale Receipt'
                           ELSE 'Advance Receipt'
                       END as receipt_type
                FROM receipts r
                JOIN customers c ON r.customer_id = c.id
                WHERE r.customer_id = ?
                ORDER BY r.date DESC
            '''
            
            params = (customer_id,)
            if limit:
                query += ' LIMIT ?'
                params = (customer_id, limit)
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching receipts for customer {customer_id}: {e}")
            return []
    
    def get_by_date_range(self, start_date, end_date):
        """Get receipts within a date range."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT r.*, c.name as customer_name,
                       CASE 
                           WHEN r.sale_id IS NOT NULL 
                           THEN 'Sale Receipt'
                           ELSE 'Advance Receipt'
                       END as receipt_type
                FROM receipts r
                JOIN customers c ON r.customer_id = c.id
                WHERE r.date BETWEEN ? AND ?
                ORDER BY r.date DESC
            ''', (start_date, end_date))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching receipts by date range: {e}")
            return []
    
    def get_by_payment_mode(self, payment_mode):
        """Get receipts by payment mode."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT r.*, c.name as customer_name
                FROM receipts r
                JOIN customers c ON r.customer_id = c.id
                WHERE r.payment_mode = ?
                ORDER BY r.date DESC
            ''', (payment_mode,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching receipts by mode {payment_mode}: {e}")
            return []
    
    def create(self, date, customer_id, amount, payment_mode, sale_id=None, reference_no=None, notes=None):
        """Create a new receipt."""
        try:
            # Validate inputs
            if not isinstance(date, (str, date)):
                raise ValueError("Date must be a string or date object")
            if amount <= 0:
                raise ValueError("Amount must be positive")
            if payment_mode not in ['cash', 'bank', 'cheque', 'upi', 'card']:
                raise ValueError("Invalid payment mode")
            
            cursor = self.db.get_db().cursor()
            
            # Check if customer exists
            cursor.execute('SELECT id FROM customers WHERE id = ?', (customer_id,))
            if not cursor.fetchone():
                raise ValueError(f"Customer with ID {customer_id} does not exist")
            
            # If sale_id is provided, validate it and check outstanding amount
            if sale_id:
                cursor.execute('''
                    SELECT amount, received_amount FROM sales 
                    WHERE id = ? AND customer_id = ?
                ''', (sale_id, customer_id))
                sale = cursor.fetchone()
                
                if not sale:
                    raise ValueError(f"Sale with ID {sale_id} does not exist for this customer")
                
                outstanding = float(sale['amount']) - float(sale['received_amount'])
                if amount > outstanding:
                    raise ValueError(f"Receipt amount ({amount}) exceeds outstanding balance ({outstanding})")
            
            # Create the receipt
            cursor.execute('''
                INSERT INTO receipts (date, customer_id, sale_id, amount, payment_mode, reference_no, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, customer_id, sale_id, amount, payment_mode, reference_no, notes))
            
            receipt_id = cursor.lastrowid
            
            # If this is a sale receipt, update the sale's received_amount
            if sale_id:
                cursor.execute('''
                    UPDATE sales 
                    SET received_amount = received_amount + ? 
                    WHERE id = ?
                ''', (amount, sale_id))
            
            self.db.get_db().commit()
            logging.info(f"Created receipt ID: {receipt_id} for customer {customer_id}")
            return receipt_id
            
        except sqlite3.Error as e:
            logging.error(f"Error creating receipt: {e}")
            self.db.get_db().rollback()
            raise
    
    def update(self, id, date=None, amount=None, payment_mode=None, reference_no=None, notes=None):
        """Update receipt information (amount changes will affect sale received_amount)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get current receipt data
            current = self.get_by_id(id)
            if not current:
                raise ValueError(f"Receipt with ID {id} does not exist")
            
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
                
                # If this receipt is linked to a sale, validate the new amount
                if current['sale_id']:
                    cursor.execute('''
                        SELECT amount, received_amount FROM sales WHERE id = ?
                    ''', (current['sale_id'],))
                    sale = cursor.fetchone()
                    
                    # Calculate what the new outstanding would be
                    other_receipts = float(sale['received_amount']) - old_amount
                    new_total_received = other_receipts + amount
                    
                    if new_total_received > float(sale['amount']):
                        raise ValueError("Updated receipt amount would exceed sale amount")
                
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
            query = f"UPDATE receipts SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, params)
            
            # If amount was updated and this is a sale receipt, update the sale
            if amount is not None and current['sale_id']:
                amount_difference = amount - old_amount
                cursor.execute('''
                    UPDATE sales 
                    SET received_amount = received_amount + ? 
                    WHERE id = ?
                ''', (amount_difference, current['sale_id']))
            
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Updated receipt ID: {id}")
                return True
            else:
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error updating receipt: {e}")
            self.db.get_db().rollback()
            raise
    
    def delete(self, id):
        """Delete a receipt (will affect sale received_amount if applicable)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Get receipt details before deletion
            receipt = self.get_by_id(id)
            if not receipt:
                raise ValueError(f"Receipt with ID {id} does not exist")
            
            # If this receipt is linked to a sale, update the sale's received_amount
            if receipt['sale_id']:
                cursor.execute('''
                    UPDATE sales 
                    SET received_amount = received_amount - ? 
                    WHERE id = ?
                ''', (receipt['amount'], receipt['sale_id']))
            
            cursor.execute('DELETE FROM receipts WHERE id = ?', (id,))
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Deleted receipt ID: {id}")
                return True
            else:
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error deleting receipt: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_summary_stats(self, start_date=None, end_date=None):
        """Get summary statistics for receipts."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT 
                    COUNT(*) as total_receipts,
                    COALESCE(SUM(amount), 0) as total_amount,
                    COUNT(CASE WHEN sale_id IS NOT NULL THEN 1 END) as sale_receipts,
                    COUNT(CASE WHEN sale_id IS NULL THEN 1 END) as advance_receipts,
                    COALESCE(SUM(CASE WHEN sale_id IS NOT NULL THEN amount ELSE 0 END), 0) as sale_receipt_amount,
                    COALESCE(SUM(CASE WHEN sale_id IS NULL THEN amount ELSE 0 END), 0) as advance_receipt_amount
                FROM receipts
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
            logging.error(f"Error fetching receipt summary: {e}")
            return None
    
    def get_by_mode_summary(self, start_date=None, end_date=None):
        """Get receipt summary grouped by payment mode."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT payment_mode, 
                       COUNT(*) as count,
                       COALESCE(SUM(amount), 0) as total_amount
                FROM receipts
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
            logging.error(f"Error fetching receipt mode summary: {e}")
            return []