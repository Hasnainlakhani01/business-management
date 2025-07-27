from datetime import datetime
import sqlite3
import logging

class Supplier:
    def __init__(self, db):
        self.db = db
    
    def get_all(self):
        """Get all suppliers with their current balance."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT s.*, 
                       COUNT(p.id) as total_purchases,
                       COALESCE(SUM(p.amount), 0) as total_purchase_amount,
                       COALESCE(SUM(p.paid_amount), 0) as total_paid_amount
                FROM suppliers s
                LEFT JOIN purchases p ON s.id = p.supplier_id
                GROUP BY s.id
                ORDER BY s.name
            ''')
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching suppliers: {e}")
            return []
    
    def get_by_id(self, id):
        """Get supplier by ID with additional details."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT s.*, 
                       COUNT(p.id) as total_purchases,
                       COALESCE(SUM(p.amount), 0) as total_purchase_amount,
                       COALESCE(SUM(p.paid_amount), 0) as total_paid_amount,
                       COALESCE(SUM(pay.amount), 0) as total_payments
                FROM suppliers s
                LEFT JOIN purchases p ON s.id = p.supplier_id
                LEFT JOIN payments pay ON s.id = pay.supplier_id
                WHERE s.id = ?
                GROUP BY s.id
            ''', (id,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching supplier {id}: {e}")
            return None
    
    def get_by_name(self, name):
        """Get supplier by exact name match."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('SELECT * FROM suppliers WHERE name = ?', (name,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching supplier by name '{name}': {e}")
            return None
    
    def search(self, query):
        """Search suppliers by name or contact."""
        try:
            cursor = self.db.get_db().cursor()
            search_pattern = f"%{query}%"
            cursor.execute('''
                SELECT * FROM suppliers 
                WHERE name LIKE ? OR contact LIKE ? 
                ORDER BY name
            ''', (search_pattern, search_pattern))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error searching suppliers: {e}")
            return []
    
    def create(self, name, contact=None, address=None):
        """Create a new supplier."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                INSERT INTO suppliers (name, contact, address)
                VALUES (?, ?, ?)
            ''', (name.strip(), contact, address))
            self.db.get_db().commit()
            supplier_id = cursor.lastrowid
            logging.info(f"Created supplier: {name} (ID: {supplier_id})")
            return supplier_id
        except sqlite3.IntegrityError:
            logging.error(f"Supplier with name '{name}' already exists")
            raise ValueError(f"Supplier with name '{name}' already exists")
        except sqlite3.Error as e:
            logging.error(f"Error creating supplier: {e}")
            self.db.get_db().rollback()
            raise
    
    def update(self, id, name=None, contact=None, address=None):
        """Update supplier information."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Build dynamic update query
            updates = []
            params = []
            
            if name is not None:
                updates.append('name = ?')
                params.append(name.strip())
            if contact is not None:
                updates.append('contact = ?')
                params.append(contact)
            if address is not None:
                updates.append('address = ?')
                params.append(address)
            
            if not updates:
                return False
            
            params.append(id)
            query = f"UPDATE suppliers SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, params)
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Updated supplier ID: {id}")
                return True
            else:
                logging.warning(f"No supplier found with ID: {id}")
                return False
                
        except sqlite3.IntegrityError:
            logging.error(f"Supplier with name '{name}' already exists")
            raise ValueError(f"Supplier with name '{name}' already exists")
        except sqlite3.Error as e:
            logging.error(f"Error updating supplier: {e}")
            self.db.get_db().rollback()
            raise
    
    def delete(self, id):
        """Delete a supplier (only if no associated transactions)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Check if supplier has any purchases or payments
            cursor.execute('''
                SELECT COUNT(*) as purchase_count FROM purchases WHERE supplier_id = ?
            ''', (id,))
            purchase_count = cursor.fetchone()['purchase_count']
            
            cursor.execute('''
                SELECT COUNT(*) as payment_count FROM payments WHERE supplier_id = ?
            ''', (id,))
            payment_count = cursor.fetchone()['payment_count']
            
            if purchase_count > 0 or payment_count > 0:
                raise ValueError("Cannot delete supplier with existing transactions")
            
            cursor.execute('DELETE FROM suppliers WHERE id = ?', (id,))
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Deleted supplier ID: {id}")
                return True
            else:
                logging.warning(f"No supplier found with ID: {id}")
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error deleting supplier: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_balance(self, id):
        """Get current balance for a supplier (uses database method)."""
        return self.db.get_supplier_balance(id)
    
    def get_transactions(self, id, limit=None):
        """Get all purchases and payments for a supplier with enhanced details."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT
                    'purchase' as type,
                    p.id,
                    p.date,
                    p.bill_no as reference,
                    p.items,
                    p.amount,
                    p.paid_amount,
                    (p.amount - p.paid_amount) as outstanding,
                    CASE
                        WHEN p.paid_amount >= p.amount THEN 'Paid'
                        WHEN p.paid_amount > 0 THEN 'Partial'
                        ELSE 'Unpaid'
                    END as payment_status,
                    p.notes,
                    p.created_at
                FROM purchases p
                WHERE p.supplier_id = ?
                
                UNION ALL
                
                SELECT
                    'payment' as type,
                    pay.id,
                    pay.date,
                    pay.reference_no as reference,
                    CASE 
                        WHEN pay.purchase_id IS NOT NULL 
                        THEN 'Payment for Purchase #' || pay.purchase_id
                        ELSE 'Advance Payment'
                    END as items,
                    pay.amount,
                    pay.amount as paid_amount,
                    0 as outstanding,
                    'Paid' as payment_status,
                    pay.notes,
                    pay.created_at
                FROM payments pay
                WHERE pay.supplier_id = ?
                
                ORDER BY date DESC, created_at DESC
            '''
            
            params = (id, id)
            
            if limit:
                base_query += ' LIMIT ?'
                params = params + (limit,)
            
            cursor.execute(base_query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching transactions for supplier {id}: {e}")
            return []
    
    def get_outstanding_purchases(self, id):
        """Get purchases with outstanding amounts for a specific supplier."""
        return self.db.get_outstanding_purchases(supplier_id=id)
    
    def get_suppliers_with_balance(self, balance_type='all'):
        """Get suppliers filtered by balance type."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = 'SELECT * FROM suppliers'
            
            if balance_type == 'payable':
                query += ' WHERE balance > 0'
            elif balance_type == 'advance':
                query += ' WHERE balance < 0'
            elif balance_type == 'zero':
                query += ' WHERE balance = 0'
            
            query += ' ORDER BY name'
            
            cursor.execute(query)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching suppliers by balance type: {e}")
            return []
    
    def get_summary_stats(self):
        """Get summary statistics for all suppliers."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_suppliers,
                    COUNT(CASE WHEN balance > 0 THEN 1 END) as suppliers_with_payable,
                    COUNT(CASE WHEN balance < 0 THEN 1 END) as suppliers_with_advance,
                    COUNT(CASE WHEN balance = 0 THEN 1 END) as suppliers_zero_balance,
                    COALESCE(SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END), 0) as total_payable,
                    COALESCE(SUM(CASE WHEN balance < 0 THEN -balance ELSE 0 END), 0) as total_advance,
                    COALESCE(SUM(balance), 0) as net_payable
                FROM suppliers
            ''')
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching supplier summary: {e}")
            return None