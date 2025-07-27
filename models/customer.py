from datetime import datetime
import sqlite3
import logging

class Customer:
    def __init__(self, db):
        self.db = db
    
    def get_all(self):
        """Get all customers with their current balance."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT c.*, 
                       COUNT(s.id) as total_sales,
                       COALESCE(SUM(s.amount), 0) as total_sale_amount,
                       COALESCE(SUM(s.received_amount), 0) as total_received_amount
                FROM customers c
                LEFT JOIN sales s ON c.id = s.customer_id
                GROUP BY c.id
                ORDER BY c.name
            ''')
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching customers: {e}")
            return []
    
    def get_by_id(self, id):
        """Get customer by ID with additional details."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT c.*, 
                       COUNT(s.id) as total_sales,
                       COALESCE(SUM(s.amount), 0) as total_sale_amount,
                       COALESCE(SUM(s.received_amount), 0) as total_received_amount,
                       COALESCE(SUM(r.amount), 0) as total_receipts
                FROM customers c
                LEFT JOIN sales s ON c.id = s.customer_id
                LEFT JOIN receipts r ON c.id = r.customer_id
                WHERE c.id = ?
                GROUP BY c.id
            ''', (id,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching customer {id}: {e}")
            return None
    
    def get_by_name(self, name):
        """Get customer by exact name match."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('SELECT * FROM customers WHERE name = ?', (name,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching customer by name '{name}': {e}")
            return None
    
    def search(self, query):
        """Search customers by name or contact."""
        try:
            cursor = self.db.get_db().cursor()
            search_pattern = f"%{query}%"
            cursor.execute('''
                SELECT * FROM customers 
                WHERE name LIKE ? OR contact LIKE ? 
                ORDER BY name
            ''', (search_pattern, search_pattern))
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error searching customers: {e}")
            return []
    
    def create(self, name, contact=None, address=None):
        """Create a new customer."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                INSERT INTO customers (name, contact, address)
                VALUES (?, ?, ?)
            ''', (name.strip(), contact, address))
            self.db.get_db().commit()
            customer_id = cursor.lastrowid
            logging.info(f"Created customer: {name} (ID: {customer_id})")
            return customer_id
        except sqlite3.IntegrityError:
            logging.error(f"Customer with name '{name}' already exists")
            raise ValueError(f"Customer with name '{name}' already exists")
        except sqlite3.Error as e:
            logging.error(f"Error creating customer: {e}")
            self.db.get_db().rollback()
            raise
    
    def update(self, id, name=None, contact=None, address=None):
        """Update customer information."""
        try:
            cursor = self.db.get_db().cursor()
            
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
            query = f"UPDATE customers SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, params)
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Updated customer ID: {id}")
                return True
            else:
                logging.warning(f"No customer found with ID: {id}")
                return False
                
        except sqlite3.IntegrityError:
            logging.error(f"Customer with name '{name}' already exists")
            raise ValueError(f"Customer with name '{name}' already exists")
        except sqlite3.Error as e:
            logging.error(f"Error updating customer: {e}")
            self.db.get_db().rollback()
            raise
    
    def delete(self, id):
        """Delete a customer (only if no associated transactions)."""
        try:
            cursor = self.db.get_db().cursor()
            
            # Check if customer has any sales or receipts
            cursor.execute('''
                SELECT COUNT(*) as sale_count FROM sales WHERE customer_id = ?
            ''', (id,))
            sale_count = cursor.fetchone()['sale_count']
            
            cursor.execute('''
                SELECT COUNT(*) as receipt_count FROM receipts WHERE customer_id = ?
            ''', (id,))
            receipt_count = cursor.fetchone()['receipt_count']
            
            if sale_count > 0 or receipt_count > 0:
                raise ValueError("Cannot delete customer with existing transactions")
            
            cursor.execute('DELETE FROM customers WHERE id = ?', (id,))
            self.db.get_db().commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Deleted customer ID: {id}")
                return True
            else:
                logging.warning(f"No customer found with ID: {id}")
                return False
                
        except sqlite3.Error as e:
            logging.error(f"Error deleting customer: {e}")
            self.db.get_db().rollback()
            raise
    
    def get_balance(self, id):
        """Get current balance for a customer (uses database method)."""
        return self.db.get_customer_balance(id)
    
    def get_transactions(self, id, limit=None):
        """Get all sales and receipts for a customer with enhanced details."""
        try:
            cursor = self.db.get_db().cursor()
            
            base_query = '''
                SELECT
                    'sale' as type,
                    s.id,
                    s.date,
                    s.invoice_no as reference,
                    s.items,
                    s.amount,
                    s.received_amount,
                    (s.amount - s.received_amount) as outstanding,
                    CASE
                        WHEN s.received_amount >= s.amount THEN 'Paid'
                        WHEN s.received_amount > 0 THEN 'Partial'
                        ELSE 'Unpaid'
                    END as payment_status,
                    s.notes,
                    s.created_at
                FROM sales s
                WHERE s.customer_id = ?
                
                UNION ALL
                
                SELECT
                    'receipt' as type,
                    r.id,
                    r.date,
                    r.reference_no as reference,
                    CASE 
                        WHEN r.sale_id IS NOT NULL 
                        THEN 'Payment for Sale #' || r.sale_id
                        ELSE 'Advance Payment'
                    END as items,
                    r.amount,
                    r.amount as received_amount,
                    0 as outstanding,
                    'Received' as payment_status,
                    r.notes,
                    r.created_at
                FROM receipts r
                WHERE r.customer_id = ?
                
                ORDER BY date DESC, created_at DESC
            '''
            
            params = (id, id)
            
            if limit:
                base_query += ' LIMIT ?'
                params = params + (limit,)
            
            cursor.execute(base_query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching transactions for customer {id}: {e}")
            return []
    
    def get_outstanding_sales(self, id):
        """Get sales with outstanding amounts for a specific customer."""
        return self.db.get_outstanding_sales(customer_id=id)
    
    def get_customers_with_balance(self, balance_type='all'):
        """Get customers filtered by balance type."""
        try:
            cursor = self.db.get_db().cursor()
            
            query = 'SELECT * FROM customers'
            
            if balance_type == 'receivable':
                query += ' WHERE balance > 0'
            elif balance_type == 'advance':
                query += ' WHERE balance < 0'
            elif balance_type == 'zero':
                query += ' WHERE balance = 0'
            
            query += ' ORDER BY name'
            
            cursor.execute(query)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching customers by balance type: {e}")
            return []
    
    def get_summary_stats(self):
        """Get summary statistics for all customers."""
        try:
            cursor = self.db.get_db().cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_customers,
                    COUNT(CASE WHEN balance > 0 THEN 1 END) as customers_with_receivable,
                    COUNT(CASE WHEN balance < 0 THEN 1 END) as customers_with_advance,
                    COUNT(CASE WHEN balance = 0 THEN 1 END) as customers_zero_balance,
                    COALESCE(SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END), 0) as total_receivable,
                    COALESCE(SUM(CASE WHEN balance < 0 THEN -balance ELSE 0 END), 0) as total_advance,
                    COALESCE(SUM(balance), 0) as net_receivable
                FROM customers
            ''')
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error fetching customer summary: {e}")
            return None