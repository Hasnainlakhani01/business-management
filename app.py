# app.py - Minimal Flask app with templates
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime, date
import os

# Import your models (assuming they exist)
from models.database import Database
from models.supplier import Supplier
from models.customer import Customer
from models.purchase import Purchase
from models.payment import Payment
from models.sale import Sale
from models.receipt import Receipt

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Initialize database
db = Database('business.db')
with app.app_context():
    db.init_db()

# Initialize models
supplier_model = Supplier(db)
customer_model = Customer(db)
purchase_model = Purchase(db)
payment_model = Payment(db)
sale_model = Sale(db)
receipt_model = Receipt(db)

@app.teardown_appcontext
def close_db(error):
    db.close_db()

# Routes
@app.route('/')
def dashboard():
    """Dashboard with summary statistics."""
    try:
        supplier_stats = supplier_model.get_summary_stats()
        customer_stats = customer_model.get_summary_stats()
        purchase_stats = purchase_model.get_summary_stats()
        sale_stats = sale_model.get_summary_stats()
        
        outstanding_purchases = purchase_model.get_outstanding()[:5]  # Latest 5
        outstanding_sales = sale_model.get_outstanding()[:5]  # Latest 5
        
        return render_template('dashboard.html',
                             supplier_stats=supplier_stats,
                             customer_stats=customer_stats,
                             purchase_stats=purchase_stats,
                             sale_stats=sale_stats,
                             outstanding_purchases=outstanding_purchases,
                             outstanding_sales=outstanding_sales)
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dashboard.html')

# Supplier routes
@app.route('/suppliers')
def suppliers():
    """List all suppliers."""
    suppliers = supplier_model.get_all()
    return render_template('suppliers.html', suppliers=suppliers)

@app.route('/suppliers/new', methods=['GET', 'POST'])
def new_supplier():
    """Create new supplier."""
    if request.method == 'POST':
        try:
            supplier_id = supplier_model.create(
                name=request.form['name'],
                contact=request.form.get('contact'),
                address=request.form.get('address')
            )
            flash(f'Supplier created successfully!', 'success')
            return redirect(url_for('suppliers'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
    
    return render_template('supplier_form.html')

@app.route('/suppliers/edit/<int:supplier_id>', methods=['GET', 'POST'])
def edit_supplier(supplier_id):
    supplier = supplier_model.get_by_id(supplier_id)
    if not supplier:
        flash('Supplier not found.', 'error')
        return redirect(url_for('suppliers'))

    if request.method == 'POST':
        try:
            supplier_model.update(
                id=supplier_id,
                name=request.form['name'],
                contact=request.form.get('contact'),
                address=request.form.get('address')
            )
            flash('Supplier updated successfully!', 'success')
            return redirect(url_for('suppliers'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')

    return render_template('supplier_form.html', supplier=supplier, edit=True)

# Purchase routes
@app.route('/purchases')
def purchases():
    """List all purchases."""
    purchases = purchase_model.get_all(limit=50)
    return render_template('purchases.html', purchases=purchases)

@app.route('/purchases/new', methods=['GET', 'POST'])
def new_purchase():
    """Create new purchase."""
    if request.method == 'POST':
        try:
            purchase_id = purchase_model.create(
                date=request.form['date'],
                supplier_id=int(request.form['supplier_id']),
                amount=float(request.form['amount']),
                paid_amount=float(request.form.get('paid_amount', 0)),
                bill_no=request.form.get('bill_no'),
                items=request.form.get('items'),
                notes=request.form.get('notes')
            )
            flash('Purchase created successfully!', 'success')
            return redirect(url_for('purchases'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
    
    suppliers = supplier_model.get_all()
    return render_template('purchase_form.html', suppliers=suppliers, date=date)

@app.route('/purchases/edit/<int:purchase_id>', methods=['GET', 'POST'])
def edit_purchase(purchase_id):
    purchase = purchase_model.get_by_id(purchase_id)
    if not purchase:
        flash('Purchase not found.', 'error')
        return redirect(url_for('purchases'))

    if request.method == 'POST':
        try:
            purchase_model.update(
                id=purchase_id,
                date=request.form['date'],
                supplier_id=int(request.form['supplier_id']),
                amount=float(request.form['amount']),
                paid_amount=float(request.form.get('paid_amount', 0)),
                bill_no=request.form.get('bill_no'),
                items=request.form.get('items'),
                notes=request.form.get('notes')
            )
            flash('Purchase updated successfully!', 'success')
            return redirect(url_for('purchases'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')

    suppliers = supplier_model.get_all()
    return render_template('purchase_form.html', purchase=purchase, suppliers=suppliers, date=date, edit=True)

# Customer routes
@app.route('/customers')
def customers():
    """List all customers."""
    customers = customer_model.get_all()
    return render_template('customers.html', customers=customers)

@app.route('/customers/new', methods=['GET', 'POST'])
def new_customer():
    """Create new customer."""
    if request.method == 'POST':
        try:
            customer_id = customer_model.create(
                name=request.form['name'],
                contact=request.form.get('contact'),
                address=request.form.get('address')
            )
            flash('Customer created successfully!', 'success')
            return redirect(url_for('customers'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
    
    return render_template('customer_form.html')

@app.route('/customers/edit/<int:customer_id>', methods=['GET', 'POST'])
def edit_customer(customer_id):
    customer = customer_model.get_by_id(customer_id)
    if not customer:
        flash('Customer not found.', 'error')
        return redirect(url_for('customers'))

    if request.method == 'POST':
        try:
            customer_model.update(
                id=customer_id,
                name=request.form['name'],
                contact=request.form.get('contact'),
                address=request.form.get('address')
            )
            flash('Customer updated successfully!', 'success')
            return redirect(url_for('customers'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')

    return render_template('customer_form.html', customer=customer, edit=True)

# Sale routes
@app.route('/sales')
def sales():
    """List all sales."""
    sales = sale_model.get_all(limit=50)
    return render_template('sales.html', sales=sales)

@app.route('/sales/new', methods=['GET', 'POST'])
def new_sale():
    """Create new sale."""
    if request.method == 'POST':
        try:
            sale_id = sale_model.create(
                date=request.form['date'],
                customer_id=int(request.form['customer_id']),
                amount=float(request.form['amount']),
                received_amount=float(request.form.get('received_amount', 0)),
                invoice_no=request.form.get('invoice_no'),
                items=request.form.get('items'),
                notes=request.form.get('notes')
            )
            flash('Sale created successfully!', 'success')
            return redirect(url_for('sales'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
    
    customers = customer_model.get_all()
    return render_template('sale_form.html', customers=customers, date=date)

@app.route('/sales/edit/<int:sale_id>', methods=['GET', 'POST'])
def edit_sale(sale_id):
    sale = sale_model.get_by_id(sale_id)
    if not sale:
        flash('Sale not found.', 'error')
        return redirect(url_for('sales'))

    if request.method == 'POST':
        try:
            sale_model.update(
                id=sale_id,
                date=request.form['date'],
                customer_id=int(request.form['customer_id']),
                amount=float(request.form['amount']),
                received_amount=float(request.form.get('received_amount', 0)),
                invoice_no=request.form.get('invoice_no'),
                items=request.form.get('items'),
                notes=request.form.get('notes')
            )
            flash('Sale updated successfully!', 'success')
            return redirect(url_for('sales'))
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')

    customers = customer_model.get_all()
    return render_template('sale_form.html', sale=sale, customers=customers, date=date, edit=True)

# Payment routes
@app.route('/payments/new/<int:supplier_id>')
def new_payment(supplier_id):
    """Create new payment for supplier."""
    supplier = supplier_model.get_by_id(supplier_id)
    outstanding_purchases = supplier_model.get_outstanding_purchases(supplier_id)
    return render_template('payment_form.html', 
                         supplier=supplier, 
                         outstanding_purchases=outstanding_purchases,
                         date=date)

@app.route('/payments', methods=['POST'])
def create_payment():
    """Process payment creation."""
    try:
        payment_id = payment_model.create(
            date=request.form['date'],
            supplier_id=int(request.form['supplier_id']),
            amount=float(request.form['amount']),
            payment_mode=request.form['payment_mode'],
            purchase_id=int(request.form['purchase_id']) if request.form.get('purchase_id') else None,
            reference_no=request.form.get('reference_no'),
            notes=request.form.get('notes')
        )
        flash('Payment recorded successfully!', 'success')
        return redirect(url_for('suppliers'))
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('suppliers'))

if __name__ == '__main__':
    app.run(debug=True)
