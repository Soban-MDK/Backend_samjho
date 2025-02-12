from flask import Blueprint, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import pandas as pd
from app import cache
from redis.exceptions import RedisError

from main.incentive_leaderboard_report_qty import generate_il_report
from main.incentive_leaderboard_report_range import generate_il_report_range
from main.advanced_urgent_reports import generate_au_reports
from main.spot_targets_reports import generate_stores_spot_targets_daily
from main.zero_brand_sales import generate_zero_brand_report
from main.stores_month_targets import generate_stores_month_targets
from main.wow_reports import generate_wow_reports


from utils.db_utils import save_csv_to_local, read_local_data

bp = Blueprint('reports', __name__)

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}

brand_tieup_1_cols = ['Month', 'product_code', 'incentive']
brand_tieup_2_cols = ['Month', 'brand_cat', 'brand_sale_range', '%applied']
brands_cols = ['product_code', 'brand_cat']
spot_targets_cols = ['StoreName', 'Date', 'SpotTarget', 'genSpotTarget']
month_targets_cols = ['StoreName', 'Month', 'Store', 'Generic', 'OTC', 'MSP', 'WOW']
wow_data_cols = ['Month', 'Wow Bill-Range', 'Incentive']

# Add this helper function at the top with other imports and constants
def validate_columns(df, required_columns):
    """
    Validates if the DataFrame has all required columns
    Returns (is_valid, missing_columns)
    """
    df_columns = set(df.columns)
    required_columns = set(required_columns)
    missing_columns = required_columns - df_columns
    return len(missing_columns) == 0, missing_columns

# Add this new route
@bp.route('/')
def index():
    return render_template('index.html')

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@bp.route('/report/incentive-qty/upload', methods=['GET', 'POST'])
def incentive_qty_upload():
    if request.method == 'GET':
        return render_template('reports/incentive_qty_upload.html')
        
    if request.method == 'POST':
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400

            file = request.files['file']
            if file and allowed_file(file.filename):
                # Read the file first to validate columns
                df = pd.read_csv(file)
                is_valid, missing_cols = validate_columns(df, brand_tieup_1_cols)
                
                if not is_valid:
                    return jsonify({
                        'error': f'Missing required columns: {", ".join(missing_cols)}'
                    }), 400

                cache.delete('three_month_report')  # Clear existing cache
                
                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)

                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                file.seek(0)  # Reset file pointer after reading
                file.save(filepath)

                save_csv_to_local(filepath, 'brand_tieup_1')
                full_report = generate_il_report()

                three_month_report = full_report[
                    (full_report['Month'].isin(['May-24','June-24','July-24']))
                ]
                
                # Convert DataFrame to JSON string with proper formatting
                json_data = three_month_report.to_json(orient='records', date_format='iso')
                cache.set('three_month_report', json_data)
                
                # Verify cache
                cached_data = cache.get('three_month_report')
                if cached_data is not None:
                    print('Data cached successfully')
                else:
                    print('Data not cached')

                return jsonify({'message': 'Data uploaded successfully'}), 200
            return jsonify({'error': 'Invalid file format'}), 400
            
        except RedisError as e:
            return jsonify({'error': f'Redis error: {str(e)}'}), 500
        except Exception as e:
            return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@bp.route('/report/incentive-qty/fetch', methods=['GET'])
def incentive_qty_fetch():
    try:
        if request.args.getlist('month'):
            requested_months = request.args.getlist('month')
            data_str = cache.get('three_month_report')
            if not data_str:
                # If no data in cache, fetch the report if it is present in the database
                db_data = read_local_data('incentive_leaderboard_report_qty')
                
                if db_data is not None:
                    three_month_report = db_data[
                        db_data['Month'].isin(['May-24','June-24','July-24'])
                    ]
                    json_data = three_month_report.to_json(orient='records', date_format='iso')
                    cache.set('three_month_report', json_data)
                
                else:
                    fresh_data = generate_il_report()
                    three_month_report = fresh_data[
                        fresh_data['Month'].isin(['May-24','June-24','July-24'])
                    ]
                    json_data = three_month_report.to_json(orient='records', date_format='iso')
                    cache.set('three_month_report', json_data)
            else:
                three_month_report = pd.read_json(data_str, orient='records')

            # Filter by the requested months
            filtered = three_month_report[three_month_report['Month'].isin(requested_months)]
            print(filtered.to_dict('records'))

            return render_template('reports/incentive_qty_fetch.html', tables=filtered.to_dict('records'))
        
        return render_template('reports/incentive_qty_fetch.html')
    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    

@bp.route('/report/incentive-range/upload', methods=['GET', 'POST'])
def incentive_range_upload():
    if request.method == 'GET':
        return render_template('reports/incentive_range_upload.html')

    if request.method == 'POST':
        try:
            file1 = request.files.get('file1')  # brands.csv
            file2 = request.files.get('file2')  # brands_tieup_2.csv

            if not file1 and not file2:
                return jsonify({'message': 'No files uploaded, proceeding with existing data'}), 200

            cache.delete('range_report')

            if not os.path.exists(UPLOAD_FOLDER):
                os.makedirs(UPLOAD_FOLDER)

            # Validate file1 (brands.csv) if uploaded
            if file1 and allowed_file(file1.filename):
                df1 = pd.read_csv(file1)
                is_valid, missing_cols = validate_columns(df1, brands_cols)
                
                if not is_valid:
                    return jsonify({
                        'error': f'brands.csv missing required columns: {", ".join(missing_cols)}'
                    }), 400

                filename1 = secure_filename("brands.csv")
                filepath1 = os.path.join(UPLOAD_FOLDER, filename1)
                file1.seek(0)
                file1.save(filepath1)
                save_csv_to_local(filepath1, 'brands')

            # Validate file2 (brands_tieup_2.csv) if uploaded
            if file2 and allowed_file(file2.filename):
                df2 = pd.read_csv(file2)
                is_valid, missing_cols = validate_columns(df2, brand_tieup_2_cols)
                
                if not is_valid:
                    return jsonify({
                        'error': f'brands_tieup_2.csv missing required columns: {", ".join(missing_cols)}'
                    }), 400

                filename2 = secure_filename("brands_tieup_2.csv")
                filepath2 = os.path.join(UPLOAD_FOLDER, filename2)
                file2.seek(0)
                file2.save(filepath2)
                save_csv_to_local(filepath2, 'brand_tieup_2')

            # Generate report even if no new files were uploaded
            full_report = generate_il_report_range()
            range_report = full_report[
                (full_report['Month'].isin(['December-24', 'January-25', 'February-25']))
            ]

            # Convert DataFrame to JSON string with proper formatting
            json_data = range_report.to_json(orient='records', date_format='iso')
            cache.set('range_report', json_data)

            return jsonify({'message': 'File(s) uploaded successfully or skipped'}), 200

        except RedisError as e:
            return jsonify({'error': f'Redis error: {str(e)}'}), 500
        except Exception as e:
            return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@bp.route('/report/incentive-range/fetch', methods=['GET'])
def incentive_range_fetch():
    try:
        if request.args.getlist('month'):
            requested_months = request.args.getlist('month')
            data_str = cache.get('range_report')
            if not data_str:
                # If no data in cache, fetch it
                db_data = read_local_data('incentive_leaderboard_report_range')
                if db_data is not None:
                    range_report = db_data[
                        db_data['Month'].isin(['December-24', 'January-25', 'February-25'])
                    ]
                    json_data = range_report.to_json(orient='records', date_format='iso')
                    cache.set('range_report', json_data)
                else:
                    fresh_data = generate_il_report_range()
                    range_report = fresh_data[
                        fresh_data['Month'].isin(['December-24', 'January-25', 'February-25'])
                    ]
                    json_data = range_report.to_json(orient='records', date_format='iso')
                    cache.set('range_report', json_data)

            else:
                range_report = pd.read_json(data_str, orient='records')

            # Filter by the requested months
            filtered = range_report[range_report['Month'].isin(requested_months)]
            print(filtered.to_dict('records'))

            return render_template('reports/incentive_range_fetch.html', tables=filtered.to_dict('records'))

        return render_template('reports/incentive_range_fetch.html')
    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    

@bp.route('/report/advanced-urgent/fetch', methods=['GET', 'POST'])
def advanced_urgent_fetch():
    try:
        # Safely parse dates or keep them None
        raw_start = request.form.get('start_date')
        raw_end = request.form.get('end_date')

        start_date = pd.to_datetime(raw_start) if raw_start else None
        end_date = pd.to_datetime(raw_end) if raw_end else None

        data_str = cache.get('advanced_urgent_report')
        if not data_str:
            db_data = read_local_data('advanced_urgent_report')
            if db_data is not None:
                cache.set('advanced_urgent_report', db_data.to_json(orient='records', date_format='iso'))
            else:
                fresh_data = generate_au_reports()
                cache.set('advanced_urgent_report', fresh_data.to_json(orient='records', date_format='iso'))

        data_str = cache.get('advanced_urgent_report')
        advanced_urgent_report = pd.read_json(data_str, orient='records')
        
        # Convert 'date' column to datetime
        advanced_urgent_report['created_at'] = pd.to_datetime(advanced_urgent_report['created_at'])

        # Filter by date range only if both are provided
        if start_date and end_date:
            advanced_urgent_report = advanced_urgent_report[
                (advanced_urgent_report['created_at'] >= start_date) & 
                (advanced_urgent_report['created_at'] <= end_date)
            ]

        # Pass either the date in YYYY-MM-DD format, or an empty string
        return render_template(
            'reports/advanced_urgent_fetch.html', 
            tables=advanced_urgent_report.to_dict('records'),
            start_date=start_date.strftime('%Y-%m-%d') if start_date else '',
            end_date=end_date.strftime('%Y-%m-%d') if end_date else ''
        )

    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    

@bp.route('/report/stores-spot-targets/upload', methods=['GET', 'POST'])
def stores_spot_targets_upload():
    if request.method == 'GET':
        return render_template('reports/stores_spot_targets_upload.html')
    
    if request.method == 'POST':
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400

            file = request.files['file']
            if file and allowed_file(file.filename):
                # Validate columns
                df = pd.read_csv(file)
                is_valid, missing_cols = validate_columns(df, spot_targets_cols)
                
                if not is_valid:
                    return jsonify({
                        'error': f'Missing required columns: {", ".join(missing_cols)}'
                    }), 400

                cache.delete('stores_spot_targets')

                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)

                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                file.seek(0)
                file.save(filepath)

                save_csv_to_local(filepath, 'spot_targets')

                full_report = generate_stores_spot_targets_daily()

                # Convert DataFrame to JSON string with proper formatting
                json_data = full_report.to_json(orient='records', date_format='iso')
                cache.set('stores_spot_targets', json_data)
                
                return jsonify({'message': 'Data uploaded successfully'}), 200
            return jsonify({'error': 'Invalid file format'}), 400
        
        except RedisError as e:
            return jsonify({'error': f'Redis error: {str(e)}'}), 500
        except Exception as e:
            return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
        
@bp.route('/report/stores-spot-targets/fetch', methods=['GET', 'POST'])
def stores_spot_targets_fetch():
    try:
        data_str = cache.get('stores_spot_targets')
        if not data_str:
            db_data = read_local_data('spot_targets_report')
            if db_data is not None:
                cache.set('stores_spot_targets', db_data.to_json(orient='records', date_format='iso'))
            else:
                fresh_data = generate_stores_spot_targets_daily()
                cache.set('stores_spot_targets', fresh_data.to_json(orient='records', date_format='iso'))

        data_str = cache.get('stores_spot_targets')
        stores_spot_targets = pd.read_json(data_str, orient='records')

        return render_template('reports/stores_spot_targets_fetch.html', tables=stores_spot_targets.to_dict('records'))

    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    

@bp.route('/report/zero-brand-sales/fetch', methods=['GET', 'POST'])
def zero_brand_sales_fetch():
    try:
        zero_start = request.form.get('start_date')
        zero_end = request.form.get('end_date')

        start_date = pd.to_datetime(zero_start) if zero_start else None
        end_date = pd.to_datetime(zero_end) if zero_end else None

        data_str = cache.get('zero_brand_sales')
        if not data_str:
            db_data = read_local_data('zero_brand_sales_report')
            if db_data is not None:
                cache.set('zero_brand_sales', db_data.to_json(orient='records', date_format='iso'))
            else:
                fresh_data = generate_zero_brand_report()
                cache.set('zero_brand_sales', fresh_data.to_json(orient='records', date_format='iso'))

        data_str = cache.get('zero_brand_sales')
        zero_brand_sales = pd.read_json(data_str, orient='records')
        
        zero_brand_sales['created_at'] = pd.to_datetime(zero_brand_sales['created_at'])

        if start_date and end_date:
            zero_brand_sales = zero_brand_sales[
                (zero_brand_sales['created_at'] >= start_date) & 
                (zero_brand_sales['created_at'] <= end_date)
            ]

        return render_template(
            'reports/zero_brand_sales_fetch.html', 
            tables=zero_brand_sales.to_dict('records'),
            start_date=start_date.strftime('%Y-%m-%d') if start_date else '',
            end_date=end_date.strftime('%Y-%m-%d') if end_date else ''
        )
    
    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    
###### BAAKI ######

@bp.route('/report/stores-month-targets/upload', methods=['GET', 'POST'])
def stores_month_targets_upload():
    if request.method == 'GET':
        return render_template('reports/stores_month_targets_upload.html')
        
    if request.method == 'POST':
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400

            file = request.files['file']
            if file and allowed_file(file.filename):
                df = pd.read_csv(file)
                is_valid, missing_cols = validate_columns(df, month_targets_cols)
                
                if not is_valid:
                    return jsonify({
                        'error': f'Missing required columns: {", ".join(missing_cols)}'
                    }), 400
                    
                cache.delete('sales_target_report')  # Clear existing cache
                
                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)

                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                file.seek(0)
                file.save(filepath)

                save_csv_to_local(filepath, 'month_targets')
                full_report = generate_stores_month_targets()

                sales_target_report = full_report[
                    (full_report['Month'].isin(['May-24','June-24','July-24']))
                ]
                
                # Convert DataFrame to JSON string with proper formatting
                json_data = sales_target_report.to_json(orient='records', date_format='iso')
                cache.set('sales_target_report', json_data)
                
                # Verify cache
                cached_data = cache.get('sales_target_report')
                if cached_data is not None:
                    print('Data cached successfully')
                else:
                    print('Data not cached')

                return jsonify({'message': 'Data uploaded successfully'}), 200
            return jsonify({'error': 'Invalid file format'}), 400
            
        except RedisError as e:
            return jsonify({'error': f'Redis error: {str(e)}'}), 500
        except Exception as e:
            return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
        
@bp.route('/report/stores-month-targets/fetch', methods=['GET', 'POST'])
def stores_month_targets_fetch():    
    try:
        if request.args.getlist('month'):
            requested_months = request.args.getlist('month')
            data_str = cache.get('sales_target_report')
            if not data_str:
                # If no data in cache, fetch the report if it is present in the database
                db_data = read_local_data('sales_target_report')
                
                if db_data is not None:
                    sales_target_report = db_data[
                        db_data['Month'].isin(['May-24','June-24','July-24'])
                    ]
                    json_data = sales_target_report.to_json(orient='records', date_format='iso')
                    cache.set('sales_target_report', json_data)
                
                else:
                    fresh_data = generate_stores_month_targets()
                    sales_target_report = fresh_data[
                        fresh_data['Month'].isin(['May-24','June-24','July-24'])
                    ]
                    json_data = sales_target_report.to_json(orient='records', date_format='iso')
                    cache.set('sales_target_report', json_data)
            else:
                sales_target_report = pd.read_json(data_str, orient='records')

            print(sales_target_report.head())
            # Filter by the requested months
            filtered = sales_target_report[sales_target_report['Month'].isin(requested_months)]
            print(filtered.to_dict('records'))

            return render_template('reports/stores_month_targets_fetch.html', tables=filtered.to_dict('records'))
        
        return render_template('reports/stores_month_targets_fetch.html')
    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500


@bp.route('/report/wow/upload', methods=['GET', 'POST'])
def wow_upload():
    if request.method == 'GET':
        return render_template('reports/wow_upload.html')
    
    if request.method == 'POST':
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400

            file = request.files['file']
            if file and allowed_file(file.filename):
                df = pd.read_csv(file)
                is_valid, missing_cols = validate_columns(df, wow_data_cols)
                
                if not is_valid:
                    return jsonify({
                        'error': f'Missing required columns: {", ".join(missing_cols)}'
                    }), 400

                cache.delete('wow_report')  # Clear existing cache
                
                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)

                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                file.seek(0)
                file.save(filepath)

                save_csv_to_local(filepath, 'wow_data')
                full_report = generate_wow_reports()

                wow_report = full_report[
                    (full_report['Month'].isin(['May-24','June-24','July-24']))
                ]
                
                # Convert DataFrame to JSON string with proper formatting
                json_data = wow_report.to_json(orient='records', date_format='iso')
                cache.set('wow_report', json_data)
                
                # Verify cache
                cached_data = cache.get('wow_report')
                if cached_data is not None:
                    print('Data cached successfully')
                else:
                    print('Data not cached')

                return jsonify({'message': 'Data uploaded successfully'}), 200
            return jsonify({'error': 'Invalid file format'}), 400
            
        except RedisError as e:
            return jsonify({'error': f'Redis error: {str(e)}'}), 500
        except Exception as e:
            return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
        
@bp.route('/report/wow/fetch', methods=['GET', 'POST'])
def wow_fetch():    
    try:
        if request.args.getlist('month'):
            requested_months = request.args.getlist('month')
            data_str = cache.get('wow_report')
            if not data_str:
                # If no data in cache, fetch the report if it is present in the database
                db_data = read_local_data('wow_report')
                
                if db_data is not None:
                    wow_report = db_data[
                        db_data['Month'].isin(['May-24','June-24','July-24'])
                    ]
                    json_data = wow_report.to_json(orient='records', date_format='iso')
                    cache.set('wow_report', json_data)
                
                else:
                    fresh_data = generate_wow_reports()
                    wow_report = fresh_data[
                        fresh_data['Month'].isin(['May-24','June-24','July-24'])
                    ]
                    json_data = wow_report.to_json(orient='records', date_format='iso')
                    cache.set('wow_report', json_data)
            else:
                wow_report = pd.read_json(data_str, orient='records')

            print(wow_report.head())
            # Filter by the requested months
            filtered = wow_report[wow_report['Month'].isin(requested_months)]
            print(filtered.to_dict('records'))

            return render_template('reports/wow_fetch.html', tables=filtered.to_dict('records'))
        
        return render_template('reports/wow_fetch.html')
    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500