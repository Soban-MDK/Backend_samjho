from flask import Blueprint, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import pandas as pd
from app import cache
from redis.exceptions import RedisError

from main.incentive_leaderboard_report_qty import generate_il_report
from main.incentive_leaderboard_report_range import generate_il_report_range
from main.advanced_urgent_reports import generate_au_reports
from main.spot_targets_reports import generate_stores_spot_targets


from utils.db_utils import save_csv_to_local, read_local_data

bp = Blueprint('reports', __name__)

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}

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
                cache.delete('three_month_report')  # Clear existing cache
                
                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)

                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)
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

            cache.delete('range_report')  # Clear existing cache

            if not os.path.exists(UPLOAD_FOLDER):
                os.makedirs(UPLOAD_FOLDER)

            # Process file1 (brands.csv) if uploaded
            if file1 and allowed_file(file1.filename):
                filename1 = secure_filename("brands.csv")
                filepath1 = os.path.join(UPLOAD_FOLDER, filename1)
                file1.save(filepath1)
                save_csv_to_local(filepath1, 'brands')

            # Process file2 (brands_tieup_2.csv) if uploaded
            if file2 and allowed_file(file2.filename):
                filename2 = secure_filename("brands_tieup_2.csv")
                filepath2 = os.path.join(UPLOAD_FOLDER, filename2)
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
                cache.delete('stores_spot_targets')

                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)

                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                file.save(filepath)

                save_csv_to_local(filepath, 'spot_targets')

                full_report = generate_stores_spot_targets()

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
                fresh_data = generate_stores_spot_targets()
                cache.set('stores_spot_targets', fresh_data.to_json(orient='records', date_format='iso'))

        data_str = cache.get('stores_spot_targets')
        stores_spot_targets = pd.read_json(data_str, orient='records')

        return render_template('reports/stores_spot_targets_fetch.html', tables=stores_spot_targets.to_dict('records'))

    except RedisError as e:
        return jsonify({'error': f'Redis error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    
