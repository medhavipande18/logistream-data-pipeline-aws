import json
import boto3
import csv
import io
from urllib.parse import quote_plus

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # --- CONFIGURATION ---
    BUCKET_NAME = "dataco-geospatial-data" 
    SOURCE_KEY = "routes.geojson"
    OUTPUT_KEY = "processed_routes/routes_flattened.csv"
    
    try:
        # 1. Read and parse the GeoJSON file
        response = s3.get_object(Bucket=BUCKET_NAME, Key=SOURCE_KEY)
        file_content = response['Body'].read().decode('utf-8')
        json_data = json.loads(file_content)
        
        # 2. Prepare CSV Buffer for writing
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(['origin_lat', 'origin_long', 'dest_lat', 'dest_long', 'shape_wkt'])
        
        features = json_data.get('features', [])
        
        # 3. Loop through features and flatten geometry
        for feature in features:
            coords = feature['geometry']['coordinates']
            
            # Identify Start/End points for linking
            start_long, start_lat = coords[0]
            end_long, end_lat = coords[-1]
            
            # Convert array of coords into a WKT LINESTRING (required for Redshift/Tableau geo)
            coord_pairs = [f"{c[0]} {c[1]}" for c in coords]
            wkt_string = f"LINESTRING({', '.join(coord_pairs)})"
            
            writer.writerow([start_lat, start_long, end_lat, end_lat, wkt_string])
            
        # 4. Upload the final CSV back to S3
        s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY, Body=csv_buffer.getvalue())
        
        return {'statusCode': 200, 'body': f"Success! Processed {len(features)} routes."}
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': f"Error: {str(e)}"}