import firebase_admin
from firebase_admin import credentials, firestore, auth
import csv
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import pytz
import pandas as pd

cred = credentials.Certificate('aftershoot-co.json')
firebase_admin.initialize_app(cred)
db = firestore.client()


class timeConversion:
    def convert_timestamp_to_ist(timestamp_dict):
        seconds = timestamp_dict['seconds']
        nanoseconds = timestamp_dict['nanoseconds']
        utc_dt = datetime.fromtimestamp(seconds, pytz.UTC) + timedelta(microseconds=nanoseconds // 1000)
        ist_tz = pytz.timezone('Asia/Kolkata')

        ist_dt = utc_dt.astimezone(ist_tz)
        formatted_ist_datetime = ist_dt.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_ist_datetime

    def convert_to_ist(utc_dt):
        ist = pytz.timezone('Asia/Kolkata')
        dt_ist = utc_dt.astimezone(ist)
        return dt_ist.strftime('%Y-%m-%d %H:%M:%S')

    def time_to_minutes(time_str):
        parts = time_str.split(', ')
        total_minutes = 0
        for part in parts:
            if 'min' in part:
                minutes = int(part.split(' ')[0])
                total_minutes += minutes
            elif 'sec' in part:
                seconds = int(part.split(' ')[0])
                total_minutes += seconds / 60
        return total_minutes


def completed_documents(collection_ref, status_field="status"):
    docs = collection_ref.where(status_field, '==', 'Completed').stream()
    return docs


class dataProcessor:
    def editsData(uid, email):
        data = completed_documents(db.collection(f'users/{uid}/edit_projects'))
        docs_data = []
        for doc in data:
            edits_data = doc.to_dict()
            if 'created_on' in edits_data:
                edits_data['created_on'] = timeConversion.convert_timestamp_to_ist(edits_data['created_on'])
            filtered_data = {field: edits_data.get(field) for field in
                             ['profile_name', 'profile_id', 'images_count', 'color_profile', 'created_on']}
            docs_data.append(filtered_data)

        customer_ref = db.document(f'customers/{uid}').get()
        customer_data = customer_ref.to_dict()
        customer_id = customer_data.get('stripeId') if customer_data else None

        result = []

        for doc_ in docs_data:
            profile_name = doc_['profile_name']
            profile_id = doc_['profile_id']
            images_count = doc_['images_count']
            color = doc_['color_profile']
            created_on = doc_['created_on']
            result.append({
                "user_id": uid,
                "email": email,
                "profile_name": profile_name,
                "profile_id": profile_id,
                "images_count": images_count,
                "color_profile": color,
                "customer_id": customer_id,
                "created_on": created_on
            })

        csv_file = "edits_data.csv"

        csv_columns = ["user_id", "email", "profile_name", "profile_id", "images_count", "color_profile", "customer_id",
                       "created_on"]

        file_exists = os.path.isfile(csv_file)

        try:
            with open(csv_file, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=csv_columns)
                if not file_exists:
                    writer.writeheader()
                for data in result:
                    writer.writerow(data)
            print(f"Data successfully saved to {csv_file}")
        except IOError:
            print("I/O error")
        return result

    def cullData(uid, email):
        data = completed_documents(db.collection(f'users/{uid}/projects'))
        docs_data = []
        for doc in data:
            project_data = doc.to_dict()
            if 'updated_at' in project_data:
                project_data['updated_at'] = timeConversion.convert_to_ist(project_data['updated_at'])
            if 'total_time' in project_data:
                project_data['total_time'] = timeConversion.time_to_minutes(project_data['total_time'])
            filtered_data = {field: project_data.get(field) for field in
                             ['cull_type', 'album_category', 'updated_at', 'total_time', 'total_images', 'is_jpeg']}
            docs_data.append(filtered_data)

        customer_ref = db.document(f'customers/{uid}').get()
        customer_data = customer_ref.to_dict()
        customer_id = customer_data.get('stripeId') if customer_data else None

        result = []
        for item in docs_data:
            cull_type = item['cull_type']
            album_category = item['album_category']
            updated_at = item['updated_at']
            total_time = item['total_time']
            is_jpeg = item['is_jpeg']
            result.append({
                "user_id": uid,
                "email": email,
                "cull_type": cull_type,
                "album_category": album_category,
                "updated_at": updated_at,
                "total_time": total_time,
                "is_jpeg": is_jpeg,
                "customer_id": customer_id
            })

        csv_file = "cull_data.csv"
        csv_columns = ["user_id", "email", "cull_type", "album_category", "updated_at", "total_time", "is_jpeg",
                       "customer_id"]

        file_exists = os.path.isfile(csv_file)

        try:
            with open(csv_file, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=csv_columns)
                if not file_exists:
                    writer.writeheader()
                for data in result:
                    writer.writerow(data)
            print(f"Data successfully saved to {csv_file}")
        except IOError:
            print("I/O error")
        return result


def processUsersData(user_data):
    uid, email = user_data
    dataProcessor.editsData(uid, email)
    dataProcessor.cullData(uid, email)

df = pd.read_csv("/Users/saurav/Documents/Aftershoot/Daily_Work/firebase_users_list.csv")

user_data_list = list(df.itertuples(index=False, name=None))

with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(processUsersData, user_data_list)
