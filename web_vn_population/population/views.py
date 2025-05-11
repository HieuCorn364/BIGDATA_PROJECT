from django.shortcuts import render
from .models import Population
from pyhdfs import HdfsClient
import os
import subprocess
from datetime import datetime
from django.db import connection

def population_list(request):
    populations = Population.objects.all()
    return render(request, 'population/population_list.html', {'populations': populations})

def region_stats(request):
    region_stats_result = None
    sqoop_message = None
    
    # Chạy truy vấn và Sqoop khi truy cập trang hoặc nhấn nút
    if request.method == 'POST' or request.GET.get('calculate') == 'region_stats':
        try:
            # Truy vấn SQL trực tiếp
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT vung, SUM(dan_so) AS dan_so, SUM(dien_tich) AS dien_tich,
                           SUM(dan_so) / (SELECT SUM(dan_so) FROM POPULATION) AS ty_trong
                    FROM POPULATION
                    GROUP BY vung
                    ORDER BY vung
                """)
                columns = [col[0] for col in cursor.description]
                region_stats_result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Ghi log debug
            print("Region stats:", region_stats_result)
            
            # Tạo tên thư mục động dựa trên thời gian
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            target_dir = f"/user/sqoop/ty_trong_vung_{timestamp}"
            
            # Lệnh Sqoop
            sqoop_command = [
                'sqoop', 'import',
                '-Dorg.apache.sqoop.splitter.allow_text_splitter=true',
                '--connect', 'jdbc:mysql://localhost:3306/BIGDATA',
                '--username', 'root',
                '--password', '@Bao1234',
                '--query', f"SELECT vung, SUM(dan_so) AS dan_so, SUM(dien_tich) AS dien_tich, SUM(dan_so) / (SELECT SUM(dan_so) FROM POPULATION) AS ty_trong FROM POPULATION WHERE $CONDITIONS GROUP BY vung",
                '--target-dir', target_dir,
                '--as-textfile',
                '--fields-terminated-by', ',',
                '-m', '1'
            ]
            
            # Xóa thư mục HDFS nếu đã tồn tại
            client = HdfsClient(hosts='localhost:9870', user_name='hdfs')
            try:
                client.delete(target_dir, recursive=True)
            except:
                pass
            # Thực thi lệnh Sqoop
            result_sqoop = subprocess.run(sqoop_command, capture_output=True, text=True)
            if result_sqoop.returncode == 0:
                sqoop_message = f"Dữ liệu đã được đẩy lên HDFS tại {target_dir}"
            else:
                sqoop_message = f"Lỗi khi chạy Sqoop: {result_sqoop.stderr}"
                
            print("Sqoop message:", sqoop_message)
                
        except Exception as e:
            sqoop_message = f"Lỗi khi thực thi: {str(e)}"
            print("Error:", str(e))
    
    return render(request, 'population/region_stats.html', {
        'region_stats_result': region_stats_result,
        'sqoop_message': sqoop_message
    })
    
def hdfs_browser(request):
    path = request.GET.get('path', '/user')
    client = HdfsClient(hosts='localhost:9870', user_name='hdfs')
    try:
        # Lấy danh sách tệp/thư mục
        listing = client.listdir(path)
        items = []
        # Lấy trạng thái của các mục
        statuses = client.list_status(path)
        status_dict = {s['pathSuffix']: s for s in statuses}  # Tạo từ điển để tra cứu

        for item in listing:
            item_path = os.path.join(path, item).replace('\\', '/')
            # Kiểm tra xem mục là thư mục hay tệp
            status = status_dict.get(item, {})
            is_dir = status.get('type') == 'DIRECTORY' if status else False
            content = None
            if not is_dir and request.GET.get('view') == item_path:
                try:
                    with client.open(item_path) as f:
                        content = f.read().decode('utf-8', errors='ignore')
                except Exception as e:
                    content = f"Lỗi khi đọc tệp: {str(e)}"
            items.append({
                'name': item,
                'path': item_path,
                'is_dir': is_dir,
                'content': content
            })
    except Exception as e:
        items = []
        error = str(e)
        return render(request, 'population/hdfs_browser.html', {'error': error})
    return render(request, 'population/hdfs_browser.html', {'items': items, 'current_path': path})