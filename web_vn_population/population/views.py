from django.shortcuts import render
from .models import Population
from pyhdfs import HdfsClient
import os
import subprocess
from datetime import datetime
from django.db import connection

def population_list(request):
    search_query = request.GET.get('search', '').strip()
    populations = Population.objects.all()
    sqoop_message = None
    
    if search_query:
        populations = Population.objects.filter(khu_vuc__icontains=search_query)
        try:
            # Tạo tên thư mục động
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            target_dir = f"/user/sqoop/tim_kiem_khu_vuc_{timestamp}"
            
            # Thoát ký tự đặc biệt trong từ khóa để tránh lỗi SQL injection
            escaped_query = search_query.replace("'", "''")
            
            # Lệnh Sqoop
            sqoop_command = [
                'sqoop', 'import',
                '-Dorg.apache.sqoop.splitter.allow_text_splitter=true',
                '--connect', 'jdbc:mysql://localhost:3306/BIGDATA',
                '--username', 'root',
                '--password', '@Bao1234',
                '--query', f"SELECT * FROM POPULATION WHERE khu_vuc LIKE '%{escaped_query}%' AND $CONDITIONS",
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
                sqoop_message = f"Dữ liệu tìm kiếm đã được đẩy lên HDFS tại {target_dir}"
            else:
                sqoop_message = f"Lỗi khi chạy Sqoop: {result_sqoop.stderr}"
                
            print("Search Sqoop message:", sqoop_message)
                
        except Exception as e:
            sqoop_message = f"Lỗi khi thực thi Sqoop: {str(e)}"
            print("Search Error:", str(e))
    
    return render(request, 'population/population_list.html', {
        'populations': populations,
        'search_query': search_query,
        'sqoop_message': sqoop_message
    })

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
    
def area_population_ratio(request):
    area_stats_result = None
    sqoop_message = None
    
    if request.method == 'POST' or request.GET.get('calculate') == 'area_stats':
        try:
            # Truy vấn SQL trực tiếp
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT khu_vuc, dan_so, dien_tich, mat_do_dan_so, vung, 
                           dan_so / (SELECT SUM(dan_so) FROM POPULATION) AS ty_trong
                    FROM POPULATION
                """)
                columns = [col[0] for col in cursor.description]
                area_stats_result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            print("Area stats:", area_stats_result)
            
            # Tạo tên thư mục động
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            target_dir = f"/user/sqoop/ty_trong_dan_so_cac_tinh_{timestamp}"
            
            # Lệnh Sqoop
            sqoop_command = [
                'sqoop', 'import',
                '-Dorg.apache.sqoop.splitter.allow_text_splitter=true',
                '--connect', 'jdbc:mysql://localhost:3306/BIGDATA',
                '--username', 'root',
                '--password', '@Bao1234',
                '--query', f"SELECT khu_vuc, dan_so, dien_tich, mat_do_dan_so, vung, dan_so / (SELECT SUM(dan_so) FROM POPULATION) AS ty_trong FROM POPULATION WHERE $CONDITIONS",
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
    
    return render(request, 'population/area_population_ratio.html', {
        'area_stats_result': area_stats_result,
        'sqoop_message': sqoop_message
    })
    
def hdfs_browser(request):
    path = request.GET.get('path', '/user')
    search_query = request.GET.get('search', '').strip()
    client = HdfsClient(hosts='localhost:9870', user_name='hdfs')
    items = []
    sqoop_message = None
    error = None
    
    try:
        # Liệt kê tất cả file và thư mục trong /user
        listing = client.listdir(path)
        statuses = client.list_status(path)
        status_dict = {s['pathSuffix']: s for s in statuses}
        
        # Tìm kiếm file nếu có search_query
        if search_query:
            # Lọc file theo tên gần đúng (không phân biệt hoa thường)
            filtered_items = [
                item for item in listing
                if search_query.lower() in item.lower() and
                status_dict.get(item, {}).get('type') == 'FILE'
            ]
            
            if filtered_items:
                # Tạo thư mục /user/tim_kiem
                tim_kiem_dir = '/user/tim_kiem'
                try:
                    client.mkdirs(tim_kiem_dir)
                except:
                    pass
                
                # Tạo file txt
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                txt_filename = f"tim_kiem_{search_query}_{timestamp}.txt"
                txt_path = f"{tim_kiem_dir}/{txt_filename}"
                txt_content = "\n".join([os.path.join(path, item) for item in filtered_items])
                with client.open(txt_path, 'w') as f:
                    f.write(txt_content.encode('utf-8'))
                
                # Thêm dữ liệu vào bảng HDFS bằng sqoop eval
                for item in filtered_items:
                    item_path = os.path.join(path, item).replace('\\', '/')
                    escaped_path = item_path.replace("'", "''")
                    sqoop_command = [
                        'sqoop', 'eval',
                        '--connect', 'jdbc:mysql://localhost:3306/BIGDATA',
                        '--username', 'root',
                        '--password', '@Bao1234',
                        '--query', f"INSERT INTO HDFS (file_path, time) VALUES ('{escaped_path}', NOW())"
                    ]
                    result_sqoop = subprocess.run(sqoop_command, capture_output=True, text=True)
                    if result_sqoop.returncode != 0:
                        sqoop_message = f"Lỗi khi chạy Sqoop eval cho {item_path}: {result_sqoop.stderr}"
                        print("Sqoop error:", sqoop_message)
                        break
                
                if not sqoop_message:
                    sqoop_message = f"Đã thêm {len(filtered_items)} file vào bảng HDFS và tạo file {txt_path}"
                    print("Sqoop success:", sqoop_message)
            else:
                sqoop_message = f"Không tìm thấy file nào khớp với từ khóa '{search_query}'."
            
            # Cập nhật items để hiển thị kết quả tìm kiếm
            for item in filtered_items:
                item_path = os.path.join(path, item).replace('\\', '/')
                status = status_dict.get(item, {})
                is_dir = status.get('type') == 'DIRECTORY'
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
        else:
            # Hiển thị toàn bộ nội dung thư mục nếu không tìm kiếm
            for item in listing:
                item_path = os.path.join(path, item).replace('\\', '/')
                status = status_dict.get(item, {})
                is_dir = status.get('type') == 'DIRECTORY'
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
        error = str(e)
        print("HDFS error:", error)
    
    return render(request, 'population/hdfs_browser.html', {
        'items': items,
        'current_path': path,
        'search_query': search_query,
        'sqoop_message': sqoop_message,
        'error': error
    })