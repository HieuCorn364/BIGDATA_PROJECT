from django.shortcuts import render
from .models import Population
from pyhdfs import HdfsClient
import os

def population_list(request):
    populations = Population.objects.all()
    return render(request, 'population/population_list.html', {'populations': populations})

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