from service.RequestWebsite import CrawlData
from model.ScoreTHPT import ScoreTHPT
import service.JsonService as JsonService
from datetime import datetime

def exprot_score_to_json(url, file_path, selector_path):
    page_source = CrawlData.get_page_source_selenium(url)
    if page_source is None:
        print("Failed to retrieve page source.")
        return
    
    index = 0
    while True:
        # Lấy đường dẫn css selector của từng bảng điểm
        selector = selector_path.replace("$x", str(2*index+1))
        if CrawlData.check_selector_exists(page_source, selector) == False:
            page_source = CrawlData.simulate_click_path(URL=url, path="#diem-thi-thpt > div > div.more-link > a", headless=True)
            #page_source = CrawlData.navigate_and_collect_click_data(url=url, selector="#diem-thi-thpt > div > div.more-link > a", max_clicks=index, headless=True)
            if page_source is None:
                print("Failed to click more link.")
                break
        index += 1
        scores = CrawlData.get_table_data_from_source(page_source, selector)
        for i in range(1, len(scores)):
            print(scores[i])
            score = scores[i]
            scoreTHPT = ScoreTHPT(
                ma_nganh=score[1],
                ten_nganh=score[2],
                to_hop_mon=score[3],
                nam_hoc= datetime.now().year - index,
                diem_chuan=float(score[4]),
                ghi_chu=score[5] if len(score) > 4 else ""
            )
            if scoreTHPT != "":
                JsonService.append_data_to_json(scoreTHPT.to_dict(), file_path)
    
def main():
    url = "https://diemthi.tuyensinh247.com/diem-chuan/dai-hoc-su-pham-ky-thuat-tphcm-SPK.html"
    file_path = "./data/score.json"
    selector_path = "#diem-thi-thpt > div > div:nth-child($x) > div > div > div > div > div > table"
    exprot_score_to_json(url, file_path, selector_path)
    
if __name__ == "__main__":
    main()
