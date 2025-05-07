from service.RequestWebsite import CrawlData
from model.Population import Population
import service.JsonService as JsonSService

def export_data_to_json(url, file_path, selector_path):
    page_source = CrawlData.get_page_source_selenium(url)
    data_populations = CrawlData.get_table_data_from_source(page_source=page_source, table_selector=selector_path)
    for i in range(1, len(data_populations)):
        data_population = data_populations[i]
        population = Population(
            khu_vuc=data_population[0],
            dan_so=int(data_population[2].replace(".", "")),
            dien_tich=float(data_population[3].replace(".", "").replace(",", "."))
            if len(data_population) > 3 else None
        )
        
        if population is not None:
            JsonSService.append_data_to_json(population.to_dir(), file_path)

def main():
    url = "https://vietnam-travel.vn/tin-dulich/quy-mo-xep-hang-dan-so-cac-tinh-thanh-viet-nam-263.html"
    file_path = "./data/population.json"
    selector_path = "body > section.single > div > div > div.col-md-9.col-sm-12 > div > div.row > div.col-md-7.col-sm-8.col-xs-12.tour > div:nth-child(3) > table"
    
    export_data_to_json(url, file_path, selector_path)

if __name__ == "__main__":
    main()

        
    