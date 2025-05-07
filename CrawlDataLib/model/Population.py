class Population:
    def __init__(self, khu_vuc, dan_so, dien_tich):
        self.khu_vuc = khu_vuc
        self.dan_so = dan_so
        self.dien_tich = dien_tich   
    def to_dir(self):
        return {
            "khu_vuc": self.khu_vuc,
            "dan_so": self.dan_so,
            "dien_tich": self.dien_tich,
        }
        