class ScoreTHPT:
    def __init__(self, ma_nganh, ten_nganh, to_hop_mon, nam_hoc,diem_chuan, ghi_chu=""):
        self.ma_nganh = ma_nganh
        self.ten_nganh = ten_nganh
        self.to_hop_mon = to_hop_mon
        self.nam_hoc = nam_hoc
        self.diem_chuan = diem_chuan
        self.ghi_chu = ghi_chu
        
    # Method of converting object into dictionary
    def to_dict(self):
        return {
            "ma_nganh": self.ma_nganh,
            "ten_nganh": self.ten_nganh,
            "to_hop_mon": self.to_hop_mon,
            "nam_hoc": self.nam_hoc,
            "diem_chuan": self.diem_chuan,
            "ghi_chu": self.ghi_chu
        }
