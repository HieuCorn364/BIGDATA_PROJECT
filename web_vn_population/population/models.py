from django.db import models

class Population(models.Model):
    khu_vuc = models.CharField(max_length=50, primary_key=True)
    dan_so = models.IntegerField()
    dien_tich = models.FloatField()
    mat_do_dan_so = models.FloatField()
    vung = models.CharField(max_length=50)

    class Meta:
        managed = False
        db_table = 'POPULATION'